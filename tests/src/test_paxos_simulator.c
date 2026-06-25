// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

#define SIM_NODES 5
#define SIM_CYCLES 25000
#define MAX_NET_QUEUE 10000

// The Virtual Network Packet
typedef struct {
    paxos_msg_t msg;
    uint64_t deliver_at_tick;
    bool active;
} net_packet_t;

// The Global Truth Ledger (To verify no two nodes diverge)
typedef struct {
    bool committed;
    uint64_t expected_client_seq;
} truth_ledger_slot_t;

static net_packet_t network[MAX_NET_QUEUE];
static truth_ledger_slot_t truth_ledger[100000];

static void enqueue_packet(paxos_msg_t* msg, uint64_t current_tick) {
    // 10% chance to maliciously drop the packet entirely (Network Partition)
    if (rand() % 100 < 10) return; 

    // 20% chance to heavily delay the packet (Out-of-order delivery)
    uint64_t delay = 1;
    if (rand() % 100 < 20) delay = (rand() % 50) + 5; 

    // 5% chance to duplicate the packet (Replay attack)
    int copies = (rand() % 100 < 5) ? 2 : 1;

    for (int c = 0; c < copies; c++) {
        for (int i = 0; i < MAX_NET_QUEUE; i++) {
            if (!network[i].active) {
                network[i].msg = *msg; // Shallow copy is fine, simulator handles lifetime
                network[i].deliver_at_tick = current_tick + delay;
                network[i].active = true;
                
                // Deep copy the payload so it survives the network queue
                if (msg->num_entries > 0 && msg->entries) {
                    network[i].msg.entries = calloc(msg->num_entries, sizeof(paxos_entry_t));
                    for (size_t j = 0; j < msg->num_entries; j++) {
                        // FIXED: Use the safe deep clone for network transit!
                        paxos_entry_clone_deep(&network[i].msg.entries[j], &msg->entries[j]);
                    }
                }
                break;
            }
        }
    }
}

MACRO_TEST(paxos_survives_chaotic_network_fuzzer) {
    uint64_t peers[] = {1, 2, 3, 4, 5};
    paxos_t* nodes[SIM_NODES];
    
    // Boot the cluster
    for (int i = 0; i < SIM_NODES; i++) {
        nodes[i] = paxos_create(peers[i], peers, SIM_NODES);
    }
    
    memset(network, 0, sizeof(network));
    memset(truth_ledger, 0, sizeof(truth_ledger));

    uint64_t global_tick = 0;
    uint64_t client_seq_generator = 1;
    int total_commits = 0;

    // Wake up Node 1 to start an election
    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(nodes[0]);

    while (global_tick < SIM_CYCLES) {
        global_tick++;

        // 1. Tick all nodes & propose random data
        for (int i = 0; i < SIM_NODES; i++) {
            paxos_tick(nodes[i]);

            // Randomly barrage the active leader with client proposals
            if (nodes[i]->state == PAXOS_STATE_ACTIVE && (rand() % 100 < 15)) {
                uint64_t payload_val = client_seq_generator++;
                paxos_entry_t e = { 
                    .type = ENTRY_NORMAL, 
                    .client_id = 99, 
                    .client_seq = payload_val, 
                    .data = (uint8_t*)&payload_val, 
                    .data_len = sizeof(uint64_t) 
                };
                paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
                paxos_step_local(nodes[i], &prop);
            }
        }

        // 2. Deliver ripe network packets
        for (int i = 0; i < MAX_NET_QUEUE; i++) {
            if (network[i].active && global_tick >= network[i].deliver_at_tick) {
                paxos_msg_t* m = &network[i].msg;
                // Route to the correct destination node
                for (int n = 0; n < SIM_NODES; n++) {
                    if (nodes[n]->id == m->to) {
                        paxos_step_remote(nodes[n], m);
                        break;
                    }
                }
                
                // Cleanup duplicated memory
                if (m->num_entries > 0 && m->entries) {
                    for (size_t j = 0; j < m->num_entries; j++) paxos_entry_destroy(&m->entries[j]);
                    free(m->entries);
                }
                network[i].active = false;
            }
        }

        // 3. Process Ready states and Assert Invariants!
        for (int i = 0; i < SIM_NODES; i++) {
            paxos_ready_t ready = paxos_get_ready(nodes[i]);

            // Enqueue all outbound messages into the chaotic network
            for (size_t m = 0; m < ready.num_messages_immediate; m++) enqueue_packet(&ready.messages_immediate[m], global_tick);
            for (size_t m = 0; m < ready.num_messages_after_persist; m++) enqueue_packet(&ready.messages_after_persist[m], global_tick);

            // FAANG INVARIANT CHECK: Prevent Split-Brain
            uint64_t max_stable = nodes[i]->stable_accepted_through;
            for (size_t c = 0; c < ready.num_chosen_entries; c++) {
                paxos_entry_t* chosen = &ready.chosen_entries[c];
                uint64_t s = chosen->slot;
                
                if (chosen->type == ENTRY_NORMAL) {
                    uint64_t val; memcpy(&val, chosen->data, sizeof(uint64_t));
                    
                    // IF we have seen this slot commit before, it MUST exactly match!
                    if (truth_ledger[s].committed) {
                        if (truth_ledger[s].expected_client_seq != val) {
                            printf("\n[FATAL INVARIANT FAILURE] Split-brain detected at slot %llu!\n", s);
                            MACRO_ASSERT_TRUE(false); 
                        }
                    } else {
                        // First time seeing this slot commit. Record it as the absolute truth.
                        truth_ledger[s].committed = true;
                        truth_ledger[s].expected_client_seq = val;
                        total_commits++;
                    }
                }
                max_stable = s; // Mark as applied
            }

            paxos_ready_destroy(&ready);
            paxos_advance(nodes[i], NULL, 0, max_stable);
        }
    }

    // Ensure the cluster actually made progress despite the chaos
    printf("\n[SIMULATOR] Survived %d cycles. Total distinct slots committed: %d\n", SIM_CYCLES, total_commits);
    MACRO_ASSERT_TRUE(total_commits > 50);

    for (int i = 0; i < SIM_NODES; i++) paxos_destroy(nodes[i]);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_survives_chaotic_network_fuzzer);

    macro_run_all("paxos_simulator", tests, test_count);
    return 0;
}

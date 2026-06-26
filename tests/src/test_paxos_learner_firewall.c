// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

static void force_active_leader(paxos_t* p) {
    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);
    paxos_advance(p, NULL, 0, 0);
    if (p->num_nodes > 1) {
        uint64_t remote_peer = p->node_directory[1];
        paxos_msg_t prom = { .type = MSG_PROMISE, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .num_entries = 0 };
        paxos_step_remote(p, &prom);
        paxos_advance(p, NULL, 0, 0);
        paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .slot = p->next_slot - 1 };
        paxos_step_remote(p, &ack);
        paxos_advance(p, NULL, 0, 0);
    }
}

MACRO_TEST(learner_firewall_blocks_blind_voters) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);
    force_active_leader(p); // Commit index is now 1

    uint64_t target = 4;
    paxos_entry_t e = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&target, .data_len = sizeof(uint64_t) };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    // Propose Node 4 (who has NEVER sent an ACK and is totally offline)
    paxos_step_local(p, &prop);

    // The Firewall MUST drop the proposal to protect the cluster!
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);

    // BUT, the engine should have silently added Node 4 as a Learner so it can start streaming!
    MACRO_ASSERT_EQ_INT(p->num_nodes, 4);
    MACRO_ASSERT_EQ_INT(p->node_directory[3], 4);

    // Simulate Node 4 catching up and sending an ACK for Slot 1
    paxos_msg_t catch_up_ack = { .type = MSG_ACCEPTED, .to = 1, .from = 4, .ballot = p->active_ballot, .slot = 1 };
    paxos_step_remote(p, &catch_up_ack);

    // Propose Node 4 AGAIN now that it is caught up
    paxos_step_local(p, &prop);

    // The Firewall allows it through! Slot 2 now holds the JOINT config.
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 2);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, learner_firewall_blocks_blind_voters);
    macro_run_all("paxos_learner_firewall", tests, test_count);
    return 0;
}

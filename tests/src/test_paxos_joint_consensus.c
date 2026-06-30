// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

static void force_active_leader(paxos_t* p) {
    extern void paxos_proposer_campaign(paxos_t* p);
    (void)paxos_proposer_campaign(p);
    paxos_advance(p, NULL, 0, 0);
    if (p->num_nodes > 1) {
        uint64_t remote_peer = p->node_directory[1];
        paxos_msg_t prom = { .type = PAXOS_MSG_PROMISE, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .num_entries = 0 };
    (void)paxos_receive(p, &prom);
        paxos_advance(p, NULL, 0, 0);

        paxos_msg_t ack = { .type = PAXOS_MSG_ACCEPTED, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .slot = p->next_slot - 1 };
    (void)paxos_receive(p, &ack);
        paxos_advance(p, NULL, 0, 0);
    }
}

MACRO_TEST(joint_consensus_two_phase_commit_completes_safely) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 2
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    force_active_leader(p);

    MACRO_ASSERT_EQ_INT(p->num_nodes, 3);
    MACRO_ASSERT_EQ_INT(p->active_config_mask, 7);

    // Explicitly catch the Learner up so the firewall allows the transition
    uint64_t new_node = 4;
    paxos_register_learner(p, new_node);
    paxos_msg_t catch_up_ack = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 4, .ballot = p->active_ballot, .slot = 1 };
    (void)paxos_receive(p, &catch_up_ack);

    // Simulate what a future paxos_propose_config() API would do internally
    uint64_t joint_nodes[] = {1, 2, 3, 4};
    uint64_t slot = p->next_slot++;
    paxos_log_accept(p, slot, p->active_ballot, PAXOS_ENTRY_CONF_JOINT, 0, 0, (uint8_t*)joint_nodes, sizeof(joint_nodes));

    paxos_inflight_slot_t* inf = &p->inflight[slot % PAXOS_INTERNAL_INFLIGHT_WINDOW];
    inf->slot = slot;
    inf->ballot = p->active_ballot;
    inf->ack_mask = paxos_peer_bit(p, p->id);
    inf->chosen = false;
    inf->active = true;
    p->pending_reconfig = true;

    paxos_entry_t* logged = paxos_log_get(p, 2);
    MACRO_ASSERT_TRUE(logged != NULL);
    MACRO_ASSERT_EQ_INT(logged->type, PAXOS_ENTRY_CONF_JOINT);
    MACRO_ASSERT_TRUE(p->pending_reconfig == true);

    // Form a quorum for the JOINT configuration
    paxos_msg_t ack_joint = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2 };
    (void)paxos_receive(p, &ack_joint);

    MACRO_ASSERT_TRUE(p->in_joint_consensus == true);
    MACRO_ASSERT_EQ_INT(p->joint_config_mask, 15);

    // The state machine should automatically generate the FINAL config upon committing JOINT
    paxos_entry_t* final_entry = paxos_log_get(p, 3);
    MACRO_ASSERT_TRUE(final_entry != NULL);
    MACRO_ASSERT_EQ_INT(final_entry->type, PAXOS_ENTRY_CONF_FINAL);

    // Now requires overlapping majority (requires Node 2 and Node 3 ACKs to satisfy the joint quorum)
    paxos_msg_t ack_final_2 = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 3 };
    (void)paxos_receive(p, &ack_final_2);

    paxos_msg_t ack_final_3 = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 3, .ballot = p->active_ballot, .slot = 3 };
    (void)paxos_receive(p, &ack_final_3);

    // System should seamlessly return to normal consensus under the new configuration
    MACRO_ASSERT_TRUE(p->in_joint_consensus == false);
    MACRO_ASSERT_TRUE(p->pending_reconfig == false);
    MACRO_ASSERT_EQ_INT(p->active_config_mask, 15);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, joint_consensus_two_phase_commit_completes_safely);
    macro_run_all("paxos_joint_consensus", tests, test_count);
    return 0;
}

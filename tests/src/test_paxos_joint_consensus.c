// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

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

MACRO_TEST(joint_consensus_two_phase_commit_completes_safely) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    MACRO_ASSERT_EQ_INT(p->num_nodes, 3);
    MACRO_ASSERT_EQ_INT(p->active_config_mask, 7); // Nodes 1, 2, 3

    // 1. Propose ADD Node 4
    uint64_t new_node = 4;
    paxos_entry_t e_conf = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    paxos_msg_t p_conf = { .type = MSG_PROPOSE, .entries = &e_conf, .num_entries = 1 };
    paxos_step_local(p, &p_conf);

    // 2. The core must intercept it, lock the Alpha-Window, and log it as JOINT!
    paxos_entry_t* logged = paxos_log_get(p, 2);
    MACRO_ASSERT_TRUE(logged != NULL);
    MACRO_ASSERT_EQ_INT(logged->type, ENTRY_CONF_JOINT);
    MACRO_ASSERT_TRUE(p->pending_reconfig == true);

    // 3. Node 2 ACKs the JOINT proposal (Requires majority of 3 = 2 votes. Node 1 + 2 passes!)
    paxos_msg_t ack_joint = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2 };
    paxos_step_remote(p, &ack_joint);

    // The joint mask is now active! It should overlap old (7) and new (15).
    MACRO_ASSERT_TRUE(p->in_joint_consensus == true);
    MACRO_ASSERT_EQ_INT(p->joint_config_mask, 15);

    // 4. The core MUST have auto-generated the FINAL proposal in slot 3!
    paxos_entry_t* final_entry = paxos_log_get(p, 3);
    MACRO_ASSERT_TRUE(final_entry != NULL);
    MACRO_ASSERT_EQ_INT(final_entry->type, ENTRY_CONF_FINAL);

    // 5. ACKs for the FINAL proposal
    paxos_msg_t ack_final_2 = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 3 };
    paxos_step_remote(p, &ack_final_2);

    // FAANG: In Joint Consensus, a majority of the NEW topology (4 nodes) is 3 votes!
    // Node 1 (Self) + Node 2 + Node 3 = 3 votes. Quorum reached!
    paxos_msg_t ack_final_3 = { .type = MSG_ACCEPTED, .to = 1, .from = 3, .ballot = p->active_ballot, .slot = 3 };
    paxos_step_remote(p, &ack_final_3);

    // 6. The transition is complete. The lock is released.
    MACRO_ASSERT_TRUE(p->in_joint_consensus == false);
    MACRO_ASSERT_TRUE(p->pending_reconfig == false);
    MACRO_ASSERT_EQ_INT(p->active_config_mask, 15); // Permanently 4 nodes

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, joint_consensus_two_phase_commit_completes_safely);
    macro_run_all("paxos_joint_consensus", tests, test_count);
    return 0;
}

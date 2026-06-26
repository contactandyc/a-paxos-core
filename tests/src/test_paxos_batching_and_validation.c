// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

// FAANG: Bring in the election simulator helper!
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

MACRO_TEST(follower_receives_batched_accept_and_persists_correctly) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);
    p->promised_ballot = 10;

    paxos_entry_t batch[3] = {
        { .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 },
        { .type = ENTRY_NORMAL, .data = (uint8_t*)"B", .data_len = 1 },
        { .type = ENTRY_NORMAL, .data = (uint8_t*)"C", .data_len = 1 }
    };

    paxos_msg_t acc = { .type = MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 5, .entries = batch, .num_entries = 3 };
    paxos_step_remote(p, &acc);

    MACRO_ASSERT_TRUE(paxos_log_get(p, 5) != NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 6) != NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 7) != NULL);

    paxos_ready_t ready = paxos_get_ready(p);
    // Follower must queue 3 separate MSG_ACCEPTED replies for the proposer
    MACRO_ASSERT_EQ_INT(ready.num_messages_after_persist, 3);
    paxos_ready_destroy(&ready);

    paxos_destroy(p);
}

MACRO_TEST(propose_rejects_malformed_secondary_entries) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    force_active_leader(p); // FIXED: Give the node quorum so it can propose!

    paxos_entry_t bad_batch[2] = {
        { .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 }, // Valid
        { .type = ENTRY_NORMAL, .data = NULL, .data_len = 10 }          // Invalid!
    };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = bad_batch, .num_entries = 2 };

    paxos_step_local(p, &prop);

    // The entire batch must be rejected at the firewall!
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1); // Only the election NoOp exists

    paxos_destroy(p);
}

MACRO_TEST(add_node_must_not_duplicate_nodes_in_joint_payload) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    force_active_leader(p); // FIXED: Give the node quorum so it can propose!

    // Node 2 is ALREADY in the cluster. If we request an ADD for Node 2,
    // the Morph must recognize it and prevent a duplicate in the JOINT array.
    uint64_t target = 2;
    paxos_entry_t e = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&target, .data_len = sizeof(uint64_t) };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    paxos_step_local(p, &prop);

    paxos_entry_t* log_e = paxos_log_get(p, 2);
    MACRO_ASSERT_TRUE(log_e != NULL);
    MACRO_ASSERT_EQ_INT(log_e->type, ENTRY_CONF_JOINT);
    MACRO_ASSERT_EQ_INT(log_e->data_len, 3 * sizeof(uint64_t)); // Still exactly 3 nodes, no duplicate added!

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, follower_receives_batched_accept_and_persists_correctly);
    MACRO_ADD(tests, propose_rejects_malformed_secondary_entries);
    MACRO_ADD(tests, add_node_must_not_duplicate_nodes_in_joint_payload);
    macro_run_all("paxos_batching_and_validation", tests, test_count);
    return 0;
}

// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

// Helper to instantly elevate a node to Active Leader for Phase 2 tests
static void force_active_leader(paxos_t* p) {
    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);

    // Clear the outbound PREPARE messages
    paxos_advance(p, 0, 0);

    // Inject a PROMISE from Node 2 to establish Quorum (Self + 2 = 2/3)
    paxos_msg_t prom = {
        .type = MSG_PROMISE,
        .to = p->id,
        .from = p->peers[0],
        .ballot = p->active_ballot,
        .num_entries = 0
    };
    paxos_step_remote(p, &prom);
    paxos_advance(p, 0, 0);
}

MACRO_TEST(paxos_acceptor_rejects_lower_ballot_with_nack) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    // Promise to Ballot 10
    paxos_msg_t prep = { .type = MSG_PREPARE, .to = 1, .from = 2, .ballot = 10, .slot = 1 };
    paxos_step_remote(p, &prep);
    paxos_advance(p, 0, 0);

    // Receive Accept for an older Ballot (5)
    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    paxos_msg_t acc = { .type = MSG_ACCEPT, .to = 1, .from = 3, .ballot = 5, .slot = 1, .entries = &e, .num_entries = 1 };
    paxos_step_remote(p, &acc);

    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_NACK);
    MACRO_ASSERT_EQ_INT(ready.messages[0].promised_ballot, 10);

    // Verify it was NOT written to the log
    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL);

    paxos_destroy(p);
}

MACRO_TEST(paxos_acceptor_allows_out_of_order_accepts) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    // Accept slot 5 first (Skipping 1, 2, 3, 4)
    paxos_entry_t e5 = { .type = ENTRY_NORMAL, .data = (uint8_t*)"5", .data_len = 1 };
    paxos_msg_t acc5 = { .type = MSG_ACCEPT, .to = 1, .from = 2, .ballot = 2, .slot = 5, .entries = &e5, .num_entries = 1 };
    paxos_step_remote(p, &acc5);

    // Accept slot 2 second
    paxos_entry_t e2 = { .type = ENTRY_NORMAL, .data = (uint8_t*)"2", .data_len = 1 };
    paxos_msg_t acc2 = { .type = MSG_ACCEPT, .to = 1, .from = 2, .ballot = 2, .slot = 2, .entries = &e2, .num_entries = 1 };
    paxos_step_remote(p, &acc2);

    // Verify the log is sparse but correctly populated
    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 3) == NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 4) == NULL);

    paxos_entry_t* r2 = paxos_log_get(p, 2);
    MACRO_ASSERT_TRUE(r2 != NULL);
    MACRO_ASSERT_TRUE(memcmp(r2->data, "2", 1) == 0);

    paxos_entry_t* r5 = paxos_log_get(p, 5);
    MACRO_ASSERT_TRUE(r5 != NULL);
    MACRO_ASSERT_TRUE(memcmp(r5->data, "5", 1) == 0);

    // Verify highest slot tracker works correctly on sparse logs
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 5);

    paxos_destroy(p);
}

MACRO_TEST(paxos_leader_advances_commit_index_on_quorum_accepted) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_ACTIVE);

    // Propose a value locally
    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    paxos_step_local(p, &prop);

    // It should occupy slot 1 (since the log was empty)
    paxos_advance(p, 0, 0);
    MACRO_ASSERT_EQ_INT(paxos_commit_index(p), 0); // Not committed yet!

    // Receive an ACCEPTED from Node 2.
    // Self + Node 2 = Quorum (2 out of 3).
    paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 1 };
    paxos_step_remote(p, &ack);

    // Commit Index must advance immediately!
    MACRO_ASSERT_EQ_INT(paxos_commit_index(p), 1);

    // Verify it surfaces via the Ready interface
    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready.num_chosen_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.chosen_entries[0].slot, 1);

    paxos_destroy(p);
}

MACRO_TEST(paxos_leader_steps_down_on_nack) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_ACTIVE);

    // Node 3 NACKs us because another leader has emerged with a higher ballot
    uint64_t higher_ballot = p->active_ballot + 10;
    paxos_msg_t nack = { .type = MSG_NACK, .to = 1, .from = 3, .promised_ballot = higher_ballot };
    paxos_step_remote(p, &nack);

    // We must immediately revert to a passive learner
    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_LEARNER);
    MACRO_ASSERT_EQ_INT(paxos_promised_ballot(p), higher_ballot); // <-- Fixed assertion!
    MACRO_ASSERT_EQ_INT(p->leader_id, 0);

    paxos_destroy(p);
}

MACRO_TEST(paxos_acceptor_updates_commit_index_from_piggyback) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    // Node 2 (Leader) sends an Accept for slot 10, but notifies us that slots 1-9 are already committed.
    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    paxos_msg_t acc = {
        .type = MSG_ACCEPT,
        .to = 1,
        .from = 2,
        .ballot = 5,
        .slot = 10,
        .commit_index = 9,
        .entries = &e,
        .num_entries = 1
    };

    paxos_step_remote(p, &acc);

    // The Acceptor should have updated its commit index
    MACRO_ASSERT_EQ_INT(paxos_commit_index(p), 9);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_acceptor_rejects_lower_ballot_with_nack);
    MACRO_ADD(tests, paxos_acceptor_allows_out_of_order_accepts);
    MACRO_ADD(tests, paxos_leader_advances_commit_index_on_quorum_accepted);
    MACRO_ADD(tests, paxos_leader_steps_down_on_nack);
    MACRO_ADD(tests, paxos_acceptor_updates_commit_index_from_piggyback);

    macro_run_all("paxos_phase2", tests, test_count);
    return 0;
}

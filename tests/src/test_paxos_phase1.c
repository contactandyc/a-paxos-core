// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(paxos_initial_state_is_learner) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    MACRO_ASSERT_TRUE(paxos_state(p) == PAXOS_STATE_LEARNER);
    MACRO_ASSERT_EQ_INT(paxos_promised_ballot(p), 0);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 0);

    paxos_destroy(p);
}

MACRO_TEST(paxos_campaign_generates_unique_ballot_and_broadcasts_prepare) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);

    MACRO_ASSERT_TRUE(paxos_state(p) == PAXOS_STATE_RECOVERING_PHASE1);

    uint64_t expected_ballot = (1ULL << 16) | 1;
    MACRO_ASSERT_EQ_INT(p->active_ballot, expected_ballot);

    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready.num_messages_after_persist, 2); // <--- FIXED
    MACRO_ASSERT_TRUE(ready.messages_after_persist[0].type == MSG_PREPARE); // <--- FIXED
    MACRO_ASSERT_EQ_INT(ready.messages_after_persist[0].ballot, expected_ballot); // <--- FIXED

    paxos_ready_destroy(&ready); // <--- Add cleanup
    paxos_destroy(p);
}

MACRO_TEST(paxos_phase1_merge_forces_noop_on_paxos_gap) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);
    paxos_advance(p, 0, 0);

    paxos_entry_t recovered_e = {
        .slot = 2,
        .accepted_ballot = 5,
        .type = ENTRY_NORMAL,
        .data = (uint8_t*)"DATA",
        .data_len = 4
    };

    paxos_msg_t prom = {
        .type = MSG_PROMISE,
        .to = 1,
        .from = 2,
        .ballot = p->active_ballot,
        .entries = &recovered_e,
        .num_entries = 1
    };

    paxos_step_remote(p, &prom);

    // Because it recovered a gap, it should bypass PHASE2 and go straight to ACTIVE
    // if there were no values recovered that needed to be re-proposed.
    // However, it DID recover slot 2, so it should transition to RECOVERING_PHASE2,
    // broadcast the ACCEPTs, and THEN transition to ACTIVE if it hits the end of the recovery buffer.
    MACRO_ASSERT_TRUE(paxos_state(p) == PAXOS_STATE_RECOVERING_PHASE2);

    paxos_ready_t ready = paxos_get_ready(p);

    // 2 messages for slot 1, 2 messages for slot 2 = 4 total outbound Accept messages
    MACRO_ASSERT_EQ_INT(ready.num_messages_after_persist, 4); // <--- FIXED

    paxos_entry_t* log_slot1 = paxos_log_get(p, 1);
    paxos_entry_t* log_slot2 = paxos_log_get(p, 2);

    MACRO_ASSERT_TRUE(log_slot1 != NULL);
    MACRO_ASSERT_TRUE(log_slot2 != NULL);

    MACRO_ASSERT_TRUE(log_slot1->type == ENTRY_NOOP);
    MACRO_ASSERT_EQ_INT(log_slot1->accepted_ballot, p->active_ballot);

    MACRO_ASSERT_TRUE(log_slot2->type == ENTRY_NORMAL);
    MACRO_ASSERT_EQ_INT(log_slot2->accepted_ballot, p->active_ballot);
    MACRO_ASSERT_EQ_INT(log_slot2->data_len, 4);

    paxos_ready_destroy(&ready); // <--- Add cleanup
    paxos_destroy(p);
}

MACRO_TEST(paxos_phase1_merge_adopts_highest_ballot_value) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);
    paxos_advance(p, 0, 0);

    paxos_entry_t e2 = { .slot = 1, .accepted_ballot = 5, .type = ENTRY_NORMAL, .data = (uint8_t*)"OLD", .data_len = 3 };
    paxos_msg_t prom2 = { .type = MSG_PROMISE, .to = 1, .from = 2, .ballot = p->active_ballot, .entries = &e2, .num_entries = 1 };

    paxos_entry_t e3 = { .slot = 1, .accepted_ballot = 10, .type = ENTRY_NORMAL, .data = (uint8_t*)"NEW", .data_len = 3 };
    paxos_msg_t prom3 = { .type = MSG_PROMISE, .to = 1, .from = 3, .ballot = p->active_ballot, .entries = &e3, .num_entries = 1 };

    paxos_step_remote(p, &prom3);
    paxos_step_remote(p, &prom2);

    paxos_entry_t* final_val = paxos_log_get(p, 1);
    MACRO_ASSERT_TRUE(final_val != NULL);
    MACRO_ASSERT_TRUE(memcmp(final_val->data, "NEW", 3) == 0);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_initial_state_is_learner);
    MACRO_ADD(tests, paxos_campaign_generates_unique_ballot_and_broadcasts_prepare);
    MACRO_ADD(tests, paxos_phase1_merge_forces_noop_on_paxos_gap);
    MACRO_ADD(tests, paxos_phase1_merge_adopts_highest_ballot_value);

    macro_run_all("paxos_phase1", tests, test_count);
    return 0;
}

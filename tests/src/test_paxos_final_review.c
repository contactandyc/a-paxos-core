// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(nack_updates_last_observed_but_not_local_promise) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);
    p->promised_ballot = 10;
    p->active_ballot = 15;
    p->state = PAXOS_STATE_RECOVERING_PHASE1;

    // We receive a NACK indicating another node has seen ballot 50
    paxos_msg_t nack = { .type = MSG_NACK, .to = 1, .from = 2, .ballot = 15, .promised_ballot = 50 };
    paxos_step_remote(p, &nack);

    // Our local disk state MUST NOT mutate, but we must observe the higher epoch!
    MACRO_ASSERT_EQ_INT(p->promised_ballot, 10);
    MACRO_ASSERT_EQ_INT(p->last_observed_ballot, 50);
    MACRO_ASSERT_EQ_INT(p->state, PAXOS_STATE_LEARNER);

    paxos_destroy(p);
}

MACRO_TEST(duplicate_read_barrier_respects_persistence_ordering) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);
    p->promised_ballot = 5;

    // A ReadBarrier arrives with a higher ballot, forcing a state change
    paxos_msg_t rb = { .type = MSG_READ_BARRIER, .to = 1, .from = 2, .ballot = 10, .read_seq = 99 };

    paxos_step_remote(p, &rb);
    paxos_ready_t r1 = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(r1.num_messages_after_persist, 1);
    MACRO_ASSERT_EQ_INT(r1.num_messages_immediate, 0);
    paxos_ready_destroy(&r1);

    // Duplicate arrives before disk is synced
    paxos_step_remote(p, &rb);
    paxos_ready_t r2 = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(r2.num_messages_after_persist, 2); // Safely queued!
    MACRO_ASSERT_EQ_INT(r2.num_messages_immediate, 0);
    paxos_ready_destroy(&r2);

    paxos_destroy(p);
}

MACRO_TEST(fetch_entries_conflicting_with_chosen_slot_fatals) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    // Locally commit "DATA_A"
    paxos_log_accept(p, 1, 5, ENTRY_NORMAL, 0, 0, (uint8_t*)"DATA_A", 6);
    paxos_log_learn_chosen(p, 1, paxos_log_get_accepted(p, 1));

    // A rogue leader tries to catch us up with conflicting "DATA_B"
    p->leader_id = 2;
    p->promised_ballot = 10;
    paxos_entry_t rogue = { .slot = 1, .accepted_ballot = 10, .type = ENTRY_NORMAL, .data = (uint8_t*)"DATA_B", .data_len = 6 };
    paxos_msg_t fetch_res = { .type = MSG_FETCH_ENTRIES_RES, .to = 1, .from = 2, .ballot = 10, .entries = &rogue, .num_entries = 1 };

    paxos_step_remote(p, &fetch_res);

    // Engine MUST crash to prevent split-brain
    MACRO_ASSERT_TRUE(p->fatal_error == true);

    paxos_destroy(p);
}

MACRO_TEST(compaction_shifts_correctly_at_exact_chunk_sizes) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    // The exact boundary case (1024 slots per chunk).
    // Compacting at 1024 means slot 1025 is the new base (which should be index 0 of the shifted array).
    paxos_log_accept(p, 1025, 1, ENTRY_NORMAL, 0, 0, (uint8_t*)"A", 1);
    p->last_applied = 1024;
    paxos_compact(p, 1024);

    MACRO_ASSERT_EQ_INT(p->log_base_slot, 1025);
    paxos_entry_t* e = paxos_log_get(p, 1025);
    MACRO_ASSERT_TRUE(e != NULL);
    MACRO_ASSERT_TRUE(p->log_chunks[0] != NULL); // It shifted properly to the root chunk

    paxos_destroy(p);
}

MACRO_TEST(crash_after_duplicate_prepare_recovers_safely) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    paxos_msg_t prep = { .type = MSG_PREPARE, .to = 1, .from = 2, .ballot = 50, .slot = 1 };
    paxos_step_remote(p, &prep);
    paxos_step_remote(p, &prep); // Duplicate

    // We generated 2 promises, but the disk CRASHES before saving hard state.
    paxos_hard_state_t disk_state = p->prev_hard_state; // The old state (ballot 0)

    paxos_destroy(p);

    // Recover from disk
    paxos_t* p2 = paxos_restore(1, peers, 3, disk_state, 0, 0, NULL, 0);

    // The engine successfully rolls back to before the prepares hit
    MACRO_ASSERT_EQ_INT(p2->promised_ballot, 0);
    MACRO_ASSERT_EQ_INT(p2->msg_queue_after_persist_len, 0);

    paxos_destroy(p2);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, nack_updates_last_observed_but_not_local_promise);
    MACRO_ADD(tests, duplicate_read_barrier_respects_persistence_ordering);
    MACRO_ADD(tests, fetch_entries_conflicting_with_chosen_slot_fatals);
    MACRO_ADD(tests, compaction_shifts_correctly_at_exact_chunk_sizes);
    MACRO_ADD(tests, crash_after_duplicate_prepare_recovers_safely);
    macro_run_all("paxos_final_review", tests, test_count);
    return 0;
}

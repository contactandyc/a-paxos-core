// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

// Replace the old test with this updated one:
MACRO_TEST(nack_fast_forwards_local_promise) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    p->promised_ballot = 10;
    p->active_ballot = 15;
    p->state = PAXOS_STATE_RECOVERING_PHASE1;

    paxos_msg_t nack = { .type = PAXOS_MSG_NACK, .to = 1, .from = 2, .ballot = 15, .promised_ballot = 50 };
    (void)paxos_receive(p, &nack);

    // FIX: The system now correctly fast-forwards to 50
    MACRO_ASSERT_EQ_INT(p->promised_ballot, 50);
    MACRO_ASSERT_EQ_INT(p->last_observed_ballot, 50);
    MACRO_ASSERT_EQ_INT(p->state, PAXOS_STATE_LEARNER);

    paxos_destroy(p);
}

MACRO_TEST(duplicate_read_barrier_respects_persistence_ordering) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    p->promised_ballot = 5;

    paxos_msg_t rb = { .type = PAXOS_MSG_READ_BARRIER, .to = 1, .from = 2, .ballot = 10, .read_seq = 99 };

    (void)paxos_receive(p, &rb);
    paxos_ready_t r1;
    (void)paxos_get_ready(p, &r1);
    MACRO_ASSERT_EQ_INT(r1.num_messages_after_persist, 1);
    MACRO_ASSERT_EQ_INT(r1.num_messages_immediate, 0);

    (void)paxos_receive(p, &rb);
    paxos_ready_t r2;
    (void)paxos_get_ready(p, &r2);
    MACRO_ASSERT_EQ_INT(r2.num_messages_after_persist, 2);
    MACRO_ASSERT_EQ_INT(r2.num_messages_immediate, 0);

    paxos_destroy(p);
}

MACRO_TEST(fetch_entries_conflicting_with_chosen_slot_fatals) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_log_accept(p, 1, 5, PAXOS_ENTRY_NORMAL, 0, 0, (uint8_t*)"DATA_A", 6);
    paxos_log_learn_chosen(p, 1, paxos_log_get_accepted(p, 1));

    p->leader_id = 2;
    p->promised_ballot = 10;
    paxos_entry_t rogue = { .slot = 1, .accepted_ballot = 10, .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"DATA_B", .data_len = 6 };
    paxos_msg_t fetch_res = { .type = PAXOS_MSG_FETCH_ENTRIES_RES, .to = 1, .from = 2, .ballot = 10, .entries = &rogue, .num_entries = 1 };

    (void)paxos_receive(p, &fetch_res);

    MACRO_ASSERT_TRUE(p->fatal_error == true);

    paxos_destroy(p);
}

MACRO_TEST(compaction_shifts_correctly_at_exact_chunk_sizes) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_log_accept(p, 1025, 1, PAXOS_ENTRY_NORMAL, 0, 0, (uint8_t*)"A", 1);
    p->last_applied = 1024;
    paxos_compact(p, 1024);

    MACRO_ASSERT_EQ_INT(p->log_base_slot, 1025);
    paxos_entry_t* e = paxos_log_get(p, 1025);
    MACRO_ASSERT_TRUE(e != NULL);
    MACRO_ASSERT_TRUE(p->log_chunks[0] != NULL);

    paxos_destroy(p);
}

MACRO_TEST(crash_after_duplicate_prepare_recovers_safely) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_msg_t prep = { .type = PAXOS_MSG_PREPARE, .to = 1, .from = 2, .ballot = 50, .slot = 1 };
    (void)paxos_receive(p, &prep);
    (void)paxos_receive(p, &prep);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);
    paxos_hard_state_t disk_state = ready.hard_state;

    disk_state.promised_ballot = 0;
    disk_state.max_generated_ballot = 0;

    paxos_destroy(p);

    // FIXED: Removed the erroneous (void) cast here!
    paxos_restore_data_t rd = {
        .struct_size = sizeof(paxos_restore_data_t),
        .hard_state = disk_state,
        .local_commit_index = 0,
        .snapshot_index = 0,
        .entries = NULL,
        .num_entries = 0
    };

    paxos_t* p2;
    (void)paxos_restore(&cfg, &rd, &p2);

    MACRO_ASSERT_EQ_INT(p2->promised_ballot, 0);
    MACRO_ASSERT_EQ_INT(p2->msg_queue_after_persist_len, 0);

    paxos_destroy(p2);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, nack_fast_forwards_local_promise);
    MACRO_ADD(tests, duplicate_read_barrier_respects_persistence_ordering);
    MACRO_ADD(tests, fetch_entries_conflicting_with_chosen_slot_fatals);
    MACRO_ADD(tests, compaction_shifts_correctly_at_exact_chunk_sizes);
    MACRO_ADD(tests, crash_after_duplicate_prepare_recovers_safely);
    macro_run_all("paxos_final_review", tests, test_count);
    return 0;
}

// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(crash_after_hard_state_persisted_but_before_entries) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_msg_t prep = { .type = PAXOS_MSG_PREPARE, .to = 1, .from = 2, .ballot = 25, .slot = 1 };
    (void)paxos_receive(p, &prep);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);

    paxos_hard_state_t saved_hs = ready.hard_state;

    paxos_destroy(p);

    // FIXED: Removed the erroneous (void) casts here!
    paxos_restore_data_t rd = {
        .struct_size = sizeof(paxos_restore_data_t),
        .hard_state = saved_hs,
        .local_commit_index = 0,
        .snapshot_index = 0,
        .entries = NULL,
        .num_entries = 0
    };

    paxos_t* p2;
    (void)paxos_restore(&cfg, &rd, &p2);

    MACRO_ASSERT_EQ_INT(p2->promised_ballot, 25);

    paxos_destroy(p2);
}

MACRO_TEST(crash_after_entries_persisted_but_before_advance) {
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
    p->leader_id = 2;

    paxos_entry_t e = { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };
    paxos_msg_t acc = { .type = PAXOS_MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = &e, .num_entries = 1 };
    (void)paxos_receive(p, &acc);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);

    // FIXED: Removed the erroneous (void) casts here!
    paxos_restored_entry_t saved_disk[1] = {
        { .entry = ready.entries_to_save[0], .chosen = false }
    };

    paxos_restore_data_t rd = {
        .struct_size = sizeof(paxos_restore_data_t),
        .hard_state = ready.hard_state,
        .local_commit_index = 0,
        .snapshot_index = 0,
        .entries = saved_disk,
        .num_entries = 1
    };

    paxos_destroy(p);

    paxos_t* p2;
    (void)paxos_restore(&cfg, &rd, &p2);

    paxos_entry_t* recovered = paxos_log_get(p2, 1);
    MACRO_ASSERT_TRUE(recovered != NULL);
    MACRO_ASSERT_EQ_INT(recovered->accepted_ballot, 10);

    uint64_t c_idx = (1 - p2->log_base_slot) / PAXOS_INTERNAL_LOG_CHUNK_SIZE;
    uint64_t c_off = (1 - p2->log_base_slot) % PAXOS_INTERNAL_LOG_CHUNK_SIZE;
    MACRO_ASSERT_TRUE(p2->log_chunks[c_idx]->slots[c_off].is_chosen == false);

    paxos_destroy(p2);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, crash_after_hard_state_persisted_but_before_entries);
    MACRO_ADD(tests, crash_after_entries_persisted_but_before_advance);
    macro_run_all("paxos_crash_lifecycle", tests, test_count);
    return 0;
}

// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(paxos_restores_from_hard_state_and_entries) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };

    paxos_hard_state_t hs = { .promised_ballot = 15, .max_generated_ballot = 15 };
    paxos_entry_t e1 = { .slot = 1, .accepted_ballot = 10, .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };

    // Fixed: Removed the erroneous (void) casts from both data structs
    paxos_restored_entry_t restored_arr[1] = {
        { .entry = e1, .chosen = false }
    };

    paxos_restore_data_t rd = {
        .struct_size = sizeof(paxos_restore_data_t),
        .hard_state = hs,
        .local_commit_index = 0,
        .snapshot_index = 0,
        .entries = restored_arr,
        .num_entries = 1
    };

    paxos_t* p;
    (void)paxos_restore(&cfg, &rd, &p);

    MACRO_ASSERT_TRUE(p != NULL);
    MACRO_ASSERT_EQ_INT(paxos_promised_ballot(p), 15);
    MACRO_ASSERT_EQ_INT(p->max_generated_ballot, 15);

    paxos_entry_t* log_e = paxos_log_get(p, 1);
    MACRO_ASSERT_TRUE(log_e != NULL);
    MACRO_ASSERT_EQ_INT(log_e->accepted_ballot, 10);

    uint64_t c_idx = 1 / PAXOS_INTERNAL_LOG_CHUNK_SIZE;
    uint64_t c_off = 1 % PAXOS_INTERNAL_LOG_CHUNK_SIZE;

    MACRO_ASSERT_TRUE(p->log_chunks[c_idx]->slots[c_off].unstable == false);
    MACRO_ASSERT_TRUE(p->log_chunks[c_idx]->slots[c_off].is_chosen == false);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, paxos_restores_from_hard_state_and_entries);
    macro_run_all("paxos_restore", tests, test_count);
    return 0;
}

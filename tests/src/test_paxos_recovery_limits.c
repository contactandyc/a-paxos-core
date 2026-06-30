// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(promise_exceeding_recovery_gap_fatals_to_prevent_dos) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };
    paxos_t* p;
    paxos_create(&cfg, &p);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);

    paxos_entry_t huge_e = { .slot = 200000, .accepted_ballot = 5, .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    paxos_msg_t prom = { .type = PAXOS_MSG_PROMISE, .to = 1, .from = 2, .ballot = p->active_ballot, .entries = &huge_e, .num_entries = 1 };

    paxos_receive(p, &prom);

    MACRO_ASSERT_TRUE(p->fatal_error == true);

    paxos_destroy(p);
}

MACRO_TEST(high_snapshot_plus_high_accept_uses_relative_indexing) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };

    paxos_hard_state_t hs = { .promised_ballot = 10, .max_generated_ballot = 10 };
    uint64_t snap_idx = 5000000;

    paxos_restore_data_t rd = {
        .struct_size = sizeof(paxos_restore_data_t),
        .hard_state = hs,
        .local_commit_index = snap_idx,
        .snapshot_index = snap_idx,
        .entries = NULL,
        .num_entries = 0
    };

    paxos_t* p;
    paxos_restore(&cfg, &rd, &p);

    paxos_log_accept(p, 5000005, 10, PAXOS_ENTRY_NORMAL, 0, 0, (uint8_t*)"A", 1);

    MACRO_ASSERT_TRUE(p->log_chunks_cap < 100);
    MACRO_ASSERT_TRUE(p->log_chunks[0] != NULL);

    paxos_entry_t* e = paxos_log_get(p, 5000005);
    MACRO_ASSERT_TRUE(e != NULL);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, promise_exceeding_recovery_gap_fatals_to_prevent_dos);
    MACRO_ADD(tests, high_snapshot_plus_high_accept_uses_relative_indexing);
    macro_run_all("paxos_recovery_limits", tests, test_count);
    return 0;
}

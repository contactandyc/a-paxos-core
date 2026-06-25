// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(chosen_slot_preserves_state_on_safe_higher_ballot_accept) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    paxos_log_accept(p, 1, 5, ENTRY_NORMAL, 0, 0, (uint8_t*)"DATA", 4);

    // FIXED: Use the official macros to find the correct memory location!
    uint64_t c_idx = paxos_chunk_idx(p, 1);
    uint64_t c_off = paxos_chunk_off(1);
    p->log_chunks[c_idx]->slots[c_off].chosen = true;

    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"DATA", .data_len = 4 };
    paxos_msg_t acc = { .type = MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = &e, .num_entries = 1 };
    paxos_step_remote(p, &acc);

    MACRO_ASSERT_TRUE(p->fatal_error == false);
    MACRO_ASSERT_TRUE(p->log_chunks[c_idx]->slots[c_off].chosen == true);
    MACRO_ASSERT_EQ_INT(p->log_chunks[c_idx]->slots[c_off].entry.accepted_ballot, 10);

    paxos_destroy(p);
}

MACRO_TEST(chosen_slot_fatals_on_unsafe_higher_ballot_accept) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    paxos_log_accept(p, 1, 5, ENTRY_NORMAL, 0, 0, (uint8_t*)"DATA", 4);

    uint64_t c_idx = paxos_chunk_idx(p, 1);
    uint64_t c_off = paxos_chunk_off(1);
    p->log_chunks[c_idx]->slots[c_off].chosen = true;

    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"EVIL", .data_len = 4 };
    paxos_msg_t acc = { .type = MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = &e, .num_entries = 1 };
    paxos_step_remote(p, &acc);

    MACRO_ASSERT_TRUE(p->fatal_error == true);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, chosen_slot_preserves_state_on_safe_higher_ballot_accept);
    MACRO_ADD(tests, chosen_slot_fatals_on_unsafe_higher_ballot_accept);
    macro_run_all("paxos_chosen_invariants", tests, test_count);
    return 0;
}

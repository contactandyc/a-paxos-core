// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(restored_accepted_entries_do_not_become_chosen) {
    uint64_t peers[] = {1, 2, 3};
    paxos_hard_state_t hs = { .promised_ballot = 15 };
    paxos_entry_t e1 = { .slot = 1, .accepted_ballot = 10, .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };

    paxos_restored_entry_t restored_arr[1] = { { .entry = e1, .chosen = false } };
    paxos_t* p = paxos_restore(1, peers, 3, hs, 0, 0, restored_arr, 1);

    uint64_t c_idx = 1 / PAXOS_LOG_CHUNK_SIZE;
    uint64_t c_off = 1 % PAXOS_LOG_CHUNK_SIZE;
    MACRO_ASSERT_TRUE(p->log_chunks[c_idx]->slots[c_off].is_chosen == false);

    paxos_destroy(p);
}

MACRO_TEST(incoming_promise_with_stack_literal_data_must_not_crash) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);

    char stack_buffer[] = "STACK_DATA";
    paxos_entry_t e = { .slot = 1, .accepted_ballot = 5, .type = ENTRY_NORMAL, .data = (uint8_t*)stack_buffer, .data_len = 10 };
    paxos_msg_t prom = { .type = MSG_PROMISE, .to = 1, .from = 2, .ballot = p->active_ballot, .entries = &e, .num_entries = 1 };

    paxos_step_remote(p, &prom);
    MACRO_ASSERT_TRUE(p->fatal_error == false);

    paxos_destroy(p);
}

MACRO_TEST(allocation_failure_in_accept_must_not_destroy_old_value) {
    uint64_t peers[] = {1, 2};
    paxos_t* p = paxos_create(1, peers, 2);

    paxos_log_accept(p, 1, 5, ENTRY_NORMAL, 0, 0, (uint8_t*)"OLD", 3);
    paxos_log_accept(p, 1, 10, ENTRY_NORMAL, 0, 0, (uint8_t*)"NEW", PAXOS_MAX_PAYLOAD_SIZE + 1);

    paxos_entry_t* e = paxos_log_get(p, 1);
    MACRO_ASSERT_TRUE(e != NULL);
    MACRO_ASSERT_EQ_INT(e->accepted_ballot, 5);
    MACRO_ASSERT_TRUE(memcmp(e->data, "OLD", 3) == 0);

    paxos_destroy(p);
}

MACRO_TEST(single_node_cluster_commits_past_inflight_window) {
    uint64_t peers[] = {1};
    paxos_t* p = paxos_create(1, peers, 1);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);

    for (int i = 0; i < 5000; i++) {
        paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
        paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
        paxos_step_local(p, &prop);
    }

    MACRO_ASSERT_EQ_INT(p->local_commit_index, 5001);

    paxos_destroy(p);
}

MACRO_TEST(same_slot_and_ballot_with_different_data_causes_fatal_error) {
    uint64_t peers[] = {1, 2};
    paxos_t* p = paxos_create(1, peers, 2);
    p->promised_ballot = 10;

    paxos_log_accept(p, 1, 10, ENTRY_NORMAL, 0, 0, (uint8_t*)"AAA", 3);

    paxos_entry_t rogue_e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"BBB", .data_len = 3 };
    paxos_msg_t acc = { .type = MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = &rogue_e, .num_entries = 1 };

    paxos_step_remote(p, &acc);
    MACRO_ASSERT_TRUE(p->fatal_error == true);

    paxos_destroy(p);
}

MACRO_TEST(paxos_log_learn_chosen_clone_failure_does_not_set_chosen) {
    uint64_t peers[] = {1, 2};
    paxos_t* p = paxos_create(1, peers, 2);

    // Triggering malloc failure using SIZE_MAX prevents memcpy out-of-bounds reads
    paxos_entry_t huge_e = { .slot = 1, .accepted_ballot = 10, .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = (size_t)-1 };

    bool success = paxos_log_learn_chosen(p, 1, &huge_e);

    MACRO_ASSERT_TRUE(success == false);
    MACRO_ASSERT_TRUE(p->fatal_error == true);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, restored_accepted_entries_do_not_become_chosen);
    MACRO_ADD(tests, incoming_promise_with_stack_literal_data_must_not_crash);
    MACRO_ADD(tests, allocation_failure_in_accept_must_not_destroy_old_value);
    MACRO_ADD(tests, single_node_cluster_commits_past_inflight_window);
    MACRO_ADD(tests, same_slot_and_ballot_with_different_data_causes_fatal_error);
    MACRO_ADD(tests, paxos_log_learn_chosen_clone_failure_does_not_set_chosen);
    macro_run_all("paxos_advanced_memory", tests, test_count);
    return 0;
}

// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(paxos_log_dynamically_resizes_on_sparse_accept) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    MACRO_ASSERT_EQ_INT(p->log_cap, 1024);

    uint64_t huge_slot = 5000;
    bool success = paxos_log_accept(p, huge_slot, 2, ENTRY_NORMAL, 1, 1, (uint8_t*)"DATA", 4);

    MACRO_ASSERT_TRUE(success);
    MACRO_ASSERT_FALSE(p->fatal_error);
    MACRO_ASSERT_TRUE(p->log_cap > huge_slot);

    paxos_entry_t* e = paxos_log_get(p, huge_slot);
    MACRO_ASSERT_TRUE(e != NULL);
    MACRO_ASSERT_EQ_INT(e->slot, huge_slot);
    MACRO_ASSERT_TRUE(memcmp(e->data, "DATA", 4) == 0);

    paxos_destroy(p);
}

MACRO_TEST(paxos_recovery_buffer_dynamically_resizes_on_high_promise) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    MACRO_ASSERT_EQ_INT(p->recovery_cap, 1024);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);

    uint64_t huge_slot = 8500;
    paxos_entry_t e = { .slot = huge_slot, .accepted_ballot = 1, .type = ENTRY_NORMAL, .data = NULL, .data_len = 0 };

    paxos_msg_t prom = {
        .type = MSG_PROMISE,
        .to = 1,
        .from = 2,
        .ballot = p->active_ballot,
        .entries = &e,
        .num_entries = 1
    };

    paxos_step_remote(p, &prom);

    MACRO_ASSERT_FALSE(p->fatal_error);
    MACRO_ASSERT_TRUE(p->recovery_cap > huge_slot);
    MACRO_ASSERT_TRUE(p->recovery_buffer[huge_slot].has_value);
    MACRO_ASSERT_EQ_INT(p->recovery_buffer[huge_slot].recovered_value.slot, huge_slot);

    paxos_destroy(p);
}

MACRO_TEST(paxos_message_queue_dynamically_resizes) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    MACRO_ASSERT_EQ_INT(p->msg_queue_after_persist_cap, 0);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);
    paxos_msg_t prom = { .type = MSG_PROMISE, .to = 1, .from = 2, .ballot = p->active_ballot };
    paxos_step_remote(p, &prom);

    paxos_advance(p, 0, 0);
    MACRO_ASSERT_EQ_INT(p->msg_queue_after_persist_len, 0);

    for (int i = 0; i < 100; i++) {
        paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
        paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
        paxos_step_local(p, &prop);
    }

    MACRO_ASSERT_FALSE(p->fatal_error);
    MACRO_ASSERT_EQ_INT(p->msg_queue_after_persist_len, 200);
    MACRO_ASSERT_TRUE(p->msg_queue_after_persist_cap >= 200);

    paxos_destroy(p);
}

MACRO_TEST(paxos_advance_safely_frees_deep_copied_promise_suffixes) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    paxos_log_accept(p, 5, 1, ENTRY_NORMAL, 1, 1, (uint8_t*)"DEEP_COPY_ME", 12);

    paxos_msg_t prep = { .type = MSG_PREPARE, .to = 1, .from = 2, .ballot = 10, .slot = 1 };
    paxos_step_remote(p, &prep);

    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready.num_messages_after_persist, 1);
    MACRO_ASSERT_TRUE(ready.messages_after_persist[0].type == MSG_PROMISE);
    MACRO_ASSERT_EQ_INT(ready.messages_after_persist[0].num_entries, 1);

    MACRO_ASSERT_TRUE(ready.messages_after_persist[0].entries[0].data != p->log[5 - p->log_base_slot].entry.data);
    MACRO_ASSERT_TRUE(memcmp(ready.messages_after_persist[0].entries[0].data, "DEEP_COPY_ME", 12) == 0);

    paxos_ready_destroy(&ready); // Free the ready extractions
    paxos_advance(p, 0, 0); // Free the internal message queue

    MACRO_ASSERT_EQ_INT(p->msg_queue_after_persist_len, 0);
    MACRO_ASSERT_FALSE(p->fatal_error);

    paxos_destroy(p);
}

MACRO_TEST(paxos_extract_suffix_skips_empty_gaps) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    paxos_log_accept(p, 2, 1, ENTRY_NORMAL, 1, 1, (uint8_t*)"2", 1);
    paxos_log_accept(p, 4, 1, ENTRY_NORMAL, 1, 1, (uint8_t*)"4", 1);

    size_t count = 0;
    paxos_entry_t* suffix = paxos_log_extract_suffix(p, 1, &count);

    MACRO_ASSERT_EQ_INT(count, 2);
    MACRO_ASSERT_TRUE(suffix != NULL);
    MACRO_ASSERT_EQ_INT(suffix[0].slot, 2);
    MACRO_ASSERT_EQ_INT(suffix[1].slot, 4);

    free(suffix[0].data);
    free(suffix[1].data);
    free(suffix);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_log_dynamically_resizes_on_sparse_accept);
    MACRO_ADD(tests, paxos_recovery_buffer_dynamically_resizes_on_high_promise);
    MACRO_ADD(tests, paxos_message_queue_dynamically_resizes);
    MACRO_ADD(tests, paxos_advance_safely_frees_deep_copied_promise_suffixes);
    MACRO_ADD(tests, paxos_extract_suffix_skips_empty_gaps);

    macro_run_all("paxos_memory", tests, test_count);
    return 0;
}

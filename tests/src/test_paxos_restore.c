// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(paxos_firewall_rejects_oversized_payloads) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    p->state = PAXOS_STATE_ACTIVE;

    paxos_entry_t e = { .type = ENTRY_NORMAL, .data_len = PAXOS_MAX_PAYLOAD_SIZE + 1 };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    paxos_step_local(p, &prop);

    // The firewall MUST drop the message before it hits the log.
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 0);
    MACRO_ASSERT_FALSE(p->fatal_error);

    paxos_destroy(p);
}

MACRO_TEST(paxos_firewall_rejects_null_entry_arrays) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    // Malformed Accept message claims 1 entry but provides a NULL pointer
    paxos_msg_t acc = { .type = MSG_ACCEPT, .to = 1, .from = 2, .ballot = 5, .slot = 1, .entries = NULL, .num_entries = 1 };

    paxos_step_remote(p, &acc);

    // If the firewall failed, the core would segfault here.
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 0);
    MACRO_ASSERT_FALSE(p->fatal_error);

    paxos_destroy(p);
}

MACRO_TEST(paxos_node_can_crash_and_restore_from_disk) {
    uint64_t peers[] = {2, 3};

    paxos_hard_state_t hs = {
        .promised_ballot = 15,
        .max_generated_ballot = 15,
        .has_update = false
    };

    paxos_entry_t entries[2];
    entries[0].slot = 101;
    entries[0].accepted_ballot = 15;
    entries[0].type = ENTRY_NORMAL;
    entries[0].client_id = 1;
    entries[0].client_seq = 1;
    entries[0].data = (uint8_t*)"A";
    entries[0].data_len = 1;

    entries[1].slot = 102;
    entries[1].accepted_ballot = 15;
    entries[1].type = ENTRY_NORMAL;
    entries[1].client_id = 1;
    entries[1].client_seq = 2;
    entries[1].data = (uint8_t*)"B";
    entries[1].data_len = 1;

    // Simulate restoring a node that crashed after taking a snapshot at slot 100
    // and accepting slots 101 and 102.
    paxos_t* p = paxos_restore(1, peers, 2, hs, 100, 100, entries, 2);

    MACRO_ASSERT_TRUE(p != NULL);
    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_LEARNER);
    MACRO_ASSERT_EQ_INT(paxos_promised_ballot(p), 15);
    MACRO_ASSERT_EQ_INT(paxos_snapshot_index(p), 100);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 102);

    // Verify it correctly populated the sparse log array mathematically
    paxos_entry_t* e = paxos_log_get(p, 102);
    MACRO_ASSERT_TRUE(e != NULL);
    MACRO_ASSERT_TRUE(memcmp(e->data, "B", 1) == 0);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_firewall_rejects_oversized_payloads);
    MACRO_ADD(tests, paxos_firewall_rejects_null_entry_arrays);
    MACRO_ADD(tests, paxos_node_can_crash_and_restore_from_disk);

    macro_run_all("paxos_restore", tests, test_count);
    return 0;
}

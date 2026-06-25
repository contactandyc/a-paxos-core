// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(crash_after_hard_state_persisted_but_before_entries) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    // Node gets a prepare, updates promised_ballot, but crashes before saving entries
    paxos_msg_t prep = { .type = MSG_PREPARE, .to = 1, .from = 2, .ballot = 25, .slot = 1 };
    paxos_step_remote(p, &prep);

    paxos_ready_t ready = paxos_get_ready(p);

    // Host saves hard state to disk...
    paxos_hard_state_t saved_hs = ready.hard_state;
    // ...CRASH happens here...

    paxos_ready_destroy(&ready);
    paxos_destroy(p);

    // Reboot engine using only the saved hard state
    paxos_t* p2 = paxos_restore(1, peers, 3, saved_hs, 0, 0, NULL, 0);

    // It should remember the ballot, preventing it from voting for older epochs
    MACRO_ASSERT_EQ_INT(p2->promised_ballot, 25);

    paxos_destroy(p2);
}

MACRO_TEST(crash_after_entries_persisted_but_before_advance) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);
    p->promised_ballot = 10;
    p->leader_id = 2;

    // Node accepts data, gets Ready
    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };
    paxos_msg_t acc = { .type = MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = &e, .num_entries = 1 };
    paxos_step_remote(p, &acc);

    paxos_ready_t ready = paxos_get_ready(p);

    // Host saves hard state and entries...
    paxos_hard_state_t saved_hs = ready.hard_state;
    paxos_restored_entry_t saved_disk[1] = {
        { .entry = ready.entries_to_save[0], .chosen = false }
    };

    // ...CRASH happens before paxos_advance() or sending after_persist messages

    paxos_ready_destroy(&ready);
    paxos_destroy(p);

    // Reboot engine using the saved disk state
    paxos_t* p2 = paxos_restore(1, peers, 3, saved_hs, 0, 0, saved_disk, 1);

    // The entry must be successfully loaded but explicitly NOT chosen yet
    paxos_entry_t* recovered = paxos_log_get(p2, 1);
    MACRO_ASSERT_TRUE(recovered != NULL);
    MACRO_ASSERT_EQ_INT(recovered->accepted_ballot, 10);

    uint64_t c_idx = (1 - p2->log_base_slot) / PAXOS_LOG_CHUNK_SIZE;
    uint64_t c_off = (1 - p2->log_base_slot) % PAXOS_LOG_CHUNK_SIZE;
    MACRO_ASSERT_TRUE(p2->log_chunks[c_idx]->slots[c_off].chosen == false);

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

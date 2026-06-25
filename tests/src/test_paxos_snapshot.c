// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(paxos_compact_removes_old_slots_and_advances_bounds) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    paxos_log_accept(p, 1, 1, ENTRY_NORMAL, 1, 1, (uint8_t*)"1", 1);
    paxos_log_accept(p, 2, 1, ENTRY_NORMAL, 1, 1, (uint8_t*)"2", 1);
    paxos_log_accept(p, 3, 1, ENTRY_NORMAL, 1, 1, (uint8_t*)"3", 1);

    p->last_applied = 2; // Can only compact up to last_applied

    paxos_compact(p, 2);

    MACRO_ASSERT_EQ_INT(p->snapshot_index, 2);
    MACRO_ASSERT_EQ_INT(p->log_base_slot, 3);

    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL); // Successfully zeroed!
    MACRO_ASSERT_TRUE(paxos_log_get(p, 2) == NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 3) != NULL);

    paxos_destroy(p);
}

MACRO_TEST(leader_rejects_fetch_entries_with_snapshot_install) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    p->state = PAXOS_STATE_ACTIVE;
    p->active_ballot = 5;
    p->snapshot_index = 100; // Leader has already compacted the first 100 slots

    // Follower asks for slot 50
    paxos_msg_t fetch = { .type = MSG_FETCH_ENTRIES, .to = 1, .from = 2, .slot = 50, .commit_index = 105 };
    paxos_step_remote(p, &fetch);

    paxos_ready_t ready = paxos_get_ready(p);

    // Leader MUST NOT return FETCH_ENTRIES_RES with reject=true.
    // It MUST immediately kick off the streaming SNAPSHOT logic to rescue the offline node!
    MACRO_ASSERT_EQ_INT(ready.num_messages_immediate, 1);
    MACRO_ASSERT_TRUE(ready.messages_immediate[0].type == MSG_INSTALL_SNAPSHOT);
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].slot, 100); // Snapshot horizon
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].snapshot_offset, 0); // Start of stream

    paxos_ready_destroy(&ready);
    paxos_destroy(p);
}

MACRO_TEST(follower_accepts_snapshot_chunk_and_resets_log_on_completion) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    paxos_log_accept(p, 1, 1, ENTRY_NORMAL, 1, 1, (uint8_t*)"GARBAGE", 7);

    // Leader sends a snapshot
    paxos_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT,
        .to = 1,
        .from = 2,
        .ballot = 5,
        .slot = 100, // Leader's snapshot covers everything up to 100
        .snapshot_data = (uint8_t*)"DB_STATE",
        .snapshot_len = 8,
        .snapshot_offset = 0,
        .snapshot_done = true
    };
    paxos_step_remote(p, &snap);

    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_TRUE(ready.install_snapshot);
    MACRO_ASSERT_EQ_INT(ready.snapshot_slot, 100);
    MACRO_ASSERT_EQ_INT(ready.snapshot_len, 8);
    MACRO_ASSERT_EQ_INT(ready.num_messages_immediate, 0); // No ACK sent until Host Application says it's on disk!

    // Host app finishes writing to disk
    paxos_snapshot_acked(p, true);

    paxos_ready_t ready_ack = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready_ack.num_messages_immediate, 1);
    MACRO_ASSERT_TRUE(ready_ack.messages_immediate[0].type == MSG_INSTALL_SNAPSHOT_RES);

    // Engine completely wipes old data and fast-forwards commit/applied
    MACRO_ASSERT_EQ_INT(p->snapshot_index, 100);
    MACRO_ASSERT_EQ_INT(p->last_applied, 100);
    MACRO_ASSERT_EQ_INT(p->local_commit_index, 100);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL);

    paxos_ready_destroy(&ready);
    paxos_ready_destroy(&ready_ack);
    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_compact_removes_old_slots_and_advances_bounds);
    MACRO_ADD(tests, leader_rejects_fetch_entries_with_snapshot_install);
    MACRO_ADD(tests, follower_accepts_snapshot_chunk_and_resets_log_on_completion);

    macro_run_all("paxos_snapshot", tests, test_count);
    return 0;
}

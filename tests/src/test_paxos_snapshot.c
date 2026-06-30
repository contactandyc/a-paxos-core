// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(paxos_compact_removes_old_slots_and_advances_bounds) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_log_accept(p, 1, 1, PAXOS_ENTRY_NORMAL, 1, 1, (uint8_t*)"1", 1);
    paxos_log_accept(p, 2, 1, PAXOS_ENTRY_NORMAL, 1, 1, (uint8_t*)"2", 1);
    paxos_log_accept(p, 3, 1, PAXOS_ENTRY_NORMAL, 1, 1, (uint8_t*)"3", 1);

    p->last_applied = 2;

    paxos_compact(p, 2);

    MACRO_ASSERT_EQ_INT(p->snapshot_index, 2);
    MACRO_ASSERT_EQ_INT(p->log_base_slot, 3);

    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 2) == NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 3) != NULL);

    paxos_destroy(p);
}

MACRO_TEST(leader_rejects_fetch_entries_with_snapshot_install) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    p->state = PAXOS_STATE_ACTIVE;
    p->active_ballot = 5;
    p->snapshot_index = 100;

    paxos_msg_t fetch = {
        .type = PAXOS_MSG_FETCH_ENTRIES,
        .to = 1,
        .from = 2,
        .ballot = 5,
        .slot = 50,
        .commit_index = 105
    };
    (void)paxos_receive(p, &fetch);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);

    MACRO_ASSERT_EQ_INT(ready.num_messages_immediate, 1);
    MACRO_ASSERT_TRUE(ready.messages_immediate[0].type == PAXOS_MSG_INSTALL_SNAPSHOT);
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].slot, 100);
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].snapshot_offset, 0);

    paxos_destroy(p);
}

MACRO_TEST(follower_accepts_snapshot_chunk_and_resets_log_on_completion) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_log_accept(p, 1, 1, PAXOS_ENTRY_NORMAL, 1, 1, (uint8_t*)"GARBAGE", 7);

    paxos_msg_t snap = {
        .type = PAXOS_MSG_INSTALL_SNAPSHOT,
        .to = 1,
        .from = 2,
        .ballot = 5,
        .slot = 100,
        .snapshot_data = (uint8_t*)"DB_STATE",
        .snapshot_len = 8,
        .snapshot_offset = 0,
        .snapshot_done = true
    };
    (void)paxos_receive(p, &snap);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);
    MACRO_ASSERT_TRUE(ready.install_snapshot);
    MACRO_ASSERT_EQ_INT(ready.snapshot_slot, 100);
    MACRO_ASSERT_EQ_INT(ready.snapshot_len, 8);
    MACRO_ASSERT_EQ_INT(ready.num_messages_immediate, 0);

    paxos_snapshot_acked(p, true);

    paxos_ready_t ready_ack;
    (void)paxos_get_ready(p, &ready_ack);
    MACRO_ASSERT_EQ_INT(ready_ack.num_messages_immediate, 1);
    MACRO_ASSERT_TRUE(ready_ack.messages_immediate[0].type == PAXOS_MSG_INSTALL_SNAPSHOT_RES);

    MACRO_ASSERT_EQ_INT(p->snapshot_index, 100);
    MACRO_ASSERT_EQ_INT(p->last_applied, 100);
    MACRO_ASSERT_EQ_INT(p->local_commit_index, 100);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL);

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

// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(stale_commit_notice_from_lower_ballot_is_ignored) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    p->promised_ballot = 20;

    paxos_msg_t stale = { .type = MSG_COMMIT_NOTICE, .to = 1, .from = 2, .ballot = 10, .commit_index = 5 };
    paxos_step_remote(p, &stale);

    MACRO_ASSERT_EQ_INT(p->leader_commit_hint, 0);

    paxos_destroy(p);
}

MACRO_TEST(fetch_entries_response_from_non_leader_is_ignored) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    p->promised_ballot = 10;
    p->leader_id = 2;

    paxos_entry_t rogue_e = { .slot = 1, .accepted_ballot = 10, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };

    paxos_msg_t rogue_fetch = { .type = MSG_FETCH_ENTRIES_RES, .to = 1, .from = 3, .ballot = 10, .entries = &rogue_e, .num_entries = 1 };
    paxos_step_remote(p, &rogue_fetch);

    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL);

    paxos_destroy(p);
}

MACRO_TEST(snapshot_install_handles_duplicate_and_wrong_offsets) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    p->promised_ballot = 10;

    uint8_t chunk1[] = "CHUNK_ONE";
    uint8_t chunk2[] = "CHUNK_TWO";

    // 1. Valid Chunk 1
    paxos_msg_t s1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .ballot = 10, .slot = 100, .snapshot_offset = 0, .snapshot_len = 9, .snapshot_data = chunk1 };
    paxos_step_remote(p, &s1);

    MACRO_ASSERT_EQ_INT(p->expected_snapshot_offset, 9);

    // FIXED: The host application MUST acknowledge it safely wrote Chunk 1 to disk!
    paxos_snapshot_acked(p, true);
    p->msg_queue_immediate_len = 0; // Clear the queue to isolate the next test phase

    // 2. Duplicate Chunk 1 (Network replay)
    paxos_step_remote(p, &s1);
    paxos_ready_t r = paxos_get_ready(p);

    MACRO_ASSERT_TRUE(r.num_messages_immediate > 0);
    MACRO_ASSERT_EQ_INT(r.messages_immediate[r.num_messages_immediate - 1].reject, true);
    MACRO_ASSERT_EQ_INT(r.messages_immediate[r.num_messages_immediate - 1].slot, 9);
    paxos_ready_destroy(&r);

    // 3. Wrong Offset (Gap in the network)
    p->msg_queue_immediate_len = 0;
    paxos_msg_t gap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .ballot = 10, .slot = 100, .snapshot_offset = 50, .snapshot_len = 9, .snapshot_data = chunk2 };
    paxos_step_remote(p, &gap);

    r = paxos_get_ready(p);
    MACRO_ASSERT_TRUE(r.num_messages_immediate > 0);
    MACRO_ASSERT_EQ_INT(r.messages_immediate[r.num_messages_immediate - 1].reject, true);
    MACRO_ASSERT_EQ_INT(r.messages_immediate[r.num_messages_immediate - 1].slot, 9);
    paxos_ready_destroy(&r);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, stale_commit_notice_from_lower_ballot_is_ignored);
    MACRO_ADD(tests, fetch_entries_response_from_non_leader_is_ignored);
    MACRO_ADD(tests, snapshot_install_handles_duplicate_and_wrong_offsets);
    macro_run_all("paxos_protocol_edges", tests, test_count);
    return 0;
}

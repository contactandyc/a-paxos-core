// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

static void force_active_leader(paxos_t* p) {
    extern void paxos_proposer_campaign(paxos_t* p);
    (void)paxos_proposer_campaign(p);
    paxos_advance(p, NULL, 0, 0);
    if (p->num_nodes > 1) {
        uint64_t remote_peer = p->node_directory[1];

        paxos_msg_t prom = { .type = PAXOS_MSG_PROMISE, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .num_entries = 0 };
    (void)paxos_receive(p, &prom);
        paxos_advance(p, NULL, 0, 0);

        paxos_msg_t ack = { .type = PAXOS_MSG_ACCEPTED, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .slot = p->next_slot - 1 };
    (void)paxos_receive(p, &ack);
        paxos_advance(p, NULL, 0, 0);
    }
}

MACRO_TEST(stale_commit_notice_from_lower_ballot_is_ignored) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    p->promised_ballot = 20;

    paxos_msg_t stale = { .type = PAXOS_MSG_COMMIT_NOTICE, .to = 1, .from = 2, .ballot = 10, .commit_index = 5 };
    (void)paxos_receive(p, &stale);

    MACRO_ASSERT_EQ_INT(p->leader_commit_hint, 0);

    paxos_destroy(p);
}

MACRO_TEST(fetch_entries_response_from_non_leader_is_ignored) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    p->promised_ballot = 10;
    p->leader_id = 2;

    paxos_entry_t rogue_e = { .slot = 1, .accepted_ballot = 10, .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };

    paxos_msg_t rogue_fetch = { .type = PAXOS_MSG_FETCH_ENTRIES_RES, .to = 1, .from = 3, .ballot = 10, .entries = &rogue_e, .num_entries = 1 };
    (void)paxos_receive(p, &rogue_fetch);

    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL);

    paxos_destroy(p);
}

MACRO_TEST(snapshot_install_handles_duplicate_and_wrong_offsets) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    p->promised_ballot = 10;

    uint8_t chunk1[] = "CHUNK_ONE";
    uint8_t chunk2[] = "CHUNK_TWO";

    paxos_msg_t s1 = { .type = PAXOS_MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .ballot = 10, .slot = 100, .snapshot_offset = 0, .snapshot_len = 9, .snapshot_data = chunk1 };
    (void)paxos_receive(p, &s1);

    MACRO_ASSERT_EQ_INT(p->expected_snapshot_offset, 9);

    paxos_snapshot_acked(p, true);
    p->msg_queue_immediate_len = 0;

    (void)paxos_receive(p, &s1);

    paxos_ready_t r;
    (void)paxos_get_ready(p, &r);

    MACRO_ASSERT_TRUE(r.num_messages_immediate > 0);
    MACRO_ASSERT_EQ_INT(r.messages_immediate[r.num_messages_immediate - 1].reject, true);
    MACRO_ASSERT_EQ_INT(r.messages_immediate[r.num_messages_immediate - 1].slot, 9);

    p->msg_queue_immediate_len = 0;
    paxos_msg_t gap = { .type = PAXOS_MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .ballot = 10, .slot = 100, .snapshot_offset = 50, .snapshot_len = 9, .snapshot_data = chunk2 };
    (void)paxos_receive(p, &gap);

    (void)paxos_get_ready(p, &r);
    MACRO_ASSERT_TRUE(r.num_messages_immediate > 0);
    MACRO_ASSERT_EQ_INT(r.messages_immediate[r.num_messages_immediate - 1].reject, true);
    MACRO_ASSERT_EQ_INT(r.messages_immediate[r.num_messages_immediate - 1].slot, 9);

    paxos_destroy(p);
}

MACRO_TEST(msg_accepted_with_wrong_hash_is_ignored) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    force_active_leader(p);

    (void)paxos_propose(p, 0, 0, "X", 1);

    paxos_msg_t ack = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2, .num_entries = 1, .value_hash = 999999999 };

    (void)paxos_receive(p, &ack);

    MACRO_ASSERT_EQ_INT(p->local_commit_index, 1);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, stale_commit_notice_from_lower_ballot_is_ignored);
    MACRO_ADD(tests, fetch_entries_response_from_non_leader_is_ignored);
    MACRO_ADD(tests, snapshot_install_handles_duplicate_and_wrong_offsets);
    MACRO_ADD(tests, msg_accepted_with_wrong_hash_is_ignored);
    macro_run_all("paxos_protocol_edges", tests, test_count);
    return 0;
}

// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

static void force_active_leader(paxos_t* p) {
    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);
    paxos_advance(p, 0, 0);
    if (p->num_nodes > 1) {
        uint64_t remote_peer = p->node_directory[1];

        paxos_msg_t prom = { .type = MSG_PROMISE, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .num_entries = 0 };
        paxos_step_remote(p, &prom);
        paxos_advance(p, 0, 0);

        paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .slot = p->next_slot - 1 };
        paxos_step_remote(p, &ack);
        paxos_advance(p, 0, 0);
    }
}

MACRO_TEST(paxos_applies_config_immediately_to_bitmask) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    uint64_t new_node = 4;
    paxos_entry_t e_conf = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    paxos_msg_t p_conf = { .type = MSG_PROPOSE, .entries = &e_conf, .num_entries = 1 };

    // Core accepts the config and immediately updates the active bitmask
    paxos_step_local(p, &p_conf);

    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 2); // NoOp at 1, Config at 2

    // The directory expands from 3 nodes (1, 2, 3) to 4 nodes
    MACRO_ASSERT_EQ_INT(p->num_nodes, 4);

    // Bits 0, 1, 2, and 3 should be set (1 + 2 + 4 + 8 = 15)
    MACRO_ASSERT_EQ_INT(p->active_config_mask, 15);

    paxos_destroy(p);
}

MACRO_TEST(paxos_rejects_malformed_proposals) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = NULL, .data_len = 50 };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    paxos_step_local(p, &prop);

    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);

    paxos_destroy(p);
}

MACRO_TEST(paxos_leader_steps_down_on_higher_prepare) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_ACTIVE);

    uint64_t higher_ballot = p->active_ballot + 65536;
    paxos_msg_t prep = { .type = MSG_PREPARE, .to = 1, .from = 2, .ballot = higher_ballot, .slot = 1 };
    paxos_step_remote(p, &prep);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_LEARNER);

    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    paxos_step_local(p, &prop);

    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);

    paxos_destroy(p);
}

MACRO_TEST(paxos_stale_follower_fetches_truth_instead_of_corrupting) {
    uint64_t peers[] = {1, 3};
    paxos_t* p = paxos_create(2, peers, 2);

    paxos_log_accept(p, 1, 5, ENTRY_NORMAL, 0, 0, (uint8_t*)"X", 1);
    p->promised_ballot = 10;

    paxos_msg_t commit = { .type = MSG_COMMIT_NOTICE, .to = 2, .from = 1, .ballot = 10, .commit_index = 1 };
    paxos_step_remote(p, &commit);

    MACRO_ASSERT_EQ_INT(p->local_commit_index, 0);

    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready.num_messages_immediate, 1);
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].type, MSG_FETCH_ENTRIES);

    paxos_ready_destroy(&ready);
    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_applies_config_immediately_to_bitmask);
    MACRO_ADD(tests, paxos_rejects_malformed_proposals);
    MACRO_ADD(tests, paxos_leader_steps_down_on_higher_prepare);
    MACRO_ADD(tests, paxos_stale_follower_fetches_truth_instead_of_corrupting);

    macro_run_all("paxos_strict_safety", tests, test_count);
    return 0;
}

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

MACRO_TEST(paxos_safely_accepts_config_and_engages_lock) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    uint64_t new_node = 4;
    paxos_register_learner(p, new_node);
    paxos_msg_t catch_up_ack = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 4, .ballot = p->active_ballot, .slot = 1 };
    (void)paxos_receive(p, &catch_up_ack);

    paxos_add_node(p, new_node);

    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 2);
    MACRO_ASSERT_TRUE(p->pending_reconfig == true);

    paxos_destroy(p);
}

MACRO_TEST(paxos_rejects_malformed_proposals) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    paxos_err_t err = paxos_propose(p, 0, 0, NULL, 50);

    MACRO_ASSERT_EQ_INT(err, PAXOS_ERR_NOMEM);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);

    paxos_destroy(p);
}

MACRO_TEST(paxos_leader_steps_down_on_higher_prepare) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_ACTIVE);

    uint64_t higher_ballot = p->active_ballot + 65536;
    paxos_msg_t prep = { .type = PAXOS_MSG_PREPARE, .to = 1, .from = 2, .ballot = higher_ballot, .slot = 1 };
    (void)paxos_receive(p, &prep);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_LEARNER);

    paxos_err_t err = paxos_propose(p, 0, 0, "X", 1);
    MACRO_ASSERT_EQ_INT(err, PAXOS_ERR_NOT_ACTIVE);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);

    paxos_destroy(p);
}

MACRO_TEST(paxos_stale_follower_fetches_truth_instead_of_corrupting) {
    uint64_t peers[] = {1, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 2, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_log_accept(p, 1, 5, PAXOS_ENTRY_NORMAL, 0, 0, (uint8_t*)"X", 1);
    p->promised_ballot = 10;

    paxos_msg_t commit = { .type = PAXOS_MSG_COMMIT_NOTICE, .to = 2, .from = 1, .ballot = 10, .commit_index = 1 };
    (void)paxos_receive(p, &commit);

    MACRO_ASSERT_EQ_INT(p->local_commit_index, 0);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);
    MACRO_ASSERT_EQ_INT(ready.num_messages_immediate, 1);
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].type, PAXOS_MSG_FETCH_ENTRIES);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_safely_accepts_config_and_engages_lock);
    MACRO_ADD(tests, paxos_rejects_malformed_proposals);
    MACRO_ADD(tests, paxos_leader_steps_down_on_higher_prepare);
    MACRO_ADD(tests, paxos_stale_follower_fetches_truth_instead_of_corrupting);

    macro_run_all("paxos_strict_safety", tests, test_count);
    return 0;
}

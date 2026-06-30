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

MACRO_TEST(paxos_read_single_node_instant_ready) {
    uint64_t peers[] = {1};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 1 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    (void)paxos_read_barrier(p, 100);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 1);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].read_seq, 100);

    paxos_destroy(p);
}

MACRO_TEST(paxos_read_multi_node_waits_for_quorum) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    (void)paxos_read_barrier(p, 500);

    paxos_ready_t ready1;
    (void)paxos_get_ready(p, &ready1);
    MACRO_ASSERT_EQ_INT(ready1.num_read_states, 0);
    MACRO_ASSERT_EQ_INT(ready1.num_messages_immediate, 2);

    paxos_msg_t res = {
        .type = PAXOS_MSG_READ_BARRIER_RES,
        .to = 1,
        .from = 2,
        .ballot = p->active_ballot,
        .read_seq = 500
    };
    (void)paxos_receive(p, &res);

    paxos_ready_t ready2;
    (void)paxos_get_ready(p, &ready2);
    MACRO_ASSERT_EQ_INT(ready2.num_read_states, 1);
    MACRO_ASSERT_EQ_INT(ready2.read_states[0].read_seq, 500);

    paxos_destroy(p);
}

MACRO_TEST(paxos_read_rejected_if_not_active) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_err_t err = paxos_read_barrier(p, 100);
    MACRO_ASSERT_EQ_INT(err, PAXOS_ERR_NOT_ACTIVE);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, paxos_read_single_node_instant_ready);
    MACRO_ADD(tests, paxos_read_multi_node_waits_for_quorum);
    MACRO_ADD(tests, paxos_read_rejected_if_not_active);
    macro_run_all("paxos_reads", tests, test_count);
    return 0;
}

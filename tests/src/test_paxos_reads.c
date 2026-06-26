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
    paxos_advance(p, NULL, 0, 0);
    if (p->num_nodes > 1) {
        uint64_t remote_peer = p->node_directory[1];

        paxos_msg_t prom = { .type = MSG_PROMISE, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .num_entries = 0 };
        paxos_step_remote(p, &prom);
        paxos_advance(p, NULL, 0, 0);

        paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .slot = p->next_slot - 1 };
        paxos_step_remote(p, &ack);
        paxos_advance(p, NULL, 0, 0);
    }
}

MACRO_TEST(paxos_read_single_node_instant_ready) {
    uint64_t peers[] = {1};
    paxos_t* p = paxos_create(1, peers, 0);
    force_active_leader(p);

    paxos_msg_t ri = { .type = MSG_READ_BARRIER, .read_seq = 100 };
    paxos_step_local(p, &ri);

    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 1);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].read_seq, 100);

    paxos_ready_destroy(&ready);
    paxos_destroy(p);
}

MACRO_TEST(paxos_read_multi_node_waits_for_quorum) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    paxos_msg_t ri = { .type = MSG_READ_BARRIER, .read_seq = 500 };
    paxos_step_local(p, &ri);

    paxos_ready_t ready1 = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready1.num_read_states, 0);
    // The read barrier ping generates 2 immediate messages to peers!
    MACRO_ASSERT_EQ_INT(ready1.num_messages_immediate, 2);

    paxos_msg_t res = {
        .type = MSG_READ_BARRIER_RES,
        .to = 1,
        .from = 2,
        .ballot = p->active_ballot,
        .read_seq = 500
    };
    paxos_step_remote(p, &res);

    paxos_ready_t ready2 = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready2.num_read_states, 1);
    MACRO_ASSERT_EQ_INT(ready2.read_states[0].read_seq, 500);

    paxos_ready_destroy(&ready1);
    paxos_ready_destroy(&ready2);
    paxos_destroy(p);
}

MACRO_TEST(paxos_read_rejected_if_not_active) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    paxos_msg_t ri = { .type = MSG_READ_BARRIER, .read_seq = 100 };
    paxos_step_local(p, &ri);

    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    paxos_ready_destroy(&ready);
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

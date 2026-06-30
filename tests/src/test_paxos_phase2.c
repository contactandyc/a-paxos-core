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

MACRO_TEST(paxos_acceptor_rejects_lower_ballot_with_nack) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    p->promised_ballot = 100;

    paxos_msg_t prep = { .type = PAXOS_MSG_PREPARE, .to = 1, .from = 2, .ballot = 50, .slot = 1 };
    (void)paxos_receive(p, &prep);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);
    MACRO_ASSERT_EQ_INT(ready.num_messages_immediate, 1);
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].type, PAXOS_MSG_NACK);
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].promised_ballot, 100);

    paxos_destroy(p);
}

MACRO_TEST(paxos_acceptor_allows_out_of_order_accepts) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    paxos_entry_t e = { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };
    paxos_msg_t acc = { .type = PAXOS_MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 5, .entries = &e, .num_entries = 1 };

    (void)paxos_receive(p, &acc);

    paxos_entry_t* stored = paxos_log_get(p, 5);
    MACRO_ASSERT_TRUE(stored != NULL);
    MACRO_ASSERT_EQ_INT(stored->accepted_ballot, 10);

    paxos_destroy(p);
}

MACRO_TEST(paxos_leader_commits_on_quorum_ack) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 2 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    (void)paxos_propose(p, 0, 0, "B", 1);

    MACRO_ASSERT_EQ_INT(p->local_commit_index, 1);

    paxos_msg_t ack = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2 };
    (void)paxos_receive(p, &ack);

    MACRO_ASSERT_EQ_INT(p->local_commit_index, 2);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_acceptor_rejects_lower_ballot_with_nack);
    MACRO_ADD(tests, paxos_acceptor_allows_out_of_order_accepts);
    MACRO_ADD(tests, paxos_leader_commits_on_quorum_ack);

    macro_run_all("paxos_phase2", tests, test_count);
    return 0;
}

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

MACRO_TEST(paxos_silently_drops_messages_from_rogue_nodes) {
    uint64_t peers[] = {2, 3}; // Valid cluster is {1, 2, 3}
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 2
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    force_active_leader(p);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_ACTIVE);

    // Rogue Node 99 attempts a hostile takeover with a massive ballot
    uint64_t hostile_ballot = p->active_ballot + 5000;
    paxos_msg_t rogue_prepare = {
        .type = PAXOS_MSG_PREPARE,
        .to = 1,
        .from = 99,
        .ballot = hostile_ballot,
        .slot = 1
    };

    (void)paxos_receive(p, &rogue_prepare);

    // Node 1 must completely ignore the packet. It should still be the leader.
    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_ACTIVE);
    MACRO_ASSERT_TRUE(paxos_promised_ballot(p) < hostile_ballot);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_silently_drops_messages_from_rogue_nodes);

    macro_run_all("paxos_firewall", tests, test_count);
    return 0;
}

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

MACRO_TEST(learner_firewall_blocks_blind_voters_via_api) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    force_active_leader(p); // Commit index is now 1

    uint64_t target = 4;

    // 1. Propose Node 4 (who has NEVER sent an ACK and is totally offline)
    paxos_err_t err = paxos_add_node(p, target);

    // 2. The API MUST reject the addition to protect the cluster!
    MACRO_ASSERT_EQ_INT(err, PAXOS_ERR_LEARNER_NOT_READY);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);

    // BUT, the engine should have silently added Node 4 as a Learner so it can start streaming!
    MACRO_ASSERT_EQ_INT(p->num_nodes, 4);
    MACRO_ASSERT_EQ_INT(p->node_directory[3], 4);

    // 3. Simulate Node 4 catching up and sending an ACK for Slot 1
    paxos_msg_t catch_up_ack = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 4, .ballot = p->active_ballot, .slot = 1 };
    (void)paxos_receive(p, &catch_up_ack);

    // 4. Propose Node 4 AGAIN now that it is caught up
    err = paxos_add_node(p, target);

    // The API allows it through! Slot 2 now holds the JOINT config.
    MACRO_ASSERT_EQ_INT(err, PAXOS_OK);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 2);

    paxos_destroy(p);
}

MACRO_TEST(promotion_request_is_gated_by_firewall) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    force_active_leader(p); // Commit index is now 1
    uint64_t target = 4;
    paxos_register_learner(p, target);

    // 1. Premature Request: The Learner requests promotion over the network
    paxos_msg_t promote_req = {
        .type = PAXOS_MSG_PROMOTE_REQUEST,
        .to = 1,
        .from = target,
        .ballot = p->active_ballot
    };
    (void)paxos_receive(p, &promote_req);

    // The firewall drops the request silently. State machine is untouched.
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);
    MACRO_ASSERT_TRUE(p->pending_reconfig == false);

    // 2. The Learner catches up
    paxos_msg_t catch_up_ack = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = target, .ballot = p->active_ballot, .slot = 1 };
    (void)paxos_receive(p, &catch_up_ack);

    // 3. Valid Request: The Learner asks again
    (void)paxos_receive(p, &promote_req);

    // The firewall passes! The leader successfully initiates joint consensus.
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 2);
    MACRO_ASSERT_TRUE(p->pending_reconfig == true);

    paxos_entry_t* conf = paxos_log_get(p, 2);
    MACRO_ASSERT_TRUE(conf != NULL);
    MACRO_ASSERT_EQ_INT(conf->type, PAXOS_ENTRY_CONF_JOINT);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, learner_firewall_blocks_blind_voters_via_api);
    MACRO_ADD(tests, promotion_request_is_gated_by_firewall);
    macro_run_all("paxos_learner_firewall", tests, test_count);
    return 0;
}

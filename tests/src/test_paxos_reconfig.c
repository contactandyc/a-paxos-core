// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

static void force_active_leader(paxos_t* p) {
    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);
    paxos_advance(p, 0, 0);

    if (p->num_peers > 0) {
        paxos_msg_t prom = { .type = MSG_PROMISE, .to = p->id, .from = p->peers[0], .ballot = p->active_ballot, .num_entries = 0 };
        paxos_step_remote(p, &prom);
        paxos_advance(p, 0, 0);
    }
}

MACRO_TEST(paxos_reconfig_pipeline_stalls_during_inflight_config) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    uint64_t new_node = 4;
    paxos_entry_t e_conf = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    paxos_msg_t p_conf = { .type = MSG_PROPOSE, .entries = &e_conf, .num_entries = 1 };
    paxos_step_local(p, &p_conf);

    // The config change is occupying slot 1, but is NOT committed yet.
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);

    // Attempt to propose a normal entry while the config is inflight
    paxos_entry_t e_norm = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    paxos_msg_t p_norm = { .type = MSG_PROPOSE, .entries = &e_norm, .num_entries = 1 };
    paxos_step_local(p, &p_norm);

    // The core MUST reject it. The log should remain at slot 1.
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 1);

    paxos_destroy(p);
}

MACRO_TEST(paxos_reconfig_applies_topology_mutation_on_commit) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    MACRO_ASSERT_EQ_INT(p->num_peers, 2);

    uint64_t new_node = 4;
    paxos_entry_t e_conf = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    paxos_msg_t p_conf = { .type = MSG_PROPOSE, .entries = &e_conf, .num_entries = 1 };
    paxos_step_local(p, &p_conf);

    // Commit the config change via an ACK from Node 2
    paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 1 };
    paxos_step_remote(p, &ack);

    // The topology should be instantly mutated.
    MACRO_ASSERT_EQ_INT(paxos_local_commit_index(p), 1);
    MACRO_ASSERT_EQ_INT(p->num_peers, 3);
    MACRO_ASSERT_EQ_INT(p->peers[2], 4);

    paxos_destroy(p);
}

MACRO_TEST(paxos_reconfig_removed_leader_steps_down) {
    uint64_t peers[] = {2};
    paxos_t* p = paxos_create(1, peers, 1);
    force_active_leader(p);

    uint64_t self_node = 1;
    paxos_entry_t e_conf = { .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&self_node, .data_len = sizeof(uint64_t) };
    paxos_msg_t p_conf = { .type = MSG_PROPOSE, .entries = &e_conf, .num_entries = 1 };
    paxos_step_local(p, &p_conf);

    paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 1 };
    paxos_step_remote(p, &ack);

    // Once a leader commits its own removal, it instantly becomes a passive learner
    MACRO_ASSERT_TRUE(paxos_state(p) == PAXOS_STATE_LEARNER);
    MACRO_ASSERT_EQ_INT(p->leader_id, 0);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_reconfig_pipeline_stalls_during_inflight_config);
    MACRO_ADD(tests, paxos_reconfig_applies_topology_mutation_on_commit);
    MACRO_ADD(tests, paxos_reconfig_removed_leader_steps_down);

    macro_run_all("paxos_reconfig", tests, test_count);
    return 0;
}

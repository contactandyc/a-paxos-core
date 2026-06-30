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
        paxos_msg_t prom = { .type = PAXOS_MSG_PROMISE, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .num_entries = 0 };
        (void)paxos_receive(p, &prom);
        paxos_advance(p, NULL, 0, 0);

        paxos_msg_t ack = { .type = PAXOS_MSG_ACCEPTED, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .slot = p->next_slot - 1 };
        (void)paxos_receive(p, &ack);
        paxos_advance(p, NULL, 0, 0);
    }
}

MACRO_TEST(pending_reconfig_lock_does_not_leak_on_log_failure) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    p->next_slot = p->local_commit_index + PAXOS_INTERNAL_INFLIGHT_WINDOW;

    uint64_t target = 4;
    paxos_err_t err = paxos_register_learner(p, target);
    (void)err;

    paxos_msg_t catch_up = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = target, .ballot = p->active_ballot, .slot = 1 };
    (void)paxos_receive(p, &catch_up);

    (void)paxos_add_node(p, target);

    MACRO_ASSERT_TRUE(p->pending_reconfig == false);

    paxos_destroy(p);
}

MACRO_TEST(restore_during_joint_consensus_preserves_joint_state) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    uint64_t target = 4;
    paxos_err_t err = paxos_register_learner(p, target);
    (void)err;

    paxos_msg_t catch_up = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = target, .ballot = p->active_ballot, .slot = 1 };
    (void)paxos_receive(p, &catch_up);

    (void)paxos_add_node(p, target);

    paxos_msg_t ack = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2 };
    (void)paxos_receive(p, &ack);

    MACRO_ASSERT_TRUE(p->in_joint_consensus == true);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);

    // FIXED: Removed (void) cast that broke variable declaration
    paxos_restored_entry_t disk_entries[2];
    disk_entries[0].entry = ready.chosen_entries[0]; disk_entries[0].chosen = true;
    disk_entries[1].entry = ready.chosen_entries[1]; disk_entries[1].chosen = true;

    // FIXED: Removed (void) cast that broke variable declaration
    paxos_restore_data_t rd = {
        .struct_size = sizeof(paxos_restore_data_t),
        .hard_state = ready.hard_state,
        .local_commit_index = 2,
        .snapshot_index = 0,
        .entries = disk_entries,
        .num_entries = 2
    };

    paxos_destroy(p);

    paxos_t* p2;
    (void)paxos_restore(&cfg, &rd, &p2);

    MACRO_ASSERT_TRUE(p2->in_joint_consensus == true);
    MACRO_ASSERT_EQ_INT(p2->joint_config_mask, 15);
    MACRO_ASSERT_TRUE(p2->pending_reconfig == true);

    MACRO_ASSERT_EQ_INT(p2->base_config_mask, 7);
    MACRO_ASSERT_EQ_INT(p2->active_config_mask, 7);

    uint64_t old_majority = (1ULL << 0) | (1ULL << 1);
    uint64_t new_majority = (1ULL << 0) | (1ULL << 1) | (1ULL << 2);

    MACRO_ASSERT_TRUE(paxos_has_quorum(p2, old_majority) == false);
    MACRO_ASSERT_TRUE(paxos_has_quorum(p2, old_majority | new_majority) == true);

    paxos_destroy(p2);
}

MACRO_TEST(snapshot_after_final_preserves_new_topology_on_restart) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    force_active_leader(p);

    uint64_t target = 4;
    paxos_err_t err = paxos_register_learner(p, target);
    (void)err;

    paxos_msg_t catch_up = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = target, .ballot = p->active_ballot, .slot = 1 };
    (void)paxos_receive(p, &catch_up);

    (void)paxos_add_node(p, target);

    paxos_msg_t ack_joint = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2 };
    (void)paxos_receive(p, &ack_joint);

    paxos_msg_t ack_final = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 3 };
    (void)paxos_receive(p, &ack_final);

    paxos_msg_t ack_final_3 = { .type = PAXOS_MSG_ACCEPTED, .to = 1, .from = 3, .ballot = p->active_ballot, .slot = 3 };
    (void)paxos_receive(p, &ack_final_3);

    MACRO_ASSERT_TRUE(p->in_joint_consensus == false);
    MACRO_ASSERT_EQ_INT(p->active_config_mask, 15);

    p->last_applied = 3;
    paxos_compact(p, 3);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);
    paxos_hard_state_t disk_hs = ready.hard_state;
    uint64_t snap_idx = p->snapshot_index;

    paxos_destroy(p);

    uint64_t new_peers[] = {1, 2, 3, 4};
    paxos_config_t new_cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = new_peers, .num_initial_voters = 4 };

    // FIXED: Removed (void) cast that broke variable declaration
    paxos_restore_data_t rd = {
        .struct_size = sizeof(paxos_restore_data_t),
        .hard_state = disk_hs,
        .local_commit_index = snap_idx,
        .snapshot_index = snap_idx,
        .entries = NULL,
        .num_entries = 0
    };

    paxos_t* p2;
    (void)paxos_restore(&new_cfg, &rd, &p2);

    MACRO_ASSERT_EQ_INT(p2->active_config_mask, 15);
    MACRO_ASSERT_TRUE(p2->in_joint_consensus == false);

    paxos_destroy(p2);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, pending_reconfig_lock_does_not_leak_on_log_failure);
    MACRO_ADD(tests, restore_during_joint_consensus_preserves_joint_state);
    MACRO_ADD(tests, snapshot_after_final_preserves_new_topology_on_restart);
    macro_run_all("paxos_reconfig_crash_restore", tests, test_count);
    return 0;
}

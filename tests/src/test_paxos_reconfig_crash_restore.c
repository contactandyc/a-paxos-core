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

// Reviewer Test 6
MACRO_TEST(pending_reconfig_lock_does_not_leak_on_log_failure) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);
    force_active_leader(p);

    p->next_slot = p->local_commit_index + INFLIGHT_WINDOW;

    uint64_t target = 4;
    paxos_entry_t e = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&target, .data_len = sizeof(uint64_t) };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    paxos_step_local(p, &prop);

    MACRO_ASSERT_TRUE(p->pending_reconfig == false);

    paxos_destroy(p);
}

// Reviewer Test 8
MACRO_TEST(restore_during_joint_consensus_preserves_joint_state) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);
    force_active_leader(p);

    uint64_t target = 4;
    paxos_entry_t e = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&target, .data_len = sizeof(uint64_t) };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    paxos_step_local(p, &prop);

    paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2 };
    paxos_step_remote(p, &ack);

    MACRO_ASSERT_TRUE(p->in_joint_consensus == true);

    paxos_ready_t ready = paxos_get_ready(p);
    paxos_hard_state_t disk_hs = ready.hard_state;

    paxos_restored_entry_t disk_entries[2];
    disk_entries[0].entry = ready.chosen_entries[0]; disk_entries[0].chosen = true; // NoOp
    disk_entries[1].entry = ready.chosen_entries[1]; disk_entries[1].chosen = true; // JOINT

    paxos_destroy(p);

    paxos_t* p2 = paxos_restore(1, peers, 3, disk_hs, 2, 0, disk_entries, 2);

    MACRO_ASSERT_TRUE(p2->in_joint_consensus == true);
    MACRO_ASSERT_EQ_INT(p2->joint_config_mask, 15);
    MACRO_ASSERT_TRUE(p2->pending_reconfig == true);

    paxos_ready_destroy(&ready);
    paxos_destroy(p2);
}

// Reviewer Test 9
MACRO_TEST(snapshot_after_final_preserves_new_topology_on_restart) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);
    force_active_leader(p);

    uint64_t target = 4;
    paxos_entry_t e = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&target, .data_len = sizeof(uint64_t) };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    paxos_step_local(p, &prop);

    paxos_msg_t ack_joint = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2 };
    paxos_step_remote(p, &ack_joint);

    paxos_msg_t ack_final = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 3 };
    paxos_step_remote(p, &ack_final);

    // FAANG: Give the engine the required 3rd vote to satisfy the new overlapping majority!
    paxos_msg_t ack_final_3 = { .type = MSG_ACCEPTED, .to = 1, .from = 3, .ballot = p->active_ballot, .slot = 3 };
    paxos_step_remote(p, &ack_final_3);

    MACRO_ASSERT_TRUE(p->in_joint_consensus == false);
    MACRO_ASSERT_EQ_INT(p->active_config_mask, 15);

    // TAKE A SNAPSHOT AND COMPACT THE LOG
    p->last_applied = 3;
    paxos_compact(p, 3);

    paxos_hard_state_t disk_hs = p->prev_hard_state;
    uint64_t snap_idx = p->snapshot_index;

    paxos_destroy(p);

    // RESTORE FROM SNAPSHOT
    // The host application reads its snapshot metadata to determine the current cluster topology
    uint64_t new_peers[] = {1, 2, 3, 4};
    paxos_t* p2 = paxos_restore(1, new_peers, 4, disk_hs, snap_idx, snap_idx, NULL, 0);

    // The restored engine awakens cleanly into the new topology
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

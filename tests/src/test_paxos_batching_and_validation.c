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

MACRO_TEST(follower_receives_batched_accept_and_persists_correctly) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    p->promised_ballot = 10;

    paxos_entry_t batch[3] = {
        { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 },
        { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"B", .data_len = 1 },
        { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"C", .data_len = 1 }
    };

    paxos_msg_t acc = { .type = PAXOS_MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 5, .entries = batch, .num_entries = 3 };
    (void)paxos_receive(p, &acc);

    MACRO_ASSERT_TRUE(paxos_log_get(p, 5) != NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 6) != NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 7) != NULL);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);

    MACRO_ASSERT_EQ_INT(ready.num_messages_after_persist, 1);
    MACRO_ASSERT_EQ_INT(ready.messages_after_persist[0].type, PAXOS_MSG_ACCEPTED);
    MACRO_ASSERT_EQ_INT(ready.messages_after_persist[0].slot, 5);
    MACRO_ASSERT_EQ_INT(ready.messages_after_persist[0].num_entries, 3);

    // No paxos_ready_destroy needed! Memory is owned by the engine.
    paxos_destroy(p);
}


MACRO_TEST(acceptor_aborts_batch_on_malformed_secondary_entries) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    p->promised_ballot = 10;

    paxos_entry_t bad_batch[2] = {
        { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 },
        // Malformed: declares length but passes NULL data pointer
        { .type = PAXOS_ENTRY_NORMAL, .data = NULL, .data_len = 10 }
    };
    paxos_msg_t acc = { .type = PAXOS_MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = bad_batch, .num_entries = 2 };

    (void)paxos_receive(p, &acc);

    // First entry succeeds, but second fails validation, truncating the batch
    MACRO_ASSERT_TRUE(paxos_log_get(p, 1) == NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 2) == NULL);

    paxos_destroy(p);
}

MACRO_TEST(joint_config_application_safely_deduplicates_nodes) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = { .struct_size = sizeof(paxos_config_t), .node_id = 1, .initial_voters = peers, .num_initial_voters = 3 };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);

    force_active_leader(p);

    // Simulate receiving a joint config that maliciously or accidentally includes Node 2 twice
    uint64_t joint_nodes[] = {1, 2, 2, 3, 4};

    paxos_log_accept(p, 2, p->active_ballot, PAXOS_ENTRY_CONF_JOINT, 0, 0, (uint8_t*)joint_nodes, sizeof(joint_nodes));
    paxos_log_learn_chosen(p, 2, paxos_log_get_accepted(p, 2));

    p->local_commit_index = 2;
    paxos_rebuild_config(p); // Trigger the state machine topology update

    MACRO_ASSERT_TRUE(p->in_joint_consensus == true);

    // Nodes 1, 2, 3, and 4 should be active.
    // The duplicate '2' must not increment num_nodes excessively.
    MACRO_ASSERT_EQ_INT(p->num_nodes, 4);

    paxos_destroy(p);
}

MACRO_TEST(batched_accept_where_one_entry_fails_allocation_preserves_durable_state) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1, .initial_voters = peers, .num_initial_voters = 3,
        .max_payload_size = 1048576 // 1MB Limit
    };
    paxos_t* p;
    (void)paxos_create(&cfg, &p);
    p->promised_ballot = 10;

    paxos_entry_t batch[3] = {
        { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 },
        { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"B", .data_len = 1 },
        { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"C", .data_len = cfg.max_payload_size + 1 } // FATAL OVERSIZED!
    };

    paxos_msg_t acc = { .type = PAXOS_MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 5, .entries = batch, .num_entries = 3 };

    paxos_err_t err = paxos_receive(p, &acc);

    // Engine drops the entire malformed packet at the network boundary
    MACRO_ASSERT_EQ_INT(err, PAXOS_ERR_INVALID_ARG);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 5) == NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 6) == NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 7) == NULL);

    paxos_ready_t ready;
    (void)paxos_get_ready(p, &ready);

    // Since the packet was dropped, NO acknowledgments are generated!
    MACRO_ASSERT_EQ_INT(ready.num_messages_after_persist, 0);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, follower_receives_batched_accept_and_persists_correctly);
    MACRO_ADD(tests, acceptor_aborts_batch_on_malformed_secondary_entries);
    MACRO_ADD(tests, joint_config_application_safely_deduplicates_nodes);
    MACRO_ADD(tests, batched_accept_where_one_entry_fails_allocation_preserves_durable_state);
    macro_run_all("paxos_batching_and_validation", tests, test_count);
    return 0;
}

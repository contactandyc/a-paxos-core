// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(accepted_but_unchosen_config_must_not_change_quorum) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    uint64_t new_node = 4;
    paxos_log_accept(p, 1, 10, ENTRY_CONF_ADD, 0, 0, (uint8_t*)&new_node, sizeof(uint64_t));

    MACRO_ASSERT_EQ_INT(p->active_config_mask, 7);
    MACRO_ASSERT_EQ_INT(p->num_nodes, 3);

    paxos_destroy(p);
}

MACRO_TEST(config_changes_membership_only_after_chosen) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    uint64_t new_node = 4;
    paxos_log_accept(p, 1, 10, ENTRY_CONF_ADD, 0, 0, (uint8_t*)&new_node, sizeof(uint64_t));

    p->leader_id = 2;
    p->promised_ballot = 10;

    // FIXED: Must provide the hint so the engine knows it is allowed to commit!
    p->leader_commit_hint = 1;

    paxos_advance_local_commit(p, 2, 10);

    MACRO_ASSERT_EQ_INT(p->active_config_mask, 15);

    paxos_destroy(p);
}

MACRO_TEST(new_node_cannot_vote_until_explicitly_added) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    p->promised_ballot = 10;
    p->state = PAXOS_STATE_ACTIVE;
    p->active_ballot = 10;
    p->leader_id = 1;

    paxos_inflight_slot_t* inf = &p->inflight[1 % INFLIGHT_WINDOW];
    inf->active = true;
    inf->slot = 1;
    inf->ballot = 10;
    inf->ack_mask = 1;

    paxos_msg_t rogue_ack = { .type = MSG_ACCEPTED, .to = 1, .from = 4, .ballot = 10, .slot = 1 };
    paxos_step_remote(p, &rogue_ack);

    MACRO_ASSERT_EQ_INT(inf->ack_mask, 1);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, accepted_but_unchosen_config_must_not_change_quorum);
    MACRO_ADD(tests, config_changes_membership_only_after_chosen);
    MACRO_ADD(tests, new_node_cannot_vote_until_explicitly_added);
    macro_run_all("paxos_reconfig_safety", tests, test_count);
    return 0;
}

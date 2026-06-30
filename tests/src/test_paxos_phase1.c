// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(paxos_initial_state_is_learner) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 2
    };
    paxos_t* p;
    paxos_create(&cfg, &p);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_LEARNER);
    MACRO_ASSERT_EQ_INT(paxos_promised_ballot(p), 0);
    MACRO_ASSERT_EQ_INT(paxos_local_commit_index(p), 0);

    paxos_destroy(p);
}

MACRO_TEST(paxos_campaign_generates_unique_ballot_and_broadcasts_prepare) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 2
    };
    paxos_t* p;
    paxos_create(&cfg, &p);

    extern void paxos_proposer_campaign(paxos_t* p);
    paxos_proposer_campaign(p);

    MACRO_ASSERT_EQ_INT(paxos_state(p), PAXOS_STATE_RECOVERING_PHASE1);
    MACRO_ASSERT_TRUE(p->active_ballot > 0);

    // Check hardware bitmask for self-promise
    MACRO_ASSERT_EQ_INT(__builtin_popcountll(p->promise_mask), 1);

    paxos_ready_t ready;
    paxos_get_ready(p, &ready);
    MACRO_ASSERT_EQ_INT(ready.num_messages_after_persist, 2);

    MACRO_ASSERT_EQ_INT(ready.messages_after_persist[0].type, PAXOS_MSG_PREPARE);
    MACRO_ASSERT_EQ_INT(ready.messages_after_persist[0].to, 2);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_initial_state_is_learner);
    MACRO_ADD(tests, paxos_campaign_generates_unique_ballot_and_broadcasts_prepare);

    macro_run_all("paxos_phase1", tests, test_count);
    return 0;
}

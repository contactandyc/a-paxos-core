// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(paxos_learner_detects_gap_and_fetches_entries) {
    uint64_t peers[] = {2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 2
    };
    paxos_t* p;
    paxos_create(&cfg, &p);

    // Simulate Node 2 sending an Accept for slot 3, but piggybacking that slot 3 is committed!
    paxos_entry_t e = { .type = PAXOS_ENTRY_NORMAL, .data = (uint8_t*)"3", .data_len = 1 };
    paxos_msg_t acc = {
        .type = PAXOS_MSG_ACCEPT,
        .to = 1,
        .from = 2,
        .ballot = 5,
        .slot = 3,
        .commit_index = 3,
        .entries = &e,
        .num_entries = 1
    };

    paxos_receive(p, &acc);

    // The learner accepted slot 3, but its local_commit_index MUST stay at 0 due to the gap at 1 and 2.
    MACRO_ASSERT_EQ_INT(p->local_commit_index, 0);
    MACRO_ASSERT_EQ_INT(p->leader_commit_hint, 3);

    paxos_ready_t ready;
    paxos_get_ready(p, &ready);

    // It should have generated an immediate message asking Node 2 for the missing slots!
    MACRO_ASSERT_EQ_INT(ready.num_messages_immediate, 1);
    MACRO_ASSERT_TRUE(ready.messages_immediate[0].type == PAXOS_MSG_FETCH_ENTRIES);

    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].slot, 1); // Start slot
    MACRO_ASSERT_EQ_INT(ready.messages_immediate[0].commit_index, 3); // End slot

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_learner_detects_gap_and_fetches_entries);

    macro_run_all("paxos_catchup", tests, test_count);
    return 0;
}

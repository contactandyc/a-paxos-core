// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(duplicate_prepare_before_advance_respects_persistence_ordering) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    paxos_create(&cfg, &p);

    paxos_msg_t prep = { .type = PAXOS_MSG_PREPARE, .to = 1, .from = 2, .ballot = 10, .slot = 1 };

    // 1st Prepare: Upgrades ballot, outputs Promise to after_persist
    paxos_receive(p, &prep);
    paxos_ready_t r1;
    paxos_get_ready(p, &r1);
    MACRO_ASSERT_EQ_INT(r1.num_messages_after_persist, 1);
    MACRO_ASSERT_EQ_INT(r1.num_messages_immediate, 0);

    // 2nd Prepare (Duplicate): Arrives before host calls paxos_advance!
    // Because the queue hasn't been cleared, it safely appends the 2nd promise here.
    paxos_receive(p, &prep);
    paxos_ready_t r2;
    paxos_get_ready(p, &r2);
    MACRO_ASSERT_EQ_INT(r2.num_messages_after_persist, 2); // FIXED: 1st and 2nd are both queued!
    MACRO_ASSERT_EQ_INT(r2.num_messages_immediate, 0);

    // 3. Host explicitly saves the ballot and advances
    p->prev_hard_state.promised_ballot = 10;
    paxos_advance(p, NULL, 0, 0);

    // 4. 3rd Prepare: Disk is durable. It can safely reply immediately now!
    paxos_receive(p, &prep);
    paxos_ready_t r3;
    paxos_get_ready(p, &r3);
    MACRO_ASSERT_EQ_INT(r3.num_messages_immediate, 1);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, duplicate_prepare_before_advance_respects_persistence_ordering);
    macro_run_all("paxos_persistence_races", tests, test_count);
    return 0;
}

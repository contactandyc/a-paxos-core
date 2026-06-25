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
    paxos_advance(p, 0, 0);
    if (p->num_peers > 0) {
        paxos_msg_t prom = { .type = MSG_PROMISE, .to = p->id, .from = p->peers[0], .ballot = p->active_ballot, .num_entries = 0 };
        paxos_step_remote(p, &prom);
        paxos_advance(p, 0, 0);

        // NEW: ACK the NoOp to fully commit the epoch!
        paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = p->id, .from = p->peers[0], .ballot = p->active_ballot, .slot = p->next_slot - 1 };
        paxos_step_remote(p, &ack);
        paxos_advance(p, 0, 0);
    }
}

MACRO_TEST(paxos_drops_duplicate_inflight_client_proposals) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    paxos_entry_t e1 = { .type = ENTRY_NORMAL, .client_id = 99, .client_seq = 1, .data = (uint8_t*)"A", .data_len = 1 };
    paxos_msg_t p1 = { .type = MSG_PROPOSE, .entries = &e1, .num_entries = 1 };
    paxos_step_local(p, &p1);

    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 2); // Shifted to 2 due to NoOp

    paxos_msg_t p2 = { .type = MSG_PROPOSE, .entries = &e1, .num_entries = 1 };
    paxos_step_local(p, &p2);

    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 2);

    paxos_destroy(p);
}

MACRO_TEST(paxos_drops_duplicate_applied_client_proposals) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    paxos_entry_t e1 = { .type = ENTRY_NORMAL, .client_id = 42, .client_seq = 5, .data = (uint8_t*)"PAY", .data_len = 3 };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e1, .num_entries = 1 };
    paxos_step_local(p, &prop);

    paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = 1, .from = 2, .ballot = p->active_ballot, .slot = 2 };
    paxos_step_remote(p, &ack);

    paxos_ready_t ready = paxos_get_ready(p);
    // The NoOp + The User Proposal = 2 chosen entries
    MACRO_ASSERT_EQ_INT(ready.num_chosen_entries, 2);

    paxos_ready_destroy(&ready);
    paxos_advance(p, 2, 2);

    paxos_step_local(p, &prop);

    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 2);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_drops_duplicate_inflight_client_proposals);
    MACRO_ADD(tests, paxos_drops_duplicate_applied_client_proposals);

    macro_run_all("paxos_dedup", tests, test_count);
    return 0;
}

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

        paxos_msg_t prom = { .type = MSG_PROMISE, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .num_entries = 0 };
        paxos_step_remote(p, &prom);
        paxos_advance(p, NULL, 0, 0);

        paxos_msg_t ack = { .type = MSG_ACCEPTED, .to = p->id, .from = remote_peer, .ballot = p->active_ballot, .slot = p->next_slot - 1 };
        paxos_step_remote(p, &ack);
        paxos_advance(p, NULL, 0, 0);
    }
}

MACRO_TEST(paxos_log_dynamically_allocates_chunks_on_sparse_accept) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    MACRO_ASSERT_EQ_INT(p->log_chunks_cap, 16);

    uint64_t huge_slot = 1500000;
    paxos_log_accept(p, huge_slot, 1, ENTRY_NORMAL, 0, 0, (uint8_t*)"A", 1);

    uint64_t expected_c_idx = paxos_chunk_idx(p, huge_slot);

    MACRO_ASSERT_TRUE(p->log_chunks_cap > expected_c_idx);
    MACRO_ASSERT_TRUE(p->log_chunks[expected_c_idx] != NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, huge_slot) != NULL);

    paxos_destroy(p);
}

MACRO_TEST(paxos_outbound_messages_use_zero_copy_ref_counting) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);
    force_active_leader(p);

    paxos_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"PAYLOAD", .data_len = 7 };
    paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    paxos_step_local(p, &prop);

    paxos_entry_t* log_entry = paxos_log_get(p, 2);
    MACRO_ASSERT_TRUE(log_entry != NULL);

    paxos_ready_t ready = paxos_get_ready(p);
    MACRO_ASSERT_TRUE(ready.num_messages_after_persist > 0);

    uint8_t* outbound_data = ready.messages_after_persist[0].entries[0].data;
    MACRO_ASSERT_TRUE(outbound_data == log_entry->data);

    paxos_ready_destroy(&ready);
    paxos_destroy(p);
}

MACRO_TEST(paxos_compact_frees_obsolete_chunks_in_o1) {
    uint64_t peers[] = {2, 3};
    paxos_t* p = paxos_create(1, peers, 2);

    paxos_log_accept(p, 5, 1, ENTRY_NORMAL, 0, 0, (uint8_t*)"A", 1);
    paxos_log_accept(p, 2500, 1, ENTRY_NORMAL, 0, 0, (uint8_t*)"B", 1);

    MACRO_ASSERT_TRUE(p->log_chunks[paxos_chunk_idx(p, 5)] != NULL);
    MACRO_ASSERT_TRUE(p->log_chunks[paxos_chunk_idx(p, 2500)] != NULL);

    p->last_applied = 1500;
    paxos_compact(p, 1500); // This completely purges the first chunk and shifts the array down!

    // The old chunk 0 should be gone, but because of relative indexing,
    // the logical chunk that holds 2500 is safely shifted down and protected.
    MACRO_ASSERT_TRUE(p->log_chunks[0] == NULL);
    MACRO_ASSERT_TRUE(p->log_chunks[paxos_chunk_idx(p, 2500)] != NULL);
    MACRO_ASSERT_TRUE(paxos_log_get(p, 2500) != NULL);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, paxos_log_dynamically_allocates_chunks_on_sparse_accept);
    MACRO_ADD(tests, paxos_outbound_messages_use_zero_copy_ref_counting);
    MACRO_ADD(tests, paxos_compact_frees_obsolete_chunks_in_o1);

    macro_run_all("paxos_memory", tests, test_count);
    return 0;
}

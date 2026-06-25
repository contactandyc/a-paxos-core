// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define PAXOS_TESTING 1
#include <stdio.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(compaction_shifts_correctly_at_chunk_boundaries) {
    uint64_t peers[] = {1, 2, 3};
    paxos_t* p = paxos_create(1, peers, 3);

    // Test Boundary 1: Compacting at 1023 (Base moves to 1024)
    paxos_log_accept(p, 1025, 1, ENTRY_NORMAL, 0, 0, (uint8_t*)"A", 1);
    p->last_applied = 1023;
    paxos_compact(p, 1023); // Should shift exactly 1 chunk

    MACRO_ASSERT_EQ_INT(p->log_base_slot, 1024);
    paxos_entry_t* e = paxos_log_get(p, 1025);
    MACRO_ASSERT_TRUE(e != NULL);

    // Test Boundary 2: Compacting at 2047 (Base moves to 2048)
    paxos_log_accept(p, 2050, 1, ENTRY_NORMAL, 0, 0, (uint8_t*)"B", 1);
    p->last_applied = 2047;
    paxos_compact(p, 2047); // Should shift exactly 1 more chunk

    MACRO_ASSERT_EQ_INT(p->log_base_slot, 2048);
    e = paxos_log_get(p, 2050);
    MACRO_ASSERT_TRUE(e != NULL);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, compaction_shifts_correctly_at_chunk_boundaries);
    macro_run_all("paxos_compaction_boundaries", tests, test_count);
    return 0;
}

// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#define PAXOS_TESTING 1
#include <stdio.h>
#include <string.h>
#include "paxos_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(malformed_joint_config_lengths_are_rejected) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    paxos_create(&cfg, &p);

    // Payload is 10 bytes (not divisible by 8 byte uint64_t)
    uint8_t bad_data[10] = {0};
    paxos_entry_t e = { .type = PAXOS_ENTRY_CONF_JOINT, .data = bad_data, .data_len = 10 };
    paxos_msg_t acc = { .type = PAXOS_MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = &e, .num_entries = 1 };

    paxos_err_t err = paxos_receive(p, &acc);

    // Engine must reject the malformed network payload entirely
    MACRO_ASSERT_EQ_INT(err, PAXOS_ERR_INVALID_ARG);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 0);

    paxos_destroy(p);
}

MACRO_TEST(joint_config_with_duplicate_nodes_is_rejected) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    paxos_create(&cfg, &p);

    // Payload tries to pass Node 2 twice!
    uint64_t bad_nodes[] = {1, 2, 2, 3};
    paxos_entry_t e = { .type = PAXOS_ENTRY_CONF_JOINT, .data = (uint8_t*)bad_nodes, .data_len = sizeof(bad_nodes) };
    paxos_msg_t acc = { .type = PAXOS_MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = &e, .num_entries = 1 };

    paxos_err_t err = paxos_receive(p, &acc);

    // Engine must intercept and reject the duplicate IDs
    MACRO_ASSERT_EQ_INT(err, PAXOS_ERR_INVALID_ARG);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 0);

    paxos_destroy(p);
}

MACRO_TEST(final_config_with_nonzero_payload_is_rejected) {
    uint64_t peers[] = {1, 2, 3};
    paxos_config_t cfg = {
        .struct_size = sizeof(paxos_config_t),
        .node_id = 1,
        .initial_voters = peers,
        .num_initial_voters = 3
    };
    paxos_t* p;
    paxos_create(&cfg, &p);

    // FINAL configs must have 0 bytes of payload. We pass 8.
    uint64_t garbage = 99;
    paxos_entry_t e = { .type = PAXOS_ENTRY_CONF_FINAL, .data = (uint8_t*)&garbage, .data_len = sizeof(uint64_t) };
    paxos_msg_t acc = { .type = PAXOS_MSG_ACCEPT, .to = 1, .from = 2, .ballot = 10, .slot = 1, .entries = &e, .num_entries = 1 };

    paxos_err_t err = paxos_receive(p, &acc);

    MACRO_ASSERT_EQ_INT(err, PAXOS_ERR_INVALID_ARG);
    MACRO_ASSERT_EQ_INT(paxos_last_slot(p), 0);

    paxos_destroy(p);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, malformed_joint_config_lengths_are_rejected);
    MACRO_ADD(tests, joint_config_with_duplicate_nodes_is_rejected);
    MACRO_ADD(tests, final_config_with_nonzero_payload_is_rejected);
    macro_run_all("paxos_malformed_configs", tests, test_count);
    return 0;
}

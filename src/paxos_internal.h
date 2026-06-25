// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef PAXOS_INTERNAL_H
#define PAXOS_INTERNAL_H

#include "a-paxos-core/paxos.h"

#define MAX_PEERS 64
#define MAX_REMOTE_PEERS (MAX_PEERS - 1)

// Tracks Phase 1 Recovery state per slot
typedef struct {
    uint64_t highest_ballot_seen;
    paxos_entry_t recovered_value;
    bool has_value;
} paxos_recovery_slot_t;

// Tracks Phase 2 Accepted state per slot
typedef struct {
    size_t acks;
    bool acked_by[MAX_PEERS];
    bool chosen;
} paxos_inflight_slot_t;

struct paxos_s {
    uint64_t id;
    paxos_state_t state;

    // Persistent Acceptor State
    uint64_t promised_ballot;
    uint64_t max_generated_ballot;

    // The Sparse Replicated Log
    paxos_entry_t* log;
    bool* log_has_value; // True if a value is accepted at this slot
    size_t log_cap;
    uint64_t log_base_slot;

    // Learner State
    uint64_t commit_index;
    uint64_t last_applied;
    uint64_t snapshot_index;

    // Active Proposer State
    uint64_t active_ballot;
    uint64_t next_slot;
    uint64_t leader_id;

    uint64_t peers[MAX_REMOTE_PEERS];
    size_t num_peers;

    // Election / Quorum Tracking
    size_t promises_received;
    bool promised_by[MAX_PEERS];

    // In-Flight Phase 2 Tracking (Sliding Window)
    paxos_inflight_slot_t* inflight;

    // Phase 1 Recovery Buffer
    paxos_recovery_slot_t* recovery_buffer;
    size_t recovery_cap;
    uint64_t recovery_max_slot;

    paxos_msg_t* msg_queue;
    size_t msg_queue_cap;
    size_t msg_queue_len;

    uint64_t uncommitted_bytes;
    bool fatal_error;
};

void paxos_send_msg(paxos_t* p, paxos_msg_t msg);

// Log Interfaces
bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len);
paxos_entry_t* paxos_log_get(paxos_t* p, uint64_t slot);
paxos_entry_t* paxos_log_extract_suffix(paxos_t* p, uint64_t start_slot, size_t* out_count);

// Role Routers
void paxos_acceptor_step(paxos_t* p, paxos_msg_t* msg);
void paxos_proposer_step(paxos_t* p, paxos_msg_t* msg);

#endif // PAXOS_INTERNAL_H

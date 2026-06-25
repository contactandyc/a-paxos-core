// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef PAXOS_INTERNAL_H
#define PAXOS_INTERNAL_H

#include "a-paxos-core/paxos.h"

#define MAX_PEERS 64
#define MAX_REMOTE_PEERS (MAX_PEERS - 1)

typedef struct {
    bool has_value;
    paxos_entry_t entry;
} paxos_log_slot_t;

typedef struct {
    uint64_t highest_ballot_seen;
    paxos_entry_t recovered_value;
    bool has_value;
} paxos_recovery_slot_t;

typedef struct {
    uint64_t slot;
    uint64_t ballot;
    size_t acks;
    bool acked_by[MAX_PEERS];
    bool chosen;
    bool active;
} paxos_inflight_slot_t;

struct paxos_s {
    uint64_t id;
    paxos_state_t state;

    uint64_t promised_ballot;
    uint64_t max_generated_ballot;
    paxos_hard_state_t prev_hard_state;

    paxos_log_slot_t* log;
    size_t log_cap;
    uint64_t log_base_slot;

    uint64_t stable_accepted_through;
    uint64_t leader_commit_hint;
    uint64_t local_commit_index;
    uint64_t last_applied;
    uint64_t snapshot_index;

    uint64_t active_ballot;
    uint64_t last_observed_ballot;
    uint64_t next_slot;
    uint64_t leader_id;

    uint64_t peers[MAX_REMOTE_PEERS];
    size_t num_peers;

    size_t promises_received;
    bool promised_by[MAX_PEERS];
    bool self_promised;

    paxos_inflight_slot_t* inflight;

    paxos_recovery_slot_t* recovery_buffer;
    size_t recovery_cap;
    uint64_t recovery_max_slot;

    paxos_msg_t* msg_queue_immediate;
    size_t msg_queue_immediate_cap;
    size_t msg_queue_immediate_len;

    paxos_msg_t* msg_queue_after_persist;
    size_t msg_queue_after_persist_cap;
    size_t msg_queue_after_persist_len;

    bool fatal_error;
};

void paxos_send_immediate(paxos_t* p, paxos_msg_t msg);
void paxos_send_after_persist(paxos_t* p, paxos_msg_t msg);

bool paxos_entry_clone(paxos_entry_t* dst, const paxos_entry_t* src);
void paxos_entry_destroy(paxos_entry_t* e);

bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len);
paxos_entry_t* paxos_log_get(paxos_t* p, uint64_t slot);
paxos_entry_t* paxos_log_extract_suffix(paxos_t* p, uint64_t start_slot, size_t* out_count);
void paxos_advance_local_commit(paxos_t* p);

void paxos_acceptor_step(paxos_t* p, paxos_msg_t* msg);
void paxos_proposer_step(paxos_t* p, paxos_msg_t* msg);

#endif // PAXOS_INTERNAL_H

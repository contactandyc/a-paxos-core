// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef PAXOS_INTERNAL_H
#define PAXOS_INTERNAL_H

#include "a-paxos-core/paxos.h"
#include <string.h>

#define MAX_PEERS 64
#define MAX_REMOTE_PEERS (MAX_PEERS - 1)
#define MAX_PENDING_READS 128
#define INFLIGHT_WINDOW 4096
#define MAX_CLIENT_TRACKING 1024

typedef struct {
    uint64_t client_id;
    uint64_t client_seq;
    uint64_t lru_tick;
} paxos_client_session_t;

typedef struct {
    bool has_value;
    bool unstable;
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

typedef struct {
    uint64_t read_seq;
    uint64_t client_ctx;
    uint64_t slot;
    size_t acks;
    bool acked_by[MAX_PEERS];
    bool active;
} paxos_pending_read_t;

struct paxos_s {
    uint64_t id;
    paxos_state_t state;

    uint64_t promised_ballot;
    uint64_t max_generated_ballot;
    paxos_hard_state_t prev_hard_state;

    paxos_log_slot_t* log;
    size_t log_cap;
    uint64_t log_base_slot;

    uint64_t stable_accepted_through; // <-- FIXED: Restored this critical tracker!
    uint64_t leader_commit_hint;
    uint64_t local_commit_index;
    uint64_t last_applied;

    uint64_t snapshot_index;
    uint64_t snapshot_ballot;

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

    uint64_t snapshot_offset[MAX_PEERS];

    bool pending_snapshot;
    uint8_t* pending_snapshot_data;
    size_t pending_snapshot_len;
    uint64_t pending_snapshot_offset;
    bool pending_snapshot_done;
    uint64_t pending_snapshot_from;
    uint64_t pending_snapshot_msg_slot;
    uint64_t pending_snapshot_msg_ballot;
    uint64_t expected_snapshot_offset;
    bool pending_snapshot_chunk_ready;

    paxos_pending_read_t pending_reads[MAX_PENDING_READS];
    uint64_t current_read_seq;
    paxos_read_state_t* read_states;
    size_t read_states_cap;
    size_t num_read_states;

    uint32_t current_tick;
    uint32_t heartbeat_timeout;
    uint32_t election_timeout;
    uint32_t randomized_election_timeout;

    paxos_client_session_t client_sessions[MAX_CLIENT_TRACKING];
    uint64_t session_tick_counter;

    bool fatal_error;
};

void paxos_send_immediate(paxos_t* p, paxos_msg_t msg);
void paxos_send_after_persist(paxos_t* p, paxos_msg_t msg);

bool paxos_entry_clone(paxos_entry_t* dst, const paxos_entry_t* src);
void paxos_entry_destroy(paxos_entry_t* e);

static inline bool paxos_entry_value_equal(const paxos_entry_t* a, const paxos_entry_t* b) {
    if (a->type != b->type) return false;
    if (a->client_id != b->client_id) return false;
    if (a->client_seq != b->client_seq) return false;
    if (a->data_len != b->data_len) return false;
    if (a->data_len == 0) return true;
    return memcmp(a->data, b->data, a->data_len) == 0;
}

static inline void observe_higher_ballot(paxos_t* p, uint64_t b) {
    if (b > p->last_observed_ballot) p->last_observed_ballot = b;
    if (b > p->active_ballot && (p->state == PAXOS_STATE_ACTIVE ||
        p->state == PAXOS_STATE_RECOVERING_PHASE1 || p->state == PAXOS_STATE_RECOVERING_PHASE2)) {
        p->state = PAXOS_STATE_LEARNER;
        p->leader_id = 0;
    }
}

bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len);
paxos_entry_t* paxos_log_get(paxos_t* p, uint64_t slot);
paxos_entry_t* paxos_log_extract_unstable(paxos_t* p, size_t* out_count);
paxos_entry_t* paxos_log_extract_range(paxos_t* p, uint64_t start_slot, uint64_t end_slot, size_t* out_count);

// <-- FIXED: Added missing declarations below! -->
paxos_entry_t* paxos_log_extract_suffix(paxos_t* p, uint64_t start_slot, size_t* out_count);
void paxos_advance_local_commit(paxos_t* p);

void paxos_acceptor_step(paxos_t* p, paxos_msg_t* msg);
void paxos_proposer_step(paxos_t* p, paxos_msg_t* msg);
void paxos_proposer_read_barrier_local(paxos_t* p, paxos_msg_t* msg);

#endif // PAXOS_INTERNAL_H

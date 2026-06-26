// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef PAXOS_INTERNAL_H
#define PAXOS_INTERNAL_H

#include "a-paxos-core/paxos.h"
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdatomic.h>

#define MAX_PEERS 64
#define MAX_PENDING_READS 128
#define INFLIGHT_WINDOW 4096
#define MAX_RECOVERY_GAP 100000
#define PAXOS_ENABLE_RECONFIG 1
#define PAXOS_LOG_CHUNK_SIZE 1024

typedef struct {
    _Atomic uint32_t ref_count;
    size_t data_len;
    uint8_t data[];
} paxos_rc_data_t;

static inline uint8_t* paxos_payload_alloc(const uint8_t* data, size_t len) {
    if (len == 0) return NULL;
    paxos_rc_data_t* rc = malloc(sizeof(paxos_rc_data_t) + len);
    if (!rc) return NULL;
    atomic_init(&rc->ref_count, 1);
    rc->data_len = len;
    if (data) memcpy(rc->data, data, len);
    return rc->data;
}

static inline void paxos_payload_retain(uint8_t* data) {
    if (!data) return;
    paxos_rc_data_t* rc = (paxos_rc_data_t*)(data - offsetof(paxos_rc_data_t, data));
    atomic_fetch_add_explicit(&rc->ref_count, 1, memory_order_relaxed);
}

static inline void paxos_payload_release(uint8_t* data) {
    if (!data) return;
    paxos_rc_data_t* rc = (paxos_rc_data_t*)(data - offsetof(paxos_rc_data_t, data));
    if (atomic_fetch_sub_explicit(&rc->ref_count, 1, memory_order_acq_rel) == 1) {
        free(rc);
    }
}

typedef struct {
    bool has_accepted;
    bool is_chosen;
    bool unstable;

    paxos_entry_t accepted_entry;
    paxos_entry_t chosen_entry;
} paxos_log_slot_t;

typedef struct {
    paxos_log_slot_t slots[PAXOS_LOG_CHUNK_SIZE];
} paxos_log_chunk_t;

typedef struct {
    uint64_t highest_ballot_seen;
    paxos_entry_t recovered_value;
    bool has_value;
} paxos_recovery_slot_t;

typedef struct {
    uint64_t slot;
    uint64_t ballot;
    uint64_t ack_mask;
    bool chosen;
    bool active;
} paxos_inflight_slot_t;

typedef struct {
    uint64_t read_seq;
    uint64_t client_ctx;
    uint64_t slot;
    uint64_t ack_mask;
    bool active;
} paxos_pending_read_t;

// FAANG: Explicit Learner State Tracking
typedef struct {
    bool snapshot_installed;
    uint64_t caught_up_through;
    bool hard_state_initialized;
    bool eligible_to_vote;
} paxos_learner_state_t;

struct paxos_s {
    uint64_t id;
    paxos_state_t state;

    uint64_t node_directory[MAX_PEERS];
    size_t num_nodes;
    uint64_t base_config_mask;
    uint64_t active_config_mask;
    uint64_t joint_config_mask;
    bool in_joint_consensus;
    bool pending_reconfig;
    bool needs_conf_final;

    uint64_t promised_ballot;
    uint64_t max_generated_ballot;
    paxos_hard_state_t prev_hard_state;

    paxos_log_chunk_t** log_chunks;
    size_t log_chunks_cap;
    uint64_t log_base_slot;
    uint64_t highest_slot;

    uint64_t* unstable_slots;
    size_t num_unstable_slots;
    size_t unstable_slots_cap;

    uint64_t stable_accepted_through;
    uint64_t leader_commit_hint;
    uint64_t local_commit_index;
    uint64_t last_applied;

    uint64_t snapshot_index;
    uint64_t snapshot_ballot;

    uint64_t active_ballot;
    uint64_t last_observed_ballot;
    uint64_t next_slot;
    uint64_t leader_id;

    // FAANG: Replaced primitive tracking with strict state
    paxos_learner_state_t learner_state[MAX_PEERS];

    uint64_t promise_mask;
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
    uint32_t ticks_since_last_fetch;

    bool fatal_error;
};

void paxos_send_immediate(paxos_t* p, paxos_msg_t msg);
void paxos_send_after_persist(paxos_t* p, paxos_msg_t msg);

bool paxos_entry_clone_deep(paxos_entry_t* dst, const paxos_entry_t* src);
bool paxos_entry_clone_retain(paxos_entry_t* dst, const paxos_entry_t* src);
void paxos_entry_destroy(paxos_entry_t* e);

static inline uint64_t paxos_chunk_idx(paxos_t* p, uint64_t slot) {
    uint64_t base_c = p->log_base_slot / PAXOS_LOG_CHUNK_SIZE;
    uint64_t slot_c = slot / PAXOS_LOG_CHUNK_SIZE;
    if (slot_c < base_c) return 0;
    return slot_c - base_c;
}

static inline uint64_t paxos_chunk_off(uint64_t slot) {
    return slot % PAXOS_LOG_CHUNK_SIZE;
}

static inline uint64_t paxos_peer_bit(paxos_t* p, uint64_t node_id) {
    if (node_id == 0) return 0;
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] == node_id) return 1ULL << i;
    }
    return 0;
}

static inline uint64_t paxos_peer_register(paxos_t* p, uint64_t node_id) {
    if (node_id == 0) return 0;
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] == node_id) return 1ULL << i;
    }
    if (p->num_nodes < MAX_PEERS) {
        p->node_directory[p->num_nodes] = node_id;
        return 1ULL << p->num_nodes++;
    }
    return 0;
}

static inline bool paxos_has_quorum(paxos_t* p, uint64_t ack_mask) {
    int active_acks = __builtin_popcountll(ack_mask & p->active_config_mask);
    int active_req = (__builtin_popcountll(p->active_config_mask) / 2) + 1;
    if (active_acks < active_req) return false;

    if (p->in_joint_consensus) {
        int joint_acks = __builtin_popcountll(ack_mask & p->joint_config_mask);
        int joint_req = (__builtin_popcountll(p->joint_config_mask) / 2) + 1;
        if (joint_acks < joint_req) return false;
    }
    return true;
}

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

void paxos_rebuild_config(paxos_t* p);
bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len);
bool paxos_log_learn_chosen(paxos_t* p, uint64_t slot, const paxos_entry_t* entry);
paxos_entry_t* paxos_log_get(paxos_t* p, uint64_t slot);
paxos_entry_t* paxos_log_get_accepted(paxos_t* p, uint64_t slot);
paxos_entry_t* paxos_log_extract_unstable(paxos_t* p, size_t* out_count);
paxos_entry_t* paxos_log_extract_range(paxos_t* p, uint64_t start_slot, uint64_t end_slot, size_t* out_count);
paxos_entry_t* paxos_log_extract_suffix(paxos_t* p, uint64_t start_slot, size_t* out_count);
void paxos_advance_local_commit(paxos_t* p, uint64_t author_id, uint64_t author_ballot);
void paxos_acceptor_step(paxos_t* p, paxos_msg_t* msg);
void paxos_proposer_step(paxos_t* p, paxos_msg_t* msg);
void paxos_proposer_read_barrier_local(paxos_t* p, paxos_msg_t* msg);

#endif // PAXOS_INTERNAL_H

// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef PAXOS_INTERNAL_H
#define PAXOS_INTERNAL_H

#include "a-paxos-core/paxos.h"
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdatomic.h>

#define PAXOS_INTERNAL_MAX_PENDING_READS 128
#define PAXOS_INTERNAL_INFLIGHT_WINDOW 4096
#define PAXOS_INTERNAL_MAX_RECOVERY_GAP 100000
#define PAXOS_INTERNAL_LOG_CHUNK_SIZE 1024

#ifndef PAXOS_ENABLE_RECONFIG
#define PAXOS_ENABLE_RECONFIG 1
#endif

// PRNG Seed State
typedef struct {
    uint32_t state;
} paxos_prng_t;

typedef struct {
    _Atomic uint32_t ref_count;
    size_t data_len;
    uint8_t data[];
} paxos_rc_data_t;

static inline uint8_t* paxos_payload_alloc(const uint8_t* data, size_t len) {
    if (len == 0 || len > PAXOS_MAX_PAYLOAD_SIZE) return NULL;
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
    paxos_entry_t accepted_entry;
    paxos_entry_t chosen_entry;
    bool has_accepted;
    bool is_chosen;
    bool unstable;
} paxos_log_slot_t;

typedef struct {
    paxos_log_slot_t slots[PAXOS_INTERNAL_LOG_CHUNK_SIZE];
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

typedef struct {
    uint64_t caught_up_through;
    bool snapshot_installed;
    bool hard_state_initialized;
    bool eligible_to_vote;
} paxos_learner_state_t;

typedef struct {
    uint64_t id;
    uint8_t index;
    bool active;
    bool tombstone;
} paxos_peer_map_entry_t;

typedef struct {
    uint64_t node_id;
    uint64_t promised_ballot;
    uint64_t local_commit_index;
    bool is_active;
    uint32_t last_active_tick;
} paxos_peer_state_t;

struct paxos_s {
    uint64_t id;
    paxos_state_t state;
    paxos_prng_t prng;

    uint64_t node_directory[PAXOS_MAX_PEERS];
    paxos_peer_map_entry_t peer_map[128];
    uint64_t allocated_peer_indices;
    size_t num_nodes;

    uint64_t base_config_mask;
    uint64_t active_config_mask;
    uint64_t joint_config_mask;
    bool in_joint_consensus;
    bool pending_reconfig;
    bool needs_conf_final;

    uint64_t promised_ballot;
    uint64_t max_generated_ballot;

    // Track previous states internally for diffing
    uint64_t prev_promised_ballot;
    uint64_t prev_max_generated_ballot;
    uint64_t prev_active_config_mask;
    uint64_t prev_joint_config_mask;
    bool prev_pending_reconfig;

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

    paxos_learner_state_t learner_state[PAXOS_MAX_PEERS];

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

    uint64_t snapshot_offset[PAXOS_MAX_PEERS];

    uint8_t* pending_snapshot_data;
    uint64_t pending_snapshot_offset;
    uint64_t pending_snapshot_from;
    uint64_t pending_snapshot_msg_slot;
    uint64_t pending_snapshot_msg_ballot;
    uint64_t expected_snapshot_offset;
    size_t pending_snapshot_len;
    bool pending_snapshot;
    bool pending_snapshot_done;
    bool pending_snapshot_chunk_ready;

    paxos_pending_read_t pending_reads[PAXOS_INTERNAL_MAX_PENDING_READS];
    uint64_t current_read_seq;
    paxos_read_state_t* read_states;
    size_t read_states_cap;
    size_t num_read_states;

    size_t max_payload_size;
    size_t max_batch_bytes;

    uint32_t current_tick;
    uint32_t heartbeat_timeout;
    uint32_t election_timeout;
    uint32_t randomized_election_timeout;
    uint32_t ticks_since_last_fetch;

    paxos_peer_state_t *peer_states;
    size_t num_peer_states;
    size_t peer_states_cap;

    bool needs_catchup;
    uint64_t catchup_target_index;

    bool fatal_error;
};

void paxos_send_immediate(paxos_t* p, paxos_msg_t msg);
void paxos_send_after_persist(paxos_t* p, paxos_msg_t msg);

bool paxos_entry_clone_deep(paxos_entry_t* dst, const paxos_entry_t* src);
bool paxos_entry_clone_retain(paxos_entry_t* dst, const paxos_entry_t* src);

static inline void paxos_entry_destroy(paxos_entry_t* e) {
    paxos_payload_release(e->data);
    memset(e, 0, sizeof(paxos_entry_t));
}

static inline uint64_t paxos_chunk_idx(paxos_t* p, uint64_t slot) {
    uint64_t base_c = p->log_base_slot / PAXOS_INTERNAL_LOG_CHUNK_SIZE;
    uint64_t slot_c = slot / PAXOS_INTERNAL_LOG_CHUNK_SIZE;
    if (slot_c < base_c) return 0;
    return slot_c - base_c;
}

static inline uint64_t paxos_chunk_off(uint64_t slot) {
    return slot % PAXOS_INTERNAL_LOG_CHUNK_SIZE;
}

static inline uint64_t paxos_peer_bit(paxos_t* p, uint64_t node_id) {
    if (node_id == 0) return 0;
    uint64_t start_idx = node_id % 128;
    for (int i = 0; i < 128; i++) {
        uint64_t idx = (start_idx + i) % 128;
        if (p->peer_map[idx].active && p->peer_map[idx].id == node_id) return 1ULL << p->peer_map[idx].index;
        if (!p->peer_map[idx].active && !p->peer_map[idx].tombstone) return 0;
    }
    return 0;
}

static inline uint64_t paxos_peer_register(paxos_t* p, uint64_t node_id) {
    if (node_id == 0) return 0;
    uint64_t start_idx = node_id % 128;
    int insert_idx = -1;

    for (int i = 0; i < 128; i++) {
        uint64_t idx = (start_idx + i) % 128;
        if (p->peer_map[idx].active && p->peer_map[idx].id == node_id) return 1ULL << p->peer_map[idx].index;
        if (!p->peer_map[idx].active && insert_idx == -1) insert_idx = idx;
        if (!p->peer_map[idx].active && !p->peer_map[idx].tombstone) break;
    }

    if (insert_idx == -1 || p->num_nodes >= PAXOS_MAX_PEERS) return 0;

    if (p->allocated_peer_indices == ~0ULL) return 0;

    uint8_t free_index = __builtin_ctzll(~p->allocated_peer_indices);
    p->allocated_peer_indices |= (1ULL << free_index);

    p->peer_map[insert_idx].active = true;
    p->peer_map[insert_idx].tombstone = false;
    p->peer_map[insert_idx].id = node_id;
    p->peer_map[insert_idx].index = free_index;
    p->node_directory[free_index] = node_id;
    p->num_nodes++;
    return 1ULL << free_index;
}

static inline void paxos_peer_deregister(paxos_t* p, uint64_t node_id) {
    if (node_id == 0) return;
    uint64_t start_idx = node_id % 128;
    for (int i = 0; i < 128; i++) {
        uint64_t idx = (start_idx + i) % 128;
        if (p->peer_map[idx].active && p->peer_map[idx].id == node_id) {
            p->allocated_peer_indices &= ~(1ULL << p->peer_map[idx].index);
            p->peer_map[idx].active = false;
            p->peer_map[idx].tombstone = true;
            p->node_directory[p->peer_map[idx].index] = 0;
            p->num_nodes--;
            return;
        }
        if (!p->peer_map[idx].active && !p->peer_map[idx].tombstone) return;
    }
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

static inline uint64_t paxos_entry_hash(const paxos_entry_t* e) {
    if (!e) return 0;
    uint64_t hash = 14695981039346656037ULL;

    uint64_t meta[4] = { e->slot, (uint64_t)e->type, e->client_id, e->client_seq };
    uint8_t* p_bytes = (uint8_t*)meta;
    for (size_t i = 0; i < sizeof(meta); i++) {
        hash ^= p_bytes[i];
        hash *= 1099511628211ULL;
    }

    if (e->data && e->data_len > 0) {
        for (size_t i = 0; i < e->data_len; i++) {
            hash ^= e->data[i];
            hash *= 1099511628211ULL;
        }
    }
    return hash;
}

static inline uint64_t paxos_batch_hash(paxos_entry_t* entries, size_t num_entries) {
    if (num_entries == 0 || !entries) return 0;
    uint64_t cumulative_hash = 14695981039346656037ULL;
    for (size_t k = 0; k < num_entries; k++) {
        cumulative_hash ^= paxos_entry_hash(&entries[k]);
        cumulative_hash *= 1099511628211ULL;
    }
    return cumulative_hash;
}

static inline void observe_higher_ballot(paxos_t* p, uint64_t b) {
    if (b > p->last_observed_ballot) p->last_observed_ballot = b;
    if (b > p->active_ballot && (p->state == PAXOS_STATE_ACTIVE ||
        p->state == PAXOS_STATE_RECOVERING_PHASE1 || p->state == PAXOS_STATE_RECOVERING_PHASE2)) {
        printf("[NODE %llu] 📉 Observed higher ballot %llu. Stepping down to LEARNER.\n",
                       (unsigned long long)p->id, (unsigned long long)b);
        p->state = PAXOS_STATE_LEARNER;
        p->leader_id = 0;
    }
}

paxos_err_t paxos_register_learner(paxos_t* p, uint64_t node_id);
void paxos_rebuild_config(paxos_t* p);
bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, paxos_entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len);
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

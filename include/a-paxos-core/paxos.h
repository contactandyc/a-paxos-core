// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef PAXOS_H
#define PAXOS_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#if defined(__GNUC__) || defined(__clang__)
    #define PAXOS_NODISCARD __attribute__((warn_unused_result))
#else
    #define PAXOS_NODISCARD
#endif

#define PAXOS_MAX_PAYLOAD_SIZE (1048576)
#define PAXOS_MAX_PEERS 64

typedef enum {
    PAXOS_OK = 0,
    PAXOS_ERR_NOT_ACTIVE = -1,
    PAXOS_ERR_NOMEM = -2,
    PAXOS_ERR_QUEUE_FULL = -3,
    PAXOS_ERR_OVERSIZED_PAYLOAD = -4,
    PAXOS_ERR_CONFIG_IN_FLIGHT = -5,
    PAXOS_ERR_INVALID_ARG = -6,
    PAXOS_ERR_LEARNER_NOT_READY = -7
} paxos_err_t;

typedef enum {
    PAXOS_STATE_LEARNER,
    PAXOS_STATE_RECOVERING_PHASE1,
    PAXOS_STATE_RECOVERING_PHASE2,
    PAXOS_STATE_ACTIVE
} paxos_state_t;

typedef enum {
    PAXOS_MSG_HEARTBEAT, // Renamed from TICK to clarify domain intent
    PAXOS_MSG_PROPOSE,
    PAXOS_MSG_PREPARE,
    PAXOS_MSG_PROMISE,
    PAXOS_MSG_ACCEPT,
    PAXOS_MSG_ACCEPTED,
    PAXOS_MSG_NACK,
    PAXOS_MSG_COMMIT_NOTICE,
    PAXOS_MSG_READ_BARRIER,
    PAXOS_MSG_READ_BARRIER_RES,
    PAXOS_MSG_FETCH_ENTRIES,
    PAXOS_MSG_FETCH_ENTRIES_RES,
    PAXOS_MSG_INSTALL_SNAPSHOT,
    PAXOS_MSG_INSTALL_SNAPSHOT_RES,
    PAXOS_MSG_PROMOTE_REQUEST
} paxos_msg_type_t;

typedef enum {
    PAXOS_ENTRY_NORMAL = 0,
    PAXOS_ENTRY_NOOP = 1,
    PAXOS_ENTRY_CONF_ADD = 2,
    PAXOS_ENTRY_CONF_REMOVE = 3,
    PAXOS_ENTRY_CONF_JOINT = 254,
    PAXOS_ENTRY_CONF_FINAL = 255
} paxos_entry_type_t;

// Structs packed largest-to-smallest to prevent cache-line padding waste
typedef struct {
    uint64_t slot;
    uint64_t accepted_ballot;
    uint64_t client_id;
    uint64_t client_seq;
    uint8_t* data;
    size_t   data_len;
    paxos_entry_type_t type;
} paxos_entry_t;

typedef struct {
    paxos_entry_t entry;
    bool chosen;
} paxos_restored_entry_t;

typedef struct {
    uint64_t to;
    uint64_t from;
    uint64_t ballot;
    uint64_t promised_ballot;
    uint64_t slot;
    uint64_t commit_index;
    paxos_entry_t* entries;
    size_t num_entries;
    uint64_t read_seq;
    uint64_t value_hash; // Deterministic state checksum
    uint8_t* snapshot_data;
    size_t snapshot_len;
    uint64_t snapshot_offset;
    paxos_msg_type_t type;
    bool snapshot_done;
    bool reject;
} paxos_msg_t;

typedef struct {
    uint64_t slot;
    uint64_t read_seq;
} paxos_read_state_t;

// Rewritten to use actual Node IDs instead of internal bitmasks for disk safety
typedef struct {
    uint64_t promised_ballot;
    uint64_t max_generated_ballot;
    uint64_t active_nodes[PAXOS_MAX_PEERS];
    size_t num_active_nodes;
    uint64_t joint_nodes[PAXOS_MAX_PEERS];
    size_t num_joint_nodes;
    bool pending_reconfig;
    bool has_update;
} paxos_hard_state_t;

/*
 * paxos_ready_t is a borrowed view into the engine.
 * The arrays and pointers provided here are owned by the library and
 * remain valid ONLY until the next call to paxos_advance().
 * DO NOT free these pointers manually.
 */
typedef struct {
    paxos_hard_state_t hard_state;

    paxos_entry_t* entries_to_save;
    size_t num_entries_to_save;

    paxos_msg_t* messages_immediate;
    size_t num_messages_immediate;

    paxos_msg_t* messages_after_persist;
    size_t num_messages_after_persist;

    paxos_entry_t* chosen_entries;
    size_t num_chosen_entries;

    paxos_read_state_t* read_states;
    size_t num_read_states;

    uint64_t snapshot_slot;
    uint64_t snapshot_ballot;
    uint8_t* snapshot_data;
    size_t snapshot_len;
    uint64_t snapshot_offset;
    bool install_snapshot;
    bool snapshot_done;
} paxos_ready_t;

// ABI Configuration Structs
typedef struct {
    size_t struct_size;
    uint64_t node_id;
    const uint64_t* initial_voters;
    size_t num_initial_voters;
    uint32_t heartbeat_ticks;
    uint32_t election_ticks;
    size_t max_payload_size;
    size_t max_batch_bytes;
} paxos_config_t;

typedef struct {
    size_t struct_size;
    paxos_hard_state_t hard_state;
    uint64_t local_commit_index;
    uint64_t snapshot_index;
    const paxos_restored_entry_t* entries;
    size_t num_entries;
} paxos_restore_data_t;

// Opaque context
typedef struct paxos_s paxos_t;

// Lifecycle
PAXOS_NODISCARD paxos_err_t paxos_create(const paxos_config_t* cfg, paxos_t** out);
PAXOS_NODISCARD paxos_err_t paxos_restore(const paxos_config_t* cfg, const paxos_restore_data_t* restore, paxos_t** out);
void paxos_destroy(paxos_t* p);

// Domain Verbs
PAXOS_NODISCARD paxos_err_t paxos_propose(paxos_t* p, uint64_t client_id, uint64_t client_seq, const void* data, size_t data_len);
PAXOS_NODISCARD paxos_err_t paxos_add_node(paxos_t* p, uint64_t node_id);
PAXOS_NODISCARD paxos_err_t paxos_remove_node(paxos_t* p, uint64_t node_id);
PAXOS_NODISCARD paxos_err_t paxos_request_promotion(paxos_t* p);
PAXOS_NODISCARD paxos_err_t paxos_read_barrier(paxos_t* p, uint64_t read_seq);
PAXOS_NODISCARD paxos_err_t paxos_receive(paxos_t* p, const paxos_msg_t* msg);
PAXOS_NODISCARD paxos_err_t paxos_tick(paxos_t* p);

// I/O & State Transitions
PAXOS_NODISCARD paxos_err_t paxos_get_ready(paxos_t* p, paxos_ready_t* out);
void paxos_advance(paxos_t* p, const uint64_t* stable_slots, size_t num_stable_slots, uint64_t applied_slot);

PAXOS_NODISCARD paxos_err_t paxos_set_snapshot_chunk(paxos_t* p, uint64_t peer_id, const uint8_t* data, size_t len, uint64_t offset, bool done);
void paxos_snapshot_acked(paxos_t* p, bool success);
void paxos_compact(paxos_t* p, uint64_t compact_slot);

// Getters
paxos_state_t paxos_state(paxos_t* p);
uint64_t paxos_promised_ballot(paxos_t* p);
uint64_t paxos_local_commit_index(paxos_t* p);
uint64_t paxos_snapshot_index(paxos_t* p);
uint64_t paxos_last_slot(paxos_t* p);
bool paxos_has_fatal_error(paxos_t* p);

#endif // PAXOS_H

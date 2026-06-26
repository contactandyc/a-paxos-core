// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef PAXOS_H
#define PAXOS_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#define PAXOS_MAX_PAYLOAD_SIZE (1048576)

typedef enum {
    PAXOS_OK = 0,
    PAXOS_ERR_NOT_ACTIVE = -1,
    PAXOS_ERR_NOMEM = -2,
    PAXOS_ERR_QUEUE_FULL = -3,
    PAXOS_ERR_OVERSIZED_PAYLOAD = -4,
    PAXOS_ERR_CONFIG_IN_FLIGHT = -5,
    PAXOS_ERR_INVALID_ARG = -6
} paxos_err_t;

typedef enum {
    PAXOS_STATE_LEARNER,
    PAXOS_STATE_RECOVERING_PHASE1,
    PAXOS_STATE_RECOVERING_PHASE2,
    PAXOS_STATE_ACTIVE
} paxos_state_t;

typedef enum {
    MSG_TICK,
    MSG_PROPOSE,
    MSG_PREPARE,
    MSG_PROMISE,
    MSG_ACCEPT,
    MSG_ACCEPTED,
    MSG_NACK,
    MSG_COMMIT_NOTICE,
    MSG_READ_BARRIER,
    MSG_READ_BARRIER_RES,
    MSG_FETCH_ENTRIES,
    MSG_FETCH_ENTRIES_RES,
    MSG_INSTALL_SNAPSHOT,      // <-- NEW: Leader streams snapshot chunk
    MSG_INSTALL_SNAPSHOT_RES   // <-- NEW: Follower acknowledges chunk
} msg_type_t;

typedef enum {
    ENTRY_NORMAL = 0,
    ENTRY_NOOP = 1,
    ENTRY_CONF_ADD = 2,
    ENTRY_CONF_REMOVE = 3,
    ENTRY_CONF_JOINT = 254,  // NEW: Alpha-Window Joint Consensus
    ENTRY_CONF_FINAL = 255   // NEW: Topology Finalization
} entry_type_t;

typedef struct {
    uint64_t slot;
    uint64_t accepted_ballot;
    entry_type_t type;
    uint64_t client_id;
    uint64_t client_seq;
    uint8_t* data;
    size_t   data_len;
} paxos_entry_t;

typedef struct {
    paxos_entry_t entry;
    bool chosen;
} paxos_restored_entry_t;

typedef struct {
    msg_type_t type;
    uint64_t to;
    uint64_t from;
    uint64_t ballot;
    uint64_t promised_ballot;
    uint64_t slot;
    uint64_t commit_index;
    paxos_entry_t* entries;
    size_t num_entries;
    uint64_t read_seq;

    // NEW: Snapshot streaming metadata
    uint8_t* snapshot_data;
    size_t snapshot_len;
    uint64_t snapshot_offset;
    bool snapshot_done;

    bool reject;
} paxos_msg_t;

typedef struct {
    uint64_t slot;
    uint64_t read_seq;
} paxos_read_state_t;

typedef struct {
    uint64_t promised_ballot;
    uint64_t max_generated_ballot;
    // FAANG: Durable Configuration Metadata
    uint64_t active_config_mask;
    uint64_t joint_config_mask;
    bool pending_reconfig;
    bool has_update;
} paxos_hard_state_t;

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

    // NEW: Snapshot receiver commands for the host application
    bool install_snapshot;
    uint64_t snapshot_slot;
    uint64_t snapshot_ballot;
    uint8_t* snapshot_data;
    size_t snapshot_len;
    uint64_t snapshot_offset;
    bool snapshot_done;
} paxos_ready_t;

typedef struct paxos_s paxos_t;

paxos_t* paxos_create(uint64_t id, uint64_t* peers, size_t num_peers);

paxos_t* paxos_restore(uint64_t id, uint64_t* peers, size_t num_peers,
                       paxos_hard_state_t hard_state,
                       uint64_t local_commit_index, uint64_t snapshot_index,
                       paxos_restored_entry_t* entries, size_t num_entries);

void     paxos_destroy(paxos_t* p);

void          paxos_step_local(paxos_t* p, paxos_msg_t* msg);
void          paxos_step_remote(paxos_t* p, paxos_msg_t* msg);

void paxos_tick(paxos_t* p);

paxos_ready_t paxos_get_ready(paxos_t* p);
void          paxos_ready_destroy(paxos_ready_t* ready);
void paxos_advance(paxos_t* p, const uint64_t* stable_slots, size_t num_stable_slots, uint64_t applied_slot);

// Provides snapshot data chunks to the core for replication to a follower.
// The host application calls this when intercepting an empty MSG_INSTALL_SNAPSHOT.
bool paxos_set_snapshot_chunk(
    paxos_t* p,
    uint64_t peer_id,
    const uint8_t* data,
    size_t len,
    uint64_t offset,
    bool done
);

// NEW: Core Snapshot Mechanics
void          paxos_compact(paxos_t* p, uint64_t compact_slot);
void          paxos_snapshot_acked(paxos_t* p, bool success);

paxos_state_t paxos_state(paxos_t* p);
uint64_t      paxos_promised_ballot(paxos_t* p);
uint64_t      paxos_local_commit_index(paxos_t* p);
uint64_t      paxos_snapshot_index(paxos_t* p);
uint64_t      paxos_last_slot(paxos_t* p);
bool          paxos_has_fatal_error(paxos_t* p);

// Explicitly wake up a new node:
void paxos_register_learner(paxos_t* p, uint64_t node_id);

#endif // PAXOS_H

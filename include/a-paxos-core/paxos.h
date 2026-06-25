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
    PAXOS_STATE_LEARNER,    // Passive follower/acceptor
    PAXOS_STATE_RECOVERING, // Phase 1: Candidate building a promise quorum
    PAXOS_STATE_ACTIVE      // Phase 2: Stable Leader proposing new slots
} paxos_state_t;

typedef enum {
    MSG_TICK,
    MSG_PROPOSE,
    MSG_PREPARE,         // Phase 1 (Leader -> Acceptors)
    MSG_PROMISE,         // Phase 1 (Acceptors -> Leader)
    MSG_ACCEPT,          // Phase 2 (Leader -> Acceptors)
    MSG_ACCEPTED,        // Phase 2 (Acceptors -> Leader)
    MSG_NACK,            // Rejection containing highest promised ballot
    MSG_COMMIT_NOTICE,   // Learner Catchup
    MSG_READ_BARRIER,
    MSG_READ_BARRIER_RES
} msg_type_t;

typedef enum {
    ENTRY_NORMAL = 0,
    ENTRY_NOOP = 1,      // Used to fill Paxos gaps during Phase 1 recovery
    ENTRY_CONF_ADD = 2,
    ENTRY_CONF_REMOVE = 3
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
    msg_type_t type;
    uint64_t to;
    uint64_t from;

    uint64_t ballot;          // The active or proposed ballot
    uint64_t promised_ballot; // Used in NACKs and Promises

    uint64_t slot;            // The log index being targeted
    uint64_t commit_index;    // Piggybacked chosen horizon

    paxos_entry_t* entries;   // Suffix array for Promise, or single entry for Accept
    size_t num_entries;

    uint64_t read_seq;
    bool reject;
} paxos_msg_t;

typedef struct {
    uint64_t slot;
    uint64_t read_seq;
} paxos_read_state_t;

typedef struct {
    paxos_msg_t* messages;
    size_t num_messages;
    paxos_entry_t* entries_to_save;
    size_t num_entries_to_save;
    paxos_entry_t* chosen_entries;
    size_t num_chosen_entries;
    paxos_read_state_t* read_states;
    size_t num_read_states;
} paxos_ready_t;

typedef struct paxos_s paxos_t;

paxos_t* paxos_create(uint64_t id, uint64_t* peers, size_t num_peers);
void     paxos_destroy(paxos_t* p);

void          paxos_step_local(paxos_t* p, paxos_msg_t* msg);
void          paxos_step_remote(paxos_t* p, paxos_msg_t* msg);
paxos_ready_t paxos_get_ready(paxos_t* p);
void          paxos_advance(paxos_t* p, uint64_t saved_slot, uint64_t applied_slot);

paxos_state_t paxos_state(paxos_t* p);
uint64_t      paxos_promised_ballot(paxos_t* p);
uint64_t      paxos_commit_index(paxos_t* p);
uint64_t      paxos_last_slot(paxos_t* p);

#endif // PAXOS_H

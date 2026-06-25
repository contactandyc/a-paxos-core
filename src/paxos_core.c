// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "paxos_internal.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// MEMORY & INITIALIZATION
// ============================================================================

paxos_t* paxos_create(uint64_t id, uint64_t* peers, size_t num_peers) {
    if (id == 0 || num_peers > MAX_REMOTE_PEERS) return NULL;

    paxos_t* p = calloc(1, sizeof(paxos_t));
    if (!p) return NULL;

    p->id = id;
    p->state = PAXOS_STATE_LEARNER; // Everyone starts as a passive learner
    p->num_peers = num_peers;
    for (size_t i = 0; i < num_peers; i++) {
        p->peers[i] = peers[i];
    }

    p->log_cap = 1024;
    p->log_base_slot = 1;
    p->log = calloc(p->log_cap, sizeof(paxos_entry_t));
    p->log_has_value = calloc(p->log_cap, sizeof(bool));

    // Inflight tracking for Phase 2 (Ring Buffer)
    p->inflight = calloc(4096, sizeof(paxos_inflight_slot_t));

    // Recovery Buffer for Phase 1
    p->recovery_cap = 1024;
    p->recovery_buffer = calloc(p->recovery_cap, sizeof(paxos_recovery_slot_t));

    if (!p->log || !p->log_has_value || !p->inflight || !p->recovery_buffer) {
        paxos_destroy(p);
        return NULL;
    }

    return p;
}

void paxos_destroy(paxos_t* p) {
    if (!p) return;

    if (p->log) {
        for (size_t i = 0; i < p->log_cap; i++) {
            if (p->log_has_value[i] && p->log[i].data) free(p->log[i].data);
        }
        free(p->log);
    }

    if (p->log_has_value) free(p->log_has_value);
    if (p->inflight) free(p->inflight);

    if (p->recovery_buffer) {
        for(size_t i = 0; i < p->recovery_cap; i++) {
            if (p->recovery_buffer[i].has_value && p->recovery_buffer[i].recovered_value.data) {
                free(p->recovery_buffer[i].recovered_value.data);
            }
        }
        free(p->recovery_buffer);
    }

    if (p->msg_queue) {
        for (size_t i = 0; i < p->msg_queue_len; i++) {
            paxos_msg_t* m = &p->msg_queue[i];
            if (m->entries) {
                // FIXED: Only MSG_PROMISE contains dynamically allocated, deep-copied entries.
                // MSG_ACCEPT points directly into the zero-copy p->log array!
                if (m->type == MSG_PROMISE) {
                    for (size_t j = 0; j < m->num_entries; j++) {
                        if (m->entries[j].data) free(m->entries[j].data);
                    }
                    free(m->entries);
                }
            }
        }
        free(p->msg_queue);
    }

    free(p);
}

void paxos_send_msg(paxos_t* p, paxos_msg_t msg) {
    if (p->msg_queue_len >= p->msg_queue_cap) {
        size_t new_cap = p->msg_queue_cap == 0 ? 16 : p->msg_queue_cap * 2;
        paxos_msg_t* new_q = realloc(p->msg_queue, new_cap * sizeof(paxos_msg_t));
        if (!new_q) { p->fatal_error = true; return; }
        p->msg_queue = new_q;
        p->msg_queue_cap = new_cap;
    }
    msg.from = p->id;
    p->msg_queue[p->msg_queue_len++] = msg;
}

// ============================================================================
// PUBLIC ROUTERS
// ============================================================================

void paxos_step_local(paxos_t* p, paxos_msg_t* msg) {
    if (p->fatal_error) return;

    if (msg->type == MSG_PROPOSE) {
        if (p->state != PAXOS_STATE_ACTIVE) return; // Drop if not stable leader

        // Single Entry Phase 2 Pipeline
        uint64_t slot = p->next_slot++;
        paxos_entry_t* in_e = &msg->entries[0];

        // Save to our own sparse log
        if (!paxos_log_accept(p, slot, p->active_ballot, in_e->type, in_e->client_id, in_e->client_seq, in_e->data, in_e->data_len)) {
            return;
        }

        // Broadcast Accept
        paxos_msg_t acc = {
            .type = MSG_ACCEPT,
            .ballot = p->active_ballot,
            .slot = slot,
            .commit_index = p->commit_index,
            .entries = paxos_log_get(p, slot),
            .num_entries = 1
        };

        for (size_t i = 0; i < p->num_peers; i++) {
            acc.to = p->peers[i];
            paxos_send_msg(p, acc);
        }
    }
}

void paxos_step_remote(paxos_t* p, paxos_msg_t* msg) {
    if (p->fatal_error || msg->to != p->id) return;

    switch (msg->type) {
        // Acceptor Role handles Phase 1 and 2 incoming requests
        case MSG_PREPARE:
        case MSG_ACCEPT:
        case MSG_COMMIT_NOTICE:
            paxos_acceptor_step(p, msg);
            break;

        // Proposer Role handles Phase 1 and 2 incoming responses
        case MSG_PROMISE:
        case MSG_ACCEPTED:
        case MSG_NACK:
            paxos_proposer_step(p, msg);
            break;

        default:
            break;
    }
}

// ============================================================================
// THE READY PATTERN
// ============================================================================

paxos_ready_t paxos_get_ready(paxos_t* p) {
    paxos_ready_t ready = {0};
    if (p->fatal_error) return ready;

    ready.messages = p->msg_queue;
    ready.num_messages = p->msg_queue_len;

    // We pass the new accepted entries to the storage layer.
    // In a highly optimized engine, we would track exactly which slots are dirty.
    // For this bridge, we extract the uncommitted suffix.
    ready.entries_to_save = paxos_log_extract_suffix(p, p->snapshot_index + 1, &ready.num_entries_to_save);

    // Extract contiguous chosen entries for the state machine
    if (p->commit_index > p->last_applied) {
        size_t apply_count = p->commit_index - p->last_applied;
        ready.chosen_entries = calloc(apply_count, sizeof(paxos_entry_t));

        if (ready.chosen_entries) {
            size_t valid = 0;
            for (uint64_t i = p->last_applied + 1; i <= p->commit_index; i++) {
                paxos_entry_t* e = paxos_log_get(p, i);
                if (e) {
                    ready.chosen_entries[valid] = *e;
                    if (e->data_len > 0) {
                        ready.chosen_entries[valid].data = malloc(e->data_len);
                        memcpy(ready.chosen_entries[valid].data, e->data, e->data_len);
                    }
                    valid++;
                }
            }
            ready.num_chosen_entries = valid;
        }
    }

    return ready;
}

void paxos_advance(paxos_t* p, uint64_t saved_slot, uint64_t applied_slot) {
    if (p->fatal_error) return;

    if (applied_slot > p->last_applied) {
        p->last_applied = applied_slot;
    }

    // Clean up outbound message allocations
    if (p->msg_queue) {
        for (size_t i = 0; i < p->msg_queue_len; i++) {
            if (p->msg_queue[i].entries) {
                // If it was a PROMISE message, we deep copied the suffix array.
                if (p->msg_queue[i].type == MSG_PROMISE) {
                    for(size_t j=0; j < p->msg_queue[i].num_entries; j++) {
                        if (p->msg_queue[i].entries[j].data) free(p->msg_queue[i].entries[j].data);
                    }
                    free(p->msg_queue[i].entries);
                }
            }
        }
    }
    p->msg_queue_len = 0;
}

// ============================================================================
// GETTERS
// ============================================================================

paxos_state_t paxos_state(paxos_t* p) { return p ? p->state : PAXOS_STATE_LEARNER; }
uint64_t paxos_promised_ballot(paxos_t* p) { return p ? p->promised_ballot : 0; }
uint64_t paxos_commit_index(paxos_t* p) { return p ? p->commit_index : 0; }
uint64_t paxos_last_slot(paxos_t* p) {
    if (!p) return 0;
    uint64_t highest = p->snapshot_index; // <-- FIXED: Starts at 0
    for(size_t i = 0; i < p->log_cap; i++) {
        if (p->log_has_value[i] && (p->log_base_slot + i) > highest) {
            highest = p->log_base_slot + i;
        }
    }
    return highest;
}

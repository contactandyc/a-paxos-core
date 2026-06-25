// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "paxos_internal.h"
#include <stdlib.h>
#include <string.h>

paxos_t* paxos_create(uint64_t id, uint64_t* peers, size_t num_peers) {
    if (id == 0 || num_peers > MAX_REMOTE_PEERS) return NULL;

    paxos_t* p = calloc(1, sizeof(paxos_t));
    if (!p) return NULL;

    p->id = id;
    p->state = PAXOS_STATE_LEARNER;
    p->num_peers = num_peers;
    for (size_t i = 0; i < num_peers; i++) p->peers[i] = peers[i];

    p->log_cap = 1024;
    p->log_base_slot = 1;
    p->log = calloc(p->log_cap, sizeof(paxos_log_slot_t));
    p->inflight = calloc(4096, sizeof(paxos_inflight_slot_t));

    p->recovery_cap = 1024;
    p->recovery_buffer = calloc(p->recovery_cap, sizeof(paxos_recovery_slot_t));

    if (!p->log || !p->inflight || !p->recovery_buffer) {
        paxos_destroy(p);
        return NULL;
    }

    return p;
}

void paxos_destroy(paxos_t* p) {
    if (!p) return;

    if (p->log) {
        for (size_t i = 0; i < p->log_cap; i++) paxos_entry_destroy(&p->log[i].entry);
        free(p->log);
    }

    if (p->inflight) free(p->inflight);

    if (p->recovery_buffer) {
        for(size_t i = 0; i < p->recovery_cap; i++) paxos_entry_destroy(&p->recovery_buffer[i].recovered_value);
        free(p->recovery_buffer);
    }

    if (p->msg_queue_immediate) free(p->msg_queue_immediate);

    if (p->msg_queue_after_persist) {
        for (size_t i = 0; i < p->msg_queue_after_persist_len; i++) {
            paxos_msg_t* m = &p->msg_queue_after_persist[i];
            if ((m->type == MSG_PROMISE || m->type == MSG_FETCH_ENTRIES_RES) && m->entries) {
                for (size_t j = 0; j < m->num_entries; j++) paxos_entry_destroy(&m->entries[j]);
                free(m->entries);
            }
        }
        free(p->msg_queue_after_persist);
    }

    if (p->pending_snapshot_data) free(p->pending_snapshot_data);

    free(p);
}

void paxos_send_immediate(paxos_t* p, paxos_msg_t msg) {
    if (p->msg_queue_immediate_len >= p->msg_queue_immediate_cap) {
        size_t new_cap = p->msg_queue_immediate_cap == 0 ? 16 : p->msg_queue_immediate_cap * 2;
        paxos_msg_t* new_q = realloc(p->msg_queue_immediate, new_cap * sizeof(paxos_msg_t));
        if (!new_q) { p->fatal_error = true; return; }
        p->msg_queue_immediate = new_q;
        p->msg_queue_immediate_cap = new_cap;
    }
    msg.from = p->id;
    p->msg_queue_immediate[p->msg_queue_immediate_len++] = msg;
}

void paxos_send_after_persist(paxos_t* p, paxos_msg_t msg) {
    if (p->msg_queue_after_persist_len >= p->msg_queue_after_persist_cap) {
        size_t new_cap = p->msg_queue_after_persist_cap == 0 ? 16 : p->msg_queue_after_persist_cap * 2;
        paxos_msg_t* new_q = realloc(p->msg_queue_after_persist, new_cap * sizeof(paxos_msg_t));
        if (!new_q) { p->fatal_error = true; return; }
        p->msg_queue_after_persist = new_q;
        p->msg_queue_after_persist_cap = new_cap;
    }
    msg.from = p->id;
    p->msg_queue_after_persist[p->msg_queue_after_persist_len++] = msg;
}

void paxos_step_local(paxos_t* p, paxos_msg_t* msg) {
    if (p->fatal_error) return;

    if (msg->type == MSG_PROPOSE) {
        if (p->state != PAXOS_STATE_ACTIVE) return;

        uint64_t slot = p->next_slot++;
        paxos_entry_t* in_e = &msg->entries[0];

        if (!paxos_log_accept(p, slot, p->active_ballot, in_e->type, in_e->client_id, in_e->client_seq, in_e->data, in_e->data_len)) {
            return;
        }

        paxos_inflight_slot_t* inf = &p->inflight[slot % 4096];
        inf->slot = slot;
        inf->ballot = p->active_ballot;
        inf->acks = 1;
        memset(inf->acked_by, 0, MAX_PEERS);
        inf->chosen = false;
        inf->active = true;

        paxos_msg_t acc = { .type = MSG_ACCEPT, .ballot = p->active_ballot, .slot = slot, .commit_index = p->local_commit_index, .entries = paxos_log_get(p, slot), .num_entries = 1 };
        for (size_t i = 0; i < p->num_peers; i++) {
            acc.to = p->peers[i];
            paxos_send_after_persist(p, acc);
        }
    }
}

void paxos_step_remote(paxos_t* p, paxos_msg_t* msg) {
    if (p->fatal_error || msg->to != p->id) return;
    switch (msg->type) {
        case MSG_PREPARE: case MSG_ACCEPT: case MSG_COMMIT_NOTICE: case MSG_FETCH_ENTRIES_RES: case MSG_INSTALL_SNAPSHOT:
            paxos_acceptor_step(p, msg); break;
        case MSG_PROMISE: case MSG_ACCEPTED: case MSG_NACK: case MSG_FETCH_ENTRIES: case MSG_INSTALL_SNAPSHOT_RES:
            paxos_proposer_step(p, msg); break;
        default: break;
    }
}

paxos_ready_t paxos_get_ready(paxos_t* p) {
    paxos_ready_t ready = {0};
    if (p->fatal_error) return ready;

    ready.hard_state.promised_ballot = p->promised_ballot;
    ready.hard_state.max_generated_ballot = p->max_generated_ballot;
    ready.hard_state.has_update = (p->promised_ballot != p->prev_hard_state.promised_ballot) ||
                                  (p->max_generated_ballot != p->prev_hard_state.max_generated_ballot);

    ready.entries_to_save = paxos_log_extract_suffix(p, p->stable_accepted_through + 1, &ready.num_entries_to_save);

    ready.messages_immediate = p->msg_queue_immediate;
    ready.num_messages_immediate = p->msg_queue_immediate_len;

    ready.messages_after_persist = p->msg_queue_after_persist;
    ready.num_messages_after_persist = p->msg_queue_after_persist_len;

    if (p->local_commit_index > p->last_applied) {
        size_t apply_count = p->local_commit_index - p->last_applied;
        ready.chosen_entries = calloc(apply_count, sizeof(paxos_entry_t));

        if (ready.chosen_entries) {
            size_t valid = 0;
            for (uint64_t i = p->last_applied + 1; i <= p->local_commit_index; i++) {
                paxos_entry_t* e = paxos_log_get(p, i);
                if (e) {
                    paxos_entry_clone(&ready.chosen_entries[valid], e);
                    valid++;
                }
            }
            ready.num_chosen_entries = valid;
        }
    }

    // NEW: Expose Snapshot Install Directives
    ready.install_snapshot = p->pending_snapshot_chunk_ready;
    ready.snapshot_slot = p->pending_snapshot_msg_slot;
    ready.snapshot_ballot = p->pending_snapshot_msg_ballot;
    ready.snapshot_data = p->pending_snapshot_data;
    ready.snapshot_len = p->pending_snapshot_len;
    ready.snapshot_offset = p->pending_snapshot_offset;
    ready.snapshot_done = p->pending_snapshot_done;

    return ready;
}

void paxos_ready_destroy(paxos_ready_t* ready) {
    if (ready->entries_to_save) {
        for (size_t i = 0; i < ready->num_entries_to_save; i++) paxos_entry_destroy(&ready->entries_to_save[i]);
        free(ready->entries_to_save);
    }
    if (ready->chosen_entries) {
        for (size_t i = 0; i < ready->num_chosen_entries; i++) paxos_entry_destroy(&ready->chosen_entries[i]);
        free(ready->chosen_entries);
    }
}

void paxos_advance(paxos_t* p, uint64_t stable_accepted_through, uint64_t applied_slot) {
    if (p->fatal_error) return;

    if (stable_accepted_through > p->stable_accepted_through) p->stable_accepted_through = stable_accepted_through;
    if (applied_slot > p->last_applied) p->last_applied = applied_slot;

    p->prev_hard_state.promised_ballot = p->promised_ballot;
    p->prev_hard_state.max_generated_ballot = p->max_generated_ballot;

    p->msg_queue_immediate_len = 0;

    if (p->msg_queue_after_persist) {
        for (size_t i = 0; i < p->msg_queue_after_persist_len; i++) {
            if ((p->msg_queue_after_persist[i].type == MSG_PROMISE ||
                 p->msg_queue_after_persist[i].type == MSG_FETCH_ENTRIES_RES) &&
                 p->msg_queue_after_persist[i].entries) {
                for(size_t j = 0; j < p->msg_queue_after_persist[i].num_entries; j++) {
                    paxos_entry_destroy(&p->msg_queue_after_persist[i].entries[j]);
                }
                free(p->msg_queue_after_persist[i].entries);
            }
        }
    }
    p->msg_queue_after_persist_len = 0;
}

// NEW: When the Host completes fsync of the snapshot chunk, it calls this to clear the block
// and inform the leader that it is ready for the next offset.
void paxos_snapshot_acked(paxos_t* p, bool success) {
    if (!p->pending_snapshot) return;

    p->pending_snapshot_chunk_ready = false;
    uint64_t next_offset = success ? p->pending_snapshot_offset + p->pending_snapshot_len : p->pending_snapshot_offset;

    if (!success) {
        p->expected_snapshot_offset = p->pending_snapshot_offset;
    }

    paxos_msg_t res = {
        .type = MSG_INSTALL_SNAPSHOT_RES,
        .to = p->pending_snapshot_from,
        .reject = !success,
        .slot = next_offset,
        .snapshot_done = p->pending_snapshot_done
    };

    if (success && p->pending_snapshot_done) {
        for (size_t i = 0; i < p->log_cap; i++) {
            if (p->log[i].has_value) {
                paxos_entry_destroy(&p->log[i].entry);
                p->log[i].has_value = false;
            }
        }
        p->log_base_slot = p->pending_snapshot_msg_slot + 1;
        p->snapshot_index = p->pending_snapshot_msg_slot;
        p->last_applied = p->snapshot_index;
        p->local_commit_index = p->snapshot_index;
        p->leader_commit_hint = p->snapshot_index;
        p->stable_accepted_through = p->snapshot_index;
    }

    paxos_send_immediate(p, res);

    if (p->pending_snapshot_data) free(p->pending_snapshot_data);
    p->pending_snapshot_data = NULL;
    p->pending_snapshot_len = 0;

    if (p->pending_snapshot_done || !success) {
        p->pending_snapshot = false;
        p->pending_snapshot_offset = 0;
        p->pending_snapshot_done = false;
    }
}

paxos_state_t paxos_state(paxos_t* p) { return p ? p->state : PAXOS_STATE_LEARNER; }
uint64_t paxos_promised_ballot(paxos_t* p) { return p ? p->promised_ballot : 0; }
uint64_t paxos_local_commit_index(paxos_t* p) { return p ? p->local_commit_index : 0; }
uint64_t paxos_snapshot_index(paxos_t* p) { return p ? p->snapshot_index : 0; }
bool paxos_has_fatal_error(paxos_t* p) { return p ? p->fatal_error : true; }
uint64_t paxos_last_slot(paxos_t* p) {
    if (!p) return 0;
    uint64_t highest = p->snapshot_index;
    for(size_t i = 0; i < p->log_cap; i++) {
        if (p->log[i].has_value && p->log[i].entry.slot > highest) highest = p->log[i].entry.slot;
    }
    return highest;
}

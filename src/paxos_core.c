// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

paxos_t* paxos_create(uint64_t id, uint64_t* peers, size_t num_peers) {
    if (id == 0 || num_peers > MAX_PEERS - 1) return NULL;
    paxos_t* p = calloc(1, sizeof(paxos_t));
    if (!p) return NULL;

    p->id = id;
    p->state = PAXOS_STATE_LEARNER;

    p->node_directory[0] = id;
    p->num_nodes = 1;
    p->active_config_mask = 1ULL << 0;
    p->joint_config_mask = 0;
    p->in_joint_consensus = false;

    for (size_t i = 0; i < num_peers; i++) p->active_config_mask |= paxos_peer_bit(p, peers[i]);
    p->base_config_mask = p->active_config_mask;

    p->log_chunks_cap = 16; // Initial chunk pointers
    p->log_chunks = calloc(p->log_chunks_cap, sizeof(paxos_log_chunk_t*));
    p->log_base_slot = 1;

    p->inflight = calloc(INFLIGHT_WINDOW, sizeof(paxos_inflight_slot_t));
    p->recovery_cap = 1024;
    p->recovery_buffer = calloc(p->recovery_cap, sizeof(paxos_recovery_slot_t));

    if (!p->log_chunks || !p->inflight || !p->recovery_buffer) {
        paxos_destroy(p);
        return NULL;
    }

    p->current_tick = 0;
    p->heartbeat_timeout = 10;
    p->election_timeout = 30;
    p->randomized_election_timeout = p->election_timeout + (rand() % p->election_timeout);
    return p;
}

paxos_t* paxos_restore(uint64_t id, uint64_t* peers, size_t num_peers,
                       paxos_hard_state_t hard_state, uint64_t local_commit_index, uint64_t snapshot_index,
                       paxos_entry_t* entries, size_t num_entries) {
    paxos_t* p = paxos_create(id, peers, num_peers);
    if (!p) return NULL;

    p->promised_ballot = hard_state.promised_ballot;
    p->max_generated_ballot = hard_state.max_generated_ballot;
    p->prev_hard_state = hard_state;

    p->snapshot_index = snapshot_index;
    p->local_commit_index = local_commit_index;
    p->leader_commit_hint = local_commit_index;
    p->last_applied = local_commit_index;
    p->stable_accepted_through = snapshot_index;
    p->log_base_slot = snapshot_index + 1;

    for (size_t i = 0; i < num_entries; i++) {
        paxos_entry_t* e = &entries[i];
        if (!paxos_log_accept(p, e->slot, e->accepted_ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len)) {
            paxos_destroy(p); return NULL;
        }
        uint64_t c_idx = e->slot / PAXOS_LOG_CHUNK_SIZE;
        uint64_t c_off = e->slot % PAXOS_LOG_CHUNK_SIZE;
        p->log_chunks[c_idx]->slots[c_off].unstable = false;
        p->log_chunks[c_idx]->slots[c_off].chosen = true;
    }
    return p;
}

void paxos_destroy(paxos_t* p) {
    if (!p) return;

    if (p->log_chunks) {
        for (size_t i = 0; i < p->log_chunks_cap; i++) {
            if (p->log_chunks[i]) {
                for (size_t j = 0; j < PAXOS_LOG_CHUNK_SIZE; j++) {
                    if (p->log_chunks[i]->slots[j].has_value) paxos_entry_destroy(&p->log_chunks[i]->slots[j].entry);
                }
                free(p->log_chunks[i]);
            }
        }
        free(p->log_chunks);
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
            if ((m->type == MSG_PROMISE || m->type == MSG_FETCH_ENTRIES_RES || m->type == MSG_ACCEPT) && m->entries) {
                for (size_t j = 0; j < m->num_entries; j++) paxos_entry_destroy(&m->entries[j]);
                free(m->entries);
            }
        }
        free(p->msg_queue_after_persist);
    }
    if (p->pending_snapshot_data) free(p->pending_snapshot_data);
    if (p->read_states) free(p->read_states);
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

static bool paxos_msg_is_valid(paxos_msg_t* msg) {
    if (msg->type == MSG_PROPOSE) {
        if (msg->num_entries == 0 || !msg->entries) return false;
        if (msg->entries[0].data_len > PAXOS_MAX_PAYLOAD_SIZE) return false;
        if (msg->entries[0].data_len > 0 && !msg->entries[0].data) return false;
        return true;
    }
    if (msg->ballot == 0 && msg->type != MSG_READ_BARRIER && msg->type != MSG_TICK && msg->type != MSG_NACK) return false;

    if (msg->type == MSG_PROMISE || msg->type == MSG_FETCH_ENTRIES_RES) {
        if (msg->num_entries > 0 && !msg->entries) return false;
        for (size_t i = 0; i < msg->num_entries; i++) {
            if (msg->entries[i].data_len > PAXOS_MAX_PAYLOAD_SIZE) return false;
            if (msg->entries[i].data_len > 0 && !msg->entries[i].data) return false;
        }
    }
    if (msg->type == MSG_ACCEPT) {
        if (msg->num_entries != 1 || !msg->entries) return false;
        if (msg->entries[0].data_len > PAXOS_MAX_PAYLOAD_SIZE) return false;
        if (msg->entries[0].data_len > 0 && !msg->entries[0].data) return false;
    }
    return true;
}

static bool paxos_is_valid_peer(paxos_t* p, uint64_t from_id) {
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] == from_id) {
            uint64_t mask = 1ULL << i;
            return (p->active_config_mask & mask) || (p->joint_config_mask & mask);
        }
    }
    return false;
}

void paxos_tick(paxos_t* p) {
    if (p->fatal_error) return;
    p->current_tick++;
    if (p->state == PAXOS_STATE_ACTIVE) {
        if (p->current_tick >= p->heartbeat_timeout) {
            p->current_tick = 0;
            uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
            for (size_t i = 0; i < p->num_nodes; i++) {
                if (p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
                    paxos_msg_t beat = { .type = MSG_TICK, .to = p->node_directory[i], .ballot = p->active_ballot, .commit_index = p->local_commit_index };
                    paxos_send_immediate(p, beat);
                }
            }
        }
    } else {
        if (p->current_tick >= p->randomized_election_timeout) {
            p->current_tick = 0;
            p->randomized_election_timeout = p->election_timeout + (rand() % p->election_timeout);
            extern void paxos_proposer_campaign(paxos_t* p);
            paxos_proposer_campaign(p);
        }
    }
}

void paxos_step_local(paxos_t* p, paxos_msg_t* msg) {
    if (p->fatal_error || !paxos_msg_is_valid(msg)) return;
    if (p->state != PAXOS_STATE_ACTIVE) return;
    if (p->active_ballot < p->promised_ballot) {
        p->state = PAXOS_STATE_LEARNER;
        p->leader_id = 0; return;
    }

    if (msg->type == MSG_PROPOSE) {
        paxos_entry_t* in_e = &msg->entries[0];
        if (p->next_slot - p->local_commit_index >= INFLIGHT_WINDOW) return;
        paxos_inflight_slot_t* target_inf = &p->inflight[p->next_slot % INFLIGHT_WINDOW];
        if (target_inf->active) return;

        for (uint64_t s = p->local_commit_index + 1; s < p->next_slot; s++) {
            paxos_entry_t* inflight_e = paxos_log_get(p, s);
            if (inflight_e) {
                if (inflight_e->type == ENTRY_CONF_JOINT || inflight_e->type == ENTRY_CONF_FINAL || inflight_e->type == ENTRY_CONF_ADD || inflight_e->type == ENTRY_CONF_REMOVE) return;
                if (in_e->client_id != 0 && inflight_e->client_id == in_e->client_id && inflight_e->client_seq >= in_e->client_seq) return;
            }
        }

        uint64_t slot = p->next_slot++;
        if (!paxos_log_accept(p, slot, p->active_ballot, in_e->type, in_e->client_id, in_e->client_seq, in_e->data, in_e->data_len)) return;

        paxos_inflight_slot_t* inf = &p->inflight[slot % INFLIGHT_WINDOW];
        inf->slot = slot;
        inf->ballot = p->active_ballot;
        inf->ack_mask = paxos_peer_bit(p, p->id);
        inf->chosen = false;
        inf->active = true;

        if (paxos_has_quorum(p, inf->ack_mask)) {
            inf->chosen = true;
            uint64_t c_idx = slot / PAXOS_LOG_CHUNK_SIZE;
            uint64_t c_off = slot % PAXOS_LOG_CHUNK_SIZE;
            p->log_chunks[c_idx]->slots[c_off].chosen = true;
            p->local_commit_index = slot;
            p->leader_commit_hint = slot;
        }

        uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
        for (size_t i = 0; i < p->num_nodes; i++) {
            if (p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
                paxos_entry_t* safe_copy = malloc(sizeof(paxos_entry_t));
                if (!safe_copy || !paxos_entry_clone(safe_copy, paxos_log_get(p, slot))) {
                    if (safe_copy) free(safe_copy);
                    p->fatal_error = true; return;
                }

                paxos_msg_t acc = { .type = MSG_ACCEPT, .ballot = p->active_ballot, .slot = slot, .commit_index = p->local_commit_index, .entries = safe_copy, .num_entries = 1 };
                acc.to = p->node_directory[i];
                paxos_send_after_persist(p, acc);
            }
        }
    } else if (msg->type == MSG_READ_BARRIER) {
        extern void paxos_proposer_read_barrier_local(paxos_t* p, paxos_msg_t* msg);
        paxos_proposer_read_barrier_local(p, msg);
    }
}

void paxos_step_remote(paxos_t* p, paxos_msg_t* msg) {
    if (p->fatal_error || msg->to != p->id || !paxos_msg_is_valid(msg)) return;
    if (!paxos_is_valid_peer(p, msg->from)) return;
    switch (msg->type) {
        case MSG_PREPARE: case MSG_ACCEPT: case MSG_COMMIT_NOTICE: case MSG_FETCH_ENTRIES_RES: case MSG_INSTALL_SNAPSHOT: case MSG_READ_BARRIER: case MSG_TICK:
            paxos_acceptor_step(p, msg); break;
        case MSG_PROMISE: case MSG_ACCEPTED: case MSG_NACK: case MSG_FETCH_ENTRIES: case MSG_INSTALL_SNAPSHOT_RES: case MSG_READ_BARRIER_RES:
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

    ready.entries_to_save = paxos_log_extract_unstable(p, &ready.num_entries_to_save);
    ready.messages_immediate = p->msg_queue_immediate;
    ready.num_messages_immediate = p->msg_queue_immediate_len;
    ready.messages_after_persist = p->msg_queue_after_persist;
    ready.num_messages_after_persist = p->msg_queue_after_persist_len;
    ready.read_states = p->read_states;
    ready.num_read_states = p->num_read_states;

    if (p->local_commit_index > p->last_applied) {
        size_t apply_count = p->local_commit_index - p->last_applied;
        ready.chosen_entries = calloc(apply_count, sizeof(paxos_entry_t));
        if (ready.chosen_entries) {
            size_t valid = 0;
            for (uint64_t i = p->last_applied + 1; i <= p->local_commit_index; i++) {
                uint64_t c_idx = i / PAXOS_LOG_CHUNK_SIZE;
                uint64_t c_off = i % PAXOS_LOG_CHUNK_SIZE;

                if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx]) break;
                paxos_log_slot_t* slot_data = &p->log_chunks[c_idx]->slots[c_off];
                if (!slot_data->has_value || !slot_data->chosen) break;

                if (paxos_entry_clone(&ready.chosen_entries[valid], &slot_data->entry)) {
                    valid++;
                } else {
                    p->fatal_error = true; break;
                }
            }
            ready.num_chosen_entries = valid;
        }
    }

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

    for (size_t c = 0; c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            if (p->log_chunks[c]->slots[o].has_value && p->log_chunks[c]->slots[o].entry.slot <= p->stable_accepted_through) {
                p->log_chunks[c]->slots[o].unstable = false;
            }
        }
    }

    p->prev_hard_state.promised_ballot = p->promised_ballot;
    p->prev_hard_state.max_generated_ballot = p->max_generated_ballot;
    p->msg_queue_immediate_len = 0;
    p->num_read_states = 0;

    if (p->msg_queue_after_persist) {
        for (size_t i = 0; i < p->msg_queue_after_persist_len; i++) {
            if ((p->msg_queue_after_persist[i].type == MSG_PROMISE ||
                 p->msg_queue_after_persist[i].type == MSG_FETCH_ENTRIES_RES ||
                 p->msg_queue_after_persist[i].type == MSG_ACCEPT) &&
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

void paxos_snapshot_acked(paxos_t* p, bool success) {
    if (!p->pending_snapshot) return;

    p->pending_snapshot_chunk_ready = false;
    uint64_t next_offset = success ? p->pending_snapshot_offset + p->pending_snapshot_len : p->pending_snapshot_offset;
    if (!success) p->expected_snapshot_offset = p->pending_snapshot_offset;

    paxos_msg_t res = {
        .type = MSG_INSTALL_SNAPSHOT_RES,
        .to = p->pending_snapshot_from,
        .ballot = p->pending_snapshot_msg_ballot,
        .reject = !success,
        .slot = next_offset,
        .snapshot_done = p->pending_snapshot_done
    };

    if (success && p->pending_snapshot_done) {
        for (size_t c = 0; c < p->log_chunks_cap; c++) {
            if (!p->log_chunks[c]) continue;
            for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
                if (p->log_chunks[c]->slots[o].has_value) {
                    paxos_entry_destroy(&p->log_chunks[c]->slots[o].entry);
                    p->log_chunks[c]->slots[o].has_value = false;
                }
            }
            free(p->log_chunks[c]);
            p->log_chunks[c] = NULL;
        }
        p->log_base_slot = p->pending_snapshot_msg_slot + 1;
        p->snapshot_index = p->pending_snapshot_msg_slot;
        p->last_applied = p->snapshot_index;
        p->local_commit_index = p->snapshot_index;
        p->leader_commit_hint = p->snapshot_index;
        paxos_rebuild_config(p);
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
    for (size_t c = 0; c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            if (p->log_chunks[c]->slots[o].has_value && p->log_chunks[c]->slots[o].entry.slot > highest) {
                highest = p->log_chunks[c]->slots[o].entry.slot;
            }
        }
    }
    return highest;
}

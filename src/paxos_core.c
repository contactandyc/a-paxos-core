// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"
#include <stdlib.h>
#include <string.h>

// --- DETERMINISTIC PRNG ---

static inline uint32_t paxos_rand(paxos_t* p) {
    p->prng.state = (p->prng.state * 1103515245 + 12345);
    return p->prng.state;
}

static void process_auto_proposals(paxos_t* p);

// --- PEER METADATA TRACKING ---

static paxos_peer_state_t* get_or_create_peer(paxos_t *p, uint64_t node_id) {
    for (size_t i = 0; i < p->num_peer_states; i++) {
        if (p->peer_states[i].node_id == node_id) {
            return &p->peer_states[i];
        }
    }

    if (p->num_peer_states == p->peer_states_cap) {
        p->peer_states_cap = p->peer_states_cap == 0 ? 8 : p->peer_states_cap * 2;
        paxos_peer_state_t *new_buf = realloc(p->peer_states, p->peer_states_cap * sizeof(paxos_peer_state_t));
        if (!new_buf) { p->fatal_error = true; return NULL; }
        p->peer_states = new_buf;
    }

    paxos_peer_state_t *peer = &p->peer_states[p->num_peer_states++];
    peer->node_id = node_id;
    peer->promised_ballot = 0;
    peer->local_commit_index = 0;
    peer->is_active = true;
    peer->last_active_tick = p->current_tick;
    return peer;
}

static void paxos_update_peer_metadata(paxos_t *p, const paxos_msg_t *msg) {
    paxos_peer_state_t *peer = get_or_create_peer(p, msg->from);
    if (!peer) return;

    peer->is_active = true;
    peer->last_active_tick = p->current_tick;

    if (msg->ballot > peer->promised_ballot) {
        peer->promised_ballot = msg->ballot;
    }

    if (msg->commit_index > peer->local_commit_index) {
        peer->local_commit_index = msg->commit_index;
    }

    // THE CATCH-UP TRIGGER:
    // If a peer is vastly ahead of us (buffer of 10 to absorb micro-jitter), flag it!
    if (msg->commit_index > p->local_commit_index + 10) {
        p->needs_catchup = true;

        // Push the target index higher if multiple peers are ahead
        if (msg->commit_index > p->catchup_target_index) {
            p->catchup_target_index = msg->commit_index;
        }
    }
}

// --- LIFECYCLE ---

paxos_err_t paxos_register_learner(paxos_t* p, uint64_t node_id) {
    if (!p || node_id == 0) return PAXOS_ERR_INVALID_ARG;
    if (p->num_nodes >= PAXOS_MAX_PEERS) return PAXOS_ERR_NOMEM;
    paxos_peer_register(p, node_id);
    return PAXOS_OK;
}

paxos_err_t paxos_create(const paxos_config_t* cfg, paxos_t** out) {
    if (!cfg || !out || cfg->node_id == 0 || cfg->num_initial_voters > PAXOS_MAX_PEERS) {
        return PAXOS_ERR_INVALID_ARG;
    }

    paxos_t* p = calloc(1, sizeof(paxos_t));
    if (!p) return PAXOS_ERR_NOMEM;

    p->id = cfg->node_id;
    p->state = PAXOS_STATE_LEARNER;
    p->max_payload_size = cfg->max_payload_size > 0 ? cfg->max_payload_size : PAXOS_MAX_PAYLOAD_SIZE;
    p->max_batch_bytes = cfg->max_batch_bytes > 0 ? cfg->max_batch_bytes : 4194304;

    p->num_nodes = 0;
    p->allocated_peer_indices = 0;
    p->active_config_mask = 0;
    p->joint_config_mask = 0;
    p->in_joint_consensus = false;
    p->pending_reconfig = false;
    p->needs_conf_final = false;

    p->active_config_mask |= paxos_peer_register(p, p->id);
    for (size_t i = 0; i < cfg->num_initial_voters; i++) {
        p->active_config_mask |= paxos_peer_register(p, cfg->initial_voters[i]);
    }
    p->base_config_mask = p->active_config_mask;

    for (size_t i = 0; i < PAXOS_MAX_PEERS; i++) {
        if (p->node_directory[i] != 0) {
            p->learner_state[i].eligible_to_vote = true;
            p->learner_state[i].snapshot_installed = true;
            p->learner_state[i].hard_state_initialized = true;
            p->learner_state[i].caught_up_through = 0;
        }
    }

    p->log_chunks_cap = 16;
    p->log_chunks = calloc(p->log_chunks_cap, sizeof(paxos_log_chunk_t*));
    p->log_base_slot = 1;
    p->highest_slot = 0;

    p->unstable_slots_cap = 1024;
    p->unstable_slots = malloc(p->unstable_slots_cap * sizeof(uint64_t));

    p->inflight = calloc(PAXOS_INTERNAL_INFLIGHT_WINDOW, sizeof(paxos_inflight_slot_t));
    p->recovery_cap = 1024;
    p->recovery_buffer = calloc(p->recovery_cap, sizeof(paxos_recovery_slot_t));

    p->peer_states_cap = 16;
    p->peer_states = calloc(p->peer_states_cap, sizeof(paxos_peer_state_t));

    if (!p->log_chunks || !p->unstable_slots || !p->inflight || !p->recovery_buffer || !p->peer_states) {
        paxos_destroy(p);
        return PAXOS_ERR_NOMEM;
    }

    p->current_tick = 0;
    p->ticks_since_last_fetch = 100;
    p->heartbeat_timeout = cfg->heartbeat_ticks > 0 ? cfg->heartbeat_ticks : 10;
    p->election_timeout = cfg->election_ticks > 0 ? cfg->election_ticks : 30;

    // Initialize Deterministic PRNG Seed
    p->prng.state = (uint32_t)(p->id ^ 0x9A7B3C1D);
    p->randomized_election_timeout = p->election_timeout + (paxos_rand(p) % p->election_timeout);

    *out = p;
    return PAXOS_OK;
}

paxos_err_t paxos_restore(const paxos_config_t* cfg, const paxos_restore_data_t* restore, paxos_t** out) {
    if (!cfg || !restore || !out) return PAXOS_ERR_INVALID_ARG;

    paxos_err_t err = paxos_create(cfg, out);
    if (err != PAXOS_OK) return err;

    paxos_t* p = *out;

    p->promised_ballot = restore->hard_state.promised_ballot;
    p->max_generated_ballot = restore->hard_state.max_generated_ballot;
    p->prev_promised_ballot = p->promised_ballot;
    p->prev_max_generated_ballot = p->max_generated_ballot;
    p->prev_pending_reconfig = restore->hard_state.pending_reconfig;

    if (restore->hard_state.num_active_nodes > 0) {
        p->active_config_mask = 0;
        p->joint_config_mask = 0;

        for (size_t i = 0; i < restore->hard_state.num_active_nodes; i++) {
            p->active_config_mask |= paxos_peer_register(p, restore->hard_state.active_nodes[i]);
        }
        for (size_t i = 0; i < restore->hard_state.num_joint_nodes; i++) {
            p->joint_config_mask |= paxos_peer_register(p, restore->hard_state.joint_nodes[i]);
        }

        p->pending_reconfig = restore->hard_state.pending_reconfig;
        p->in_joint_consensus = p->pending_reconfig && (p->joint_config_mask != 0);
        p->base_config_mask = p->active_config_mask;
    }

    p->prev_active_config_mask = p->active_config_mask;
    p->prev_joint_config_mask = p->joint_config_mask;

    p->snapshot_index = restore->snapshot_index;
    p->local_commit_index = restore->local_commit_index;
    p->leader_commit_hint = restore->local_commit_index;
    p->last_applied = restore->local_commit_index;
    p->stable_accepted_through = restore->snapshot_index;
    p->log_base_slot = restore->snapshot_index + 1;
    p->highest_slot = restore->snapshot_index;

    for (size_t i = 0; i < restore->num_entries; i++) {
        const paxos_entry_t* e = &restore->entries[i].entry;

#if !PAXOS_ENABLE_RECONFIG
        if (e->type >= PAXOS_ENTRY_CONF_ADD && e->type <= PAXOS_ENTRY_CONF_FINAL) continue;
#endif

        if (restore->entries[i].chosen) {
            paxos_log_learn_chosen(p, e->slot, e);
            uint64_t c_idx = paxos_chunk_idx(p, e->slot);
            uint64_t c_off = paxos_chunk_off(e->slot);
            p->log_chunks[c_idx]->slots[c_off].unstable = false;
        } else {
            paxos_log_accept(p, e->slot, e->accepted_ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len);
            uint64_t c_idx = paxos_chunk_idx(p, e->slot);
            uint64_t c_off = paxos_chunk_off(e->slot);
            p->log_chunks[c_idx]->slots[c_off].unstable = false;
        }
    }

    paxos_rebuild_config(p);
    p->num_unstable_slots = 0;

    uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
    for (size_t i = 0; i < PAXOS_MAX_PEERS; i++) {
        if (p->node_directory[i] != 0 && ((1ULL << i) & combined_mask)) {
            p->learner_state[i].eligible_to_vote = true;
            p->learner_state[i].snapshot_installed = true;
            p->learner_state[i].hard_state_initialized = true;
            p->learner_state[i].caught_up_through = p->local_commit_index;
        }
    }

    return PAXOS_OK;
}

void paxos_destroy(paxos_t* p) {
    if (!p) return;
    if (p->log_chunks) {
        for (size_t i = 0; i < p->log_chunks_cap; i++) {
            if (p->log_chunks[i]) {
                for (size_t j = 0; j < PAXOS_INTERNAL_LOG_CHUNK_SIZE; j++) {
                    if (p->log_chunks[i]->slots[j].has_accepted) paxos_entry_destroy(&p->log_chunks[i]->slots[j].accepted_entry);
                    if (p->log_chunks[i]->slots[j].is_chosen) paxos_entry_destroy(&p->log_chunks[i]->slots[j].chosen_entry);
                }
                free(p->log_chunks[i]);
            }
        }
        free(p->log_chunks);
    }
    if (p->unstable_slots) free(p->unstable_slots);
    if (p->inflight) free(p->inflight);
    if (p->recovery_buffer) {
        for(size_t i = 0; i < p->recovery_cap; i++) paxos_entry_destroy(&p->recovery_buffer[i].recovered_value);
        free(p->recovery_buffer);
    }

    if (p->msg_queue_immediate) {
        for (size_t i = 0; i < p->msg_queue_immediate_len; i++) {
            if (p->msg_queue_immediate[i].type == PAXOS_MSG_INSTALL_SNAPSHOT && p->msg_queue_immediate[i].snapshot_data) {
                free(p->msg_queue_immediate[i].snapshot_data);
            }
        }
        free(p->msg_queue_immediate);
    }

    if (p->msg_queue_after_persist) {
        for (size_t i = 0; i < p->msg_queue_after_persist_len; i++) {
            paxos_msg_t* m = &p->msg_queue_after_persist[i];
            if ((m->type == PAXOS_MSG_PROMISE || m->type == PAXOS_MSG_FETCH_ENTRIES_RES || m->type == PAXOS_MSG_ACCEPT) && m->entries) {
                for (size_t j = 0; j < m->num_entries; j++) paxos_entry_destroy(&m->entries[j]);
                free(m->entries);
            } else if (m->type == PAXOS_MSG_INSTALL_SNAPSHOT && m->snapshot_data) {
                free(m->snapshot_data);
            }
        }
        free(p->msg_queue_after_persist);
    }

    if (p->peer_states) free(p->peer_states);
    if (p->pending_snapshot_data) free(p->pending_snapshot_data);
    if (p->read_states) free(p->read_states);
    free(p);
}

// --- QUEUEING HELPERS ---

static bool paxos_msg_queue_push(paxos_t* p, paxos_msg_t** queue, size_t* len, size_t* cap, paxos_msg_t msg) {
    if (*len >= *cap) {
        size_t new_cap = *cap == 0 ? 16 : *cap * 2;
        paxos_msg_t* new_q = realloc(*queue, new_cap * sizeof(paxos_msg_t));
        if (!new_q) { p->fatal_error = true; return false; }
        *queue = new_q;
        *cap = new_cap;
    }
    msg.from = p->id;
    (*queue)[(*len)++] = msg;
    return true;
}

void paxos_send_immediate(paxos_t* p, paxos_msg_t msg) {
    paxos_msg_queue_push(p, &p->msg_queue_immediate, &p->msg_queue_immediate_len, &p->msg_queue_immediate_cap, msg);
}

void paxos_send_after_persist(paxos_t* p, paxos_msg_t msg) {
    paxos_msg_queue_push(p, &p->msg_queue_after_persist, &p->msg_queue_after_persist_len, &p->msg_queue_after_persist_cap, msg);
}

// --- CORE TICK ---

paxos_err_t paxos_tick(paxos_t* p) {
    if (p->fatal_error) return PAXOS_ERR_NOT_ACTIVE;
    p->current_tick++;
    p->ticks_since_last_fetch++;

    if (p->state == PAXOS_STATE_ACTIVE) {
        if (p->current_tick >= p->heartbeat_timeout) {
            p->current_tick = 0;

            paxos_entry_t* c_entry = paxos_log_get(p, p->local_commit_index);
            uint64_t hash = c_entry ? paxos_entry_hash(c_entry) : 0;

            uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
            for (size_t i = 0; i < PAXOS_MAX_PEERS; i++) {
                if (p->node_directory[i] != 0 && p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
                    paxos_msg_t beat = {
                        .type = PAXOS_MSG_HEARTBEAT, .to = p->node_directory[i],
                        .ballot = p->active_ballot, .commit_index = p->local_commit_index,
                        .value_hash = hash
                    };
                    paxos_send_immediate(p, beat);
                }
            }
        }
    } else {
        if (p->current_tick >= p->randomized_election_timeout) {
            p->current_tick = 0;
            p->randomized_election_timeout = p->election_timeout + (paxos_rand(p) % p->election_timeout);
            extern void paxos_proposer_campaign(paxos_t* p);
            paxos_proposer_campaign(p);
        }
    }
    return PAXOS_OK;
}

// --- RECEIVE & VALIDATION ---

static bool paxos_is_valid_peer(paxos_t* p, uint64_t from_id) {
    for (size_t i = 0; i < PAXOS_MAX_PEERS; i++) {
        if (p->node_directory[i] == from_id) return true;
    }
    return false;
}

static bool validate_entry(const paxos_entry_t* e) {
    if (e->data_len > PAXOS_MAX_PAYLOAD_SIZE) return false;
    if (e->data_len > 0 && !e->data) return false;

    if ((e->type == PAXOS_ENTRY_CONF_ADD || e->type == PAXOS_ENTRY_CONF_REMOVE) && e->data_len != sizeof(uint64_t)) return false;

    if (e->type == PAXOS_ENTRY_CONF_JOINT) {
        if (e->data_len == 0 || e->data_len % sizeof(uint64_t) != 0) return false;
        size_t count = e->data_len / sizeof(uint64_t);
        if (count > PAXOS_MAX_PEERS) return false;

        uint64_t* nodes = (uint64_t*)e->data;
        for (size_t i = 0; i < count; i++) {
            if (nodes[i] == 0) return false;
            for (size_t j = i + 1; j < count; j++) {
                if (nodes[i] == nodes[j]) return false;
            }
        }
    }

    if (e->type == PAXOS_ENTRY_CONF_FINAL && e->data_len != 0) return false;

    return true;
}

paxos_err_t paxos_receive(paxos_t* p, const paxos_msg_t* msg) {
    if (p->fatal_error || !msg || msg->to != p->id) return PAXOS_ERR_INVALID_ARG;
    if (!paxos_is_valid_peer(p, msg->from)) return PAXOS_ERR_INVALID_ARG;

    paxos_msg_t internal_msg = *msg;

    if (internal_msg.num_entries > 0 && internal_msg.entries) {
        for (size_t i = 0; i < internal_msg.num_entries; i++) {
            if (!validate_entry(&internal_msg.entries[i])) return PAXOS_ERR_INVALID_ARG;
        }
    }

    // Update our internal map of who has what data for auto catch-up
    paxos_update_peer_metadata(p, &internal_msg);

    switch (internal_msg.type) {
        case PAXOS_MSG_PREPARE: case PAXOS_MSG_ACCEPT: case PAXOS_MSG_COMMIT_NOTICE:
        case PAXOS_MSG_FETCH_ENTRIES_RES: case PAXOS_MSG_INSTALL_SNAPSHOT:
        case PAXOS_MSG_READ_BARRIER: case PAXOS_MSG_HEARTBEAT:
            paxos_acceptor_step(p, &internal_msg); break;
        case PAXOS_MSG_PROMISE: case PAXOS_MSG_ACCEPTED: case PAXOS_MSG_NACK:
        case PAXOS_MSG_FETCH_ENTRIES: case PAXOS_MSG_INSTALL_SNAPSHOT_RES:
        case PAXOS_MSG_READ_BARRIER_RES: case PAXOS_MSG_PROMOTE_REQUEST:
            paxos_proposer_step(p, &internal_msg); break;
        default: break;
    }

    process_auto_proposals(p);
    return PAXOS_OK;
}

static void process_auto_proposals(paxos_t* p) {
    if (p->needs_conf_final && p->state == PAXOS_STATE_ACTIVE && p->id == p->leader_id) {
        p->needs_conf_final = false;
        uint64_t slot = p->next_slot++;
        if (paxos_log_accept(p, slot, p->active_ballot, PAXOS_ENTRY_CONF_FINAL, 0, 0, NULL, 0)) {
            paxos_inflight_slot_t* inf = &p->inflight[slot % PAXOS_INTERNAL_INFLIGHT_WINDOW];
            inf->slot = slot;
            inf->ballot = p->active_ballot;
            inf->ack_mask = paxos_peer_bit(p, p->id);
            inf->chosen = false;
            inf->active = true;

            if (paxos_has_quorum(p, inf->ack_mask)) {
                inf->chosen = true;
                paxos_log_learn_chosen(p, slot, paxos_log_get_accepted(p, slot));
                p->local_commit_index = slot;
                p->leader_commit_hint = slot;
                inf->active = false;
                paxos_rebuild_config(p);
            }
        }
    }
}

// --- READY AND ADVANCE ---

paxos_err_t paxos_get_ready(paxos_t* p, paxos_ready_t* ready) {
    if (p->fatal_error || !ready) return PAXOS_ERR_INVALID_ARG;
    memset(ready, 0, sizeof(paxos_ready_t));

    paxos_hard_state_t* hs = &ready->hard_state;
    hs->promised_ballot = p->promised_ballot;
    hs->max_generated_ballot = p->max_generated_ballot;
    hs->pending_reconfig = p->pending_reconfig;
    hs->num_active_nodes = 0;
    hs->num_joint_nodes = 0;

    for (int i = 0; i < 128; i++) {
        if (p->peer_map[i].active) {
            uint64_t node_id = p->peer_map[i].id;
            uint8_t index = p->peer_map[i].index;
            if ((1ULL << index) & p->active_config_mask) hs->active_nodes[hs->num_active_nodes++] = node_id;
            if ((1ULL << index) & p->joint_config_mask) hs->joint_nodes[hs->num_joint_nodes++] = node_id;
        }
    }

    hs->has_update = (p->promised_ballot != p->prev_promised_ballot) ||
                     (p->max_generated_ballot != p->prev_max_generated_ballot) ||
                     (p->active_config_mask != p->prev_active_config_mask) ||
                     (p->joint_config_mask != p->prev_joint_config_mask);

    ready->entries_to_save = paxos_log_extract_unstable(p, &ready->num_entries_to_save);
    ready->messages_immediate = p->msg_queue_immediate;
    ready->num_messages_immediate = p->msg_queue_immediate_len;
    ready->messages_after_persist = p->msg_queue_after_persist;
    ready->num_messages_after_persist = p->msg_queue_after_persist_len;
    ready->read_states = p->read_states;
    ready->num_read_states = p->num_read_states;

    if (p->local_commit_index > p->last_applied) {
        size_t apply_count = p->local_commit_index - p->last_applied;
        ready->chosen_entries = calloc(apply_count, sizeof(paxos_entry_t));
        if (ready->chosen_entries) {
            size_t valid = 0;
            for (uint64_t i = p->last_applied + 1; i <= p->local_commit_index; i++) {
                uint64_t c_idx = paxos_chunk_idx(p, i);
                uint64_t c_off = paxos_chunk_off(i);

                if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx]) break;
                paxos_log_slot_t* slot_data = &p->log_chunks[c_idx]->slots[c_off];

                if (!slot_data->is_chosen) break;

                if (paxos_entry_clone_retain(&ready->chosen_entries[valid], &slot_data->chosen_entry)) {
                    valid++;
                } else {
                    p->fatal_error = true; break;
                }
            }
            ready->num_chosen_entries = valid;
        } else {
            p->fatal_error = true;
        }
    }

    ready->install_snapshot = p->pending_snapshot_chunk_ready;
    ready->snapshot_slot = p->pending_snapshot_msg_slot;
    ready->snapshot_ballot = p->pending_snapshot_msg_ballot;
    ready->snapshot_data = p->pending_snapshot_data;
    ready->snapshot_len = p->pending_snapshot_len;
    ready->snapshot_offset = p->pending_snapshot_offset;
    ready->snapshot_done = p->pending_snapshot_done;

    // Hand off the catch-up trigger to the Daemon
    ready->needs_catchup = p->needs_catchup;
    ready->catchup_target_index = p->catchup_target_index;

    return PAXOS_OK;
}

static void paxos_msg_destroy_payload(paxos_msg_t* m) {
    if ((m->type == PAXOS_MSG_PROMISE || m->type == PAXOS_MSG_FETCH_ENTRIES_RES || m->type == PAXOS_MSG_ACCEPT) && m->entries) {
        for (size_t i = 0; i < m->num_entries; i++) paxos_entry_destroy(&m->entries[i]);
        free(m->entries);
        m->entries = NULL;
        m->num_entries = 0;
    }
    if (m->type == PAXOS_MSG_INSTALL_SNAPSHOT && m->snapshot_data) {
        free(m->snapshot_data);
        m->snapshot_data = NULL;
        m->snapshot_len = 0;
    }
}

void paxos_advance(paxos_t* p, const uint64_t* stable_slots, size_t num_stable_slots, uint64_t applied_slot) {
    if (p->fatal_error) return;

    if (applied_slot > p->last_applied) p->last_applied = applied_slot;

    for (size_t i = 0; i < num_stable_slots; i++) {
        uint64_t s = stable_slots[i];
        if (s < p->log_base_slot) continue;
        uint64_t c_idx = paxos_chunk_idx(p, s);
        uint64_t c_off = paxos_chunk_off(s);
        if (c_idx < p->log_chunks_cap && p->log_chunks[c_idx]) {
            p->log_chunks[c_idx]->slots[c_off].unstable = false;
            if (s > p->stable_accepted_through) p->stable_accepted_through = s;
        }
    }

    size_t kept = 0;
    for (size_t i = 0; i < p->num_unstable_slots; i++) {
        uint64_t s = p->unstable_slots[i];
        if (s < p->log_base_slot) continue;
        uint64_t c_idx = paxos_chunk_idx(p, s);
        uint64_t c_off = paxos_chunk_off(s);
        if (c_idx < p->log_chunks_cap && p->log_chunks[c_idx] && p->log_chunks[c_idx]->slots[c_off].unstable) {
            p->unstable_slots[kept++] = s;
        }
    }
    p->num_unstable_slots = kept;

    p->prev_promised_ballot = p->promised_ballot;
    p->prev_max_generated_ballot = p->max_generated_ballot;
    p->prev_active_config_mask = p->active_config_mask;
    p->prev_joint_config_mask = p->joint_config_mask;
    p->prev_pending_reconfig = p->pending_reconfig;

    for (size_t i = 0; i < p->msg_queue_immediate_len; i++) paxos_msg_destroy_payload(&p->msg_queue_immediate[i]);
    p->msg_queue_immediate_len = 0;
    p->num_read_states = 0;

    for (size_t i = 0; i < p->msg_queue_after_persist_len; i++) paxos_msg_destroy_payload(&p->msg_queue_after_persist[i]);
    p->msg_queue_after_persist_len = 0;

    // Reset catch-up triggers so the daemon doesn't fire continuously
    p->needs_catchup = false;
    p->catchup_target_index = 0;
}

// --- PUBLIC GETTERS ---

paxos_state_t paxos_state(paxos_t* p) { return p ? p->state : PAXOS_STATE_LEARNER; }
uint64_t paxos_promised_ballot(paxos_t* p) { return p ? p->promised_ballot : 0; }
uint64_t paxos_local_commit_index(paxos_t* p) { return p ? p->local_commit_index : 0; }
uint64_t paxos_snapshot_index(paxos_t* p) { return p ? p->snapshot_index : 0; }
bool paxos_has_fatal_error(paxos_t* p) { return p ? p->fatal_error : true; }
uint64_t paxos_last_slot(paxos_t* p) { return p ? p->highest_slot : 0; }

// --- PUBLIC METADATA API ---

size_t paxos_get_peers(paxos_t *p, uint64_t **out_node_ids) {
    if (!p || p->num_peer_states == 0) {
        *out_node_ids = NULL;
        return 0;
    }
    *out_node_ids = malloc(p->num_peer_states * sizeof(uint64_t));
    for (size_t i = 0; i < p->num_peer_states; i++) {
        (*out_node_ids)[i] = p->peer_states[i].node_id;
    }
    return p->num_peer_states;
}

uint64_t paxos_peer_commit_index(paxos_t *p, uint64_t node_id) {
    if (!p) return 0;
    for (size_t i = 0; i < p->num_peer_states; i++) {
        if (p->peer_states[i].node_id == node_id) {
            return p->peer_states[i].local_commit_index;
        }
    }
    return 0;
}

bool paxos_peer_is_leader(paxos_t *p, uint64_t node_id) {
    if (!p) return false;
    return (p->leader_id == node_id);
}

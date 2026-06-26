// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

// FAANG: Explicit Learner Registration API
void paxos_register_learner(paxos_t* p, uint64_t node_id) {
    if (!p || p->num_nodes >= MAX_PEERS) return;
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] == node_id) return;
    }
    p->node_directory[p->num_nodes++] = node_id;
}

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
    p->pending_reconfig = false;
    p->needs_conf_final = false;

    for (size_t i = 0; i < num_peers; i++) p->active_config_mask |= paxos_peer_bit(p, peers[i]);
    p->base_config_mask = p->active_config_mask;

    p->log_chunks_cap = 16;
    p->log_chunks = calloc(p->log_chunks_cap, sizeof(paxos_log_chunk_t*));
    p->log_base_slot = 1;
    p->highest_slot = 0;

    p->unstable_slots_cap = 1024;
    p->unstable_slots = malloc(p->unstable_slots_cap * sizeof(uint64_t));

    p->inflight = calloc(INFLIGHT_WINDOW, sizeof(paxos_inflight_slot_t));
    p->recovery_cap = 1024;
    p->recovery_buffer = calloc(p->recovery_cap, sizeof(paxos_recovery_slot_t));

    if (!p->log_chunks || !p->unstable_slots || !p->inflight || !p->recovery_buffer) {
        paxos_destroy(p);
        return NULL;
    }

    p->current_tick = 0;
    p->ticks_since_last_fetch = 100;
    p->heartbeat_timeout = 10;
    p->election_timeout = 30;
    p->randomized_election_timeout = p->election_timeout + (rand() % p->election_timeout);
    return p;
}

paxos_t* paxos_restore(uint64_t id, uint64_t* peers, size_t num_peers,
                       paxos_hard_state_t hard_state, uint64_t local_commit_index, uint64_t snapshot_index,
                       paxos_restored_entry_t* entries, size_t num_entries) {
    paxos_t* p = paxos_create(id, peers, num_peers);
    if (!p) return NULL;

    p->promised_ballot = hard_state.promised_ballot;
    p->max_generated_ballot = hard_state.max_generated_ballot;
    p->prev_hard_state = hard_state;

    // FAANG: Restore Durable Configuration from Hard State!
    if (hard_state.active_config_mask != 0) {
        p->active_config_mask = hard_state.active_config_mask;
        p->joint_config_mask = hard_state.joint_config_mask;
        p->pending_reconfig = hard_state.pending_reconfig;
        p->in_joint_consensus = (p->joint_config_mask != 0);
        p->base_config_mask = p->in_joint_consensus ? p->joint_config_mask : p->active_config_mask;
    }

    p->snapshot_index = snapshot_index;
    p->local_commit_index = local_commit_index;
    p->leader_commit_hint = local_commit_index;
    p->last_applied = local_commit_index;
    p->stable_accepted_through = snapshot_index;
    p->log_base_slot = snapshot_index + 1;
    p->highest_slot = snapshot_index;

    for (size_t i = 0; i < num_entries; i++) {
        paxos_entry_t* e = &entries[i].entry;

#if !PAXOS_ENABLE_RECONFIG
        if (e->type >= ENTRY_CONF_ADD && e->type <= ENTRY_CONF_FINAL) continue;
#endif

        if (!paxos_log_accept(p, e->slot, e->accepted_ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len)) {
            paxos_destroy(p); return NULL;
        }
        uint64_t c_idx = paxos_chunk_idx(p, e->slot);
        uint64_t c_off = paxos_chunk_off(e->slot);
        p->log_chunks[c_idx]->slots[c_off].unstable = false;
        p->log_chunks[c_idx]->slots[c_off].chosen = entries[i].chosen;
    }

    paxos_rebuild_config(p);

    // Clear unstable vector since restored slots are already persisted on disk
    p->num_unstable_slots = 0;

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
    if (p->unstable_slots) free(p->unstable_slots);
    if (p->inflight) free(p->inflight);
    if (p->recovery_buffer) {
        for(size_t i = 0; i < p->recovery_cap; i++) paxos_entry_destroy(&p->recovery_buffer[i].recovered_value);
        free(p->recovery_buffer);
    }

    if (p->msg_queue_immediate) {
        for (size_t i = 0; i < p->msg_queue_immediate_len; i++) {
            if (p->msg_queue_immediate[i].type == MSG_INSTALL_SNAPSHOT && p->msg_queue_immediate[i].snapshot_data) {
                free(p->msg_queue_immediate[i].snapshot_data);
            }
        }
        free(p->msg_queue_immediate);
    }

    if (p->msg_queue_after_persist) {
        for (size_t i = 0; i < p->msg_queue_after_persist_len; i++) {
            paxos_msg_t* m = &p->msg_queue_after_persist[i];
            if ((m->type == MSG_PROMISE || m->type == MSG_FETCH_ENTRIES_RES || m->type == MSG_ACCEPT) && m->entries) {
                for (size_t j = 0; j < m->num_entries; j++) paxos_entry_destroy(&m->entries[j]);
                free(m->entries);
            } else if (m->type == MSG_INSTALL_SNAPSHOT && m->snapshot_data) {
                free(m->snapshot_data);
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

static bool validate_entry(paxos_entry_t* e) {
    if (e->data_len > PAXOS_MAX_PAYLOAD_SIZE) return false;
    if (e->data_len > 0 && !e->data) return false;

    // Validate ADD/REMOVE
    if ((e->type == ENTRY_CONF_ADD || e->type == ENTRY_CONF_REMOVE) && e->data_len != sizeof(uint64_t)) return false;

    // FAANG: Strict JOINT validation
    if (e->type == ENTRY_CONF_JOINT) {
        if (e->data_len == 0 || e->data_len % sizeof(uint64_t) != 0) return false;

        size_t count = e->data_len / sizeof(uint64_t);
        if (count > MAX_PEERS) return false;

        uint64_t* nodes = (uint64_t*)e->data;
        for (size_t i = 0; i < count; i++) {
            if (nodes[i] == 0) return false; // Null node ID
            for (size_t j = i + 1; j < count; j++) {
                if (nodes[i] == nodes[j]) return false; // Duplicate node ID!
            }
        }
    }

    // FAANG: Strict FINAL validation
    if (e->type == ENTRY_CONF_FINAL && e->data_len != 0) return false;

    return true;
}

static bool paxos_msg_is_valid(paxos_msg_t* msg) {
    if (msg->type == MSG_PROPOSE) {
        if (msg->num_entries == 0 || !msg->entries) return false;
        for (size_t i = 0; i < msg->num_entries; i++) {
            if (!validate_entry(&msg->entries[i])) return false;
        }
        return true;
    }
    if (msg->ballot == 0 && msg->type != MSG_READ_BARRIER && msg->type != MSG_TICK && msg->type != MSG_NACK) return false;
    if (msg->type == MSG_PROMISE || msg->type == MSG_FETCH_ENTRIES_RES || msg->type == MSG_ACCEPT) {
        if (msg->num_entries > 0 && !msg->entries) return false;
        for (size_t i = 0; i < msg->num_entries; i++) {
            if (!validate_entry(&msg->entries[i])) return false;
        }
    }
    return true;
}

static bool paxos_is_valid_peer(paxos_t* p, uint64_t from_id) {
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] == from_id) {
            // FAANG: Allow silent learners to communicate and catch up!
            return true;
        }
    }
    return false;
}

void paxos_tick(paxos_t* p) {
    if (p->fatal_error) return;
    p->current_tick++;
    p->ticks_since_last_fetch++;

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

// FAANG: Joint Consensus Auto-Proposal Interceptor
static void process_auto_proposals(paxos_t* p) {
    if (p->needs_conf_final && p->state == PAXOS_STATE_ACTIVE && p->id == p->leader_id) {
        p->needs_conf_final = false;
        paxos_entry_t e_final = { .type = ENTRY_CONF_FINAL, .data_len = 0, .data = NULL };
        paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e_final, .num_entries = 1 };
        paxos_step_local(p, &prop);
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
        uint64_t batch_start_slot = p->next_slot;
        size_t batch_count = 0;

        for (size_t i = 0; i < msg->num_entries; i++) {
            paxos_entry_t proposal = msg->entries[i];

#if PAXOS_ENABLE_RECONFIG
            uint64_t joint_nodes[MAX_PEERS];

            if (proposal.type == ENTRY_CONF_ADD || proposal.type == ENTRY_CONF_REMOVE) {
                if (p->pending_reconfig) continue; // Drop to enforce Alpha-Window

                uint64_t target_node; memcpy(&target_node, proposal.data, sizeof(uint64_t));
                size_t t_idx = 0; bool found = false;

                // Implicitly add them as a silent Learner so they start getting heartbeats
                for (size_t n = 0; n < p->num_nodes; n++) {
                    if (p->node_directory[n] == target_node) { t_idx = n; found = true; break; }
                }
                if (!found && p->num_nodes < MAX_PEERS) {
                    p->node_directory[p->num_nodes] = target_node;
                    t_idx = p->num_nodes++;
                }

                // FAANG: The Learner Liveness Firewall
                if (proposal.type == ENTRY_CONF_ADD) {
                    if (p->peer_match_index[t_idx] == 0 ||
                       (p->local_commit_index > 100 && p->peer_match_index[t_idx] < p->local_commit_index - 100)) {
                        continue; // Drop the proposal! They aren't ready to vote.
                    }
                }

                size_t j_count = 0;
                for(size_t n = 0; n < p->num_nodes; n++) {
                    uint64_t current = p->node_directory[n];
                    if (proposal.type == ENTRY_CONF_REMOVE && current == target_node) continue;
                    joint_nodes[j_count++] = current;
                }

                proposal.type = ENTRY_CONF_JOINT;
                proposal.data = (uint8_t*)joint_nodes;
                proposal.data_len = j_count * sizeof(uint64_t);
            }
#else
            if (proposal.type >= ENTRY_CONF_ADD && proposal.type <= ENTRY_CONF_FINAL) continue;
#endif

            if (p->next_slot - p->local_commit_index >= INFLIGHT_WINDOW) break;
            paxos_inflight_slot_t* target_inf = &p->inflight[p->next_slot % INFLIGHT_WINDOW];
            if (target_inf->active) break;

            uint64_t slot = p->next_slot++;

            if (!paxos_log_accept(p, slot, p->active_ballot, proposal.type, proposal.client_id, proposal.client_seq, proposal.data, proposal.data_len)) break;

#if PAXOS_ENABLE_RECONFIG
            if (proposal.type == ENTRY_CONF_JOINT) p->pending_reconfig = true;
#endif

            paxos_inflight_slot_t* inf = &p->inflight[slot % INFLIGHT_WINDOW];
            inf->slot = slot;
            inf->ballot = p->active_ballot;
            inf->ack_mask = paxos_peer_bit(p, p->id);
            inf->chosen = false;
            inf->active = true;

            batch_count++;

            // Single Node Instant Commit Fast-Path
            if (paxos_has_quorum(p, inf->ack_mask)) {
                inf->chosen = true;
                uint64_t c_idx = paxos_chunk_idx(p, slot);
                uint64_t c_off = paxos_chunk_off(slot);
                p->log_chunks[c_idx]->slots[c_off].chosen = true;
                p->local_commit_index = slot;
                p->leader_commit_hint = slot;
                inf->active = false;

#if PAXOS_ENABLE_RECONFIG
                if (proposal.type >= ENTRY_CONF_ADD && proposal.type <= ENTRY_CONF_FINAL) paxos_rebuild_config(p);
#endif
            }
        }

        // FAANG: Batched Broadcasts
        if (batch_count > 0 && p->num_nodes > 1) {
            uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
            for (size_t i = 0; i < p->num_nodes; i++) {
                if (p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {

                    paxos_entry_t* batch_arr = calloc(batch_count, sizeof(paxos_entry_t));
                    for (size_t b = 0; b < batch_count; b++) {
                        paxos_entry_clone_retain(&batch_arr[b], paxos_log_get(p, batch_start_slot + b));
                    }

                    paxos_msg_t acc = {
                        .type = MSG_ACCEPT,
                        .ballot = p->active_ballot,
                        .slot = batch_start_slot,
                        .commit_index = p->local_commit_index,
                        .entries = batch_arr,
                        .num_entries = batch_count
                    };
                    acc.to = p->node_directory[i];
                    paxos_send_after_persist(p, acc);
                }
            }
        }
    } else if (msg->type == MSG_READ_BARRIER) {
        extern void paxos_proposer_read_barrier_local(paxos_t* p, paxos_msg_t* msg);
        paxos_proposer_read_barrier_local(p, msg);
    }

    process_auto_proposals(p);
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
    process_auto_proposals(p);
}

paxos_ready_t paxos_get_ready(paxos_t* p) {
    paxos_ready_t ready = {0};
    if (p->fatal_error) return ready;

    ready.hard_state.promised_ballot = p->promised_ballot;
    ready.hard_state.max_generated_ballot = p->max_generated_ballot;

    // FAANG: Export Durable Configuration
    ready.hard_state.active_config_mask = p->active_config_mask;
    ready.hard_state.joint_config_mask = p->joint_config_mask;
    ready.hard_state.pending_reconfig = p->pending_reconfig;

    ready.hard_state.has_update = (p->promised_ballot != p->prev_hard_state.promised_ballot) ||
                                  (p->max_generated_ballot != p->prev_hard_state.max_generated_ballot) ||
                                  (p->active_config_mask != p->prev_hard_state.active_config_mask) ||
                                  (p->joint_config_mask != p->prev_hard_state.joint_config_mask);

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
                uint64_t c_idx = paxos_chunk_idx(p, i);
                uint64_t c_off = paxos_chunk_off(i);

                if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx]) break;
                paxos_log_slot_t* slot_data = &p->log_chunks[c_idx]->slots[c_off];
                if (!slot_data->has_value || !slot_data->chosen) break;

                if (paxos_entry_clone_retain(&ready.chosen_entries[valid], &slot_data->entry)) {
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

void paxos_advance(paxos_t* p, const uint64_t* stable_slots, size_t num_stable_slots, uint64_t applied_slot) {
    if (p->fatal_error) return;

    if (applied_slot > p->last_applied) p->last_applied = applied_slot;

    for (size_t i = 0; i < num_stable_slots; i++) {
        uint64_t s = stable_slots[i];
        if (s < p->log_base_slot) continue;
        uint64_t c_idx = paxos_chunk_idx(p, s);
        uint64_t c_off = paxos_chunk_off(s);
        if (c_idx < p->log_chunks_cap && p->log_chunks[c_idx] && p->log_chunks[c_idx]->slots[c_off].has_value) {
            p->log_chunks[c_idx]->slots[c_off].unstable = false;
            if (s > p->stable_accepted_through) p->stable_accepted_through = s;
        }
    }

    // FAANG: Lazy Unstable Vector Compaction.
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

    p->prev_hard_state.promised_ballot = p->promised_ballot;
    p->prev_hard_state.max_generated_ballot = p->max_generated_ballot;
    p->prev_hard_state.active_config_mask = p->active_config_mask;
    p->prev_hard_state.joint_config_mask = p->joint_config_mask;
    p->prev_hard_state.pending_reconfig = p->pending_reconfig;

    for (size_t i = 0; i < p->msg_queue_immediate_len; i++) {
        if (p->msg_queue_immediate[i].type == MSG_INSTALL_SNAPSHOT && p->msg_queue_immediate[i].snapshot_data) {
            free(p->msg_queue_immediate[i].snapshot_data);
        }
    }
    p->msg_queue_immediate_len = 0;
    p->num_read_states = 0;

    if (p->msg_queue_after_persist) {
        for (size_t i = 0; i < p->msg_queue_after_persist_len; i++) {
            paxos_msg_t* m = &p->msg_queue_after_persist[i];
            if ((m->type == MSG_PROMISE || m->type == MSG_FETCH_ENTRIES_RES || m->type == MSG_ACCEPT) && m->entries) {
                for(size_t j = 0; j < m->num_entries; j++) {
                    paxos_entry_destroy(&m->entries[j]);
                }
                free(m->entries);
            } else if (m->type == MSG_INSTALL_SNAPSHOT && m->snapshot_data) {
                free(m->snapshot_data);
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
uint64_t paxos_last_slot(paxos_t* p) { return p ? p->highest_slot : 0; }

bool paxos_set_snapshot_chunk(paxos_t* p, uint64_t peer_id, const uint8_t* data, size_t len, uint64_t offset, bool done) {
    if (p->fatal_error || p->state != PAXOS_STATE_ACTIVE) return false;

    size_t peer_idx = 0; bool found = false;
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] == peer_id) { peer_idx = i; found = true; break; }
    }
    if (!found) return false;

    if (p->snapshot_offset[peer_idx] != offset) return false;

    uint8_t* chunk = NULL;
    if (len > 0 && data) {
        chunk = malloc(len);
        if (!chunk) { p->fatal_error = true; return false; }
        memcpy(chunk, data, len);
    }

    paxos_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT,
        .to = peer_id,
        .ballot = p->active_ballot,
        .slot = p->snapshot_index,
        .snapshot_offset = offset,
        .snapshot_len = len,
        .snapshot_data = chunk,
        .snapshot_done = done
    };

    paxos_send_immediate(p, snap);
    return true;
}

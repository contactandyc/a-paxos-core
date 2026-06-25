// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

static void merge_into_recovery(paxos_t* p, paxos_entry_t* e) {
    if (e->slot >= p->recovery_cap) {
        size_t new_cap = e->slot + 1024;
        paxos_recovery_slot_t* new_buf = realloc(p->recovery_buffer, new_cap * sizeof(paxos_recovery_slot_t));
        if (!new_buf) { p->fatal_error = true; return; }
        memset(new_buf + p->recovery_cap, 0, (new_cap - p->recovery_cap) * sizeof(paxos_recovery_slot_t));
        p->recovery_buffer = new_buf;
        p->recovery_cap = new_cap;
    }
    if (e->slot > p->recovery_max_slot) p->recovery_max_slot = e->slot;

    paxos_recovery_slot_t* r_slot = &p->recovery_buffer[e->slot];
    if (!r_slot->has_value || e->accepted_ballot > r_slot->highest_ballot_seen) {
        if (r_slot->has_value) paxos_entry_destroy(&r_slot->recovered_value);

        r_slot->highest_ballot_seen = e->accepted_ballot;
        r_slot->has_value = true;
        // The network payload is already deep-copied by the framework into msg, we can retain it here safely.
        if (!paxos_entry_clone_retain(&r_slot->recovered_value, e)) p->fatal_error = true;
    }
}

static void check_promise_quorum_and_activate(paxos_t* p) {
    if (paxos_has_quorum(p, p->promise_mask)) {
        p->state = PAXOS_STATE_RECOVERING_PHASE2; p->leader_id = p->id;

        for (uint64_t s = p->local_commit_index + 1; s <= p->recovery_max_slot; s++) {
            paxos_recovery_slot_t* r_slot = &p->recovery_buffer[s];
            paxos_entry_t final_val = {0};
            if (r_slot->has_value) final_val = r_slot->recovered_value;
            else { final_val.type = ENTRY_NOOP; final_val.data_len = 0; }

            paxos_log_accept(p, s, p->active_ballot, final_val.type, final_val.client_id, final_val.client_seq, final_val.data, final_val.data_len);

            paxos_inflight_slot_t* r_inf = &p->inflight[s % INFLIGHT_WINDOW];
            r_inf->slot = s; r_inf->ballot = p->active_ballot;
            r_inf->ack_mask = paxos_peer_bit(p, p->id);
            r_inf->chosen = false; r_inf->active = true;

            if (paxos_has_quorum(p, r_inf->ack_mask)) {
                uint64_t c_idx = s / PAXOS_LOG_CHUNK_SIZE;
                uint64_t c_off = s % PAXOS_LOG_CHUNK_SIZE;
                p->log_chunks[c_idx]->slots[c_off].chosen = true;
                if (final_val.type >= ENTRY_CONF_ADD && final_val.type <= ENTRY_CONF_FINAL) paxos_rebuild_config(p);
                if (s > p->local_commit_index) p->local_commit_index = s;
                p->leader_commit_hint = s;
                r_inf->active = false; // Clear single-node inflight
            } else {
                uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
                for(size_t j = 0; j < p->num_nodes; j++) {
                    if (p->node_directory[j] != p->id && ((1ULL << j) & combined_mask)) {
                        paxos_entry_t* safe_copy = malloc(sizeof(paxos_entry_t));
                        if (safe_copy && paxos_entry_clone_retain(safe_copy, paxos_log_get(p, s))) {
                            paxos_msg_t acc = { .type = MSG_ACCEPT, .ballot = p->active_ballot, .slot = s, .commit_index = p->local_commit_index, .entries = safe_copy, .num_entries = 1 };
                            acc.to = p->node_directory[j]; paxos_send_after_persist(p, acc);
                        } else { if (safe_copy) free(safe_copy); p->fatal_error = true; }
                    }
                }
            }
        }
        p->next_slot = p->recovery_max_slot + 1;

        if (p->local_commit_index >= p->recovery_max_slot) {
            p->state = PAXOS_STATE_ACTIVE;
            uint64_t noop_slot = p->next_slot++;
            if (paxos_log_accept(p, noop_slot, p->active_ballot, ENTRY_NOOP, 0, 0, NULL, 0)) {
                paxos_inflight_slot_t* n_inf = &p->inflight[noop_slot % INFLIGHT_WINDOW];
                n_inf->slot = noop_slot; n_inf->ballot = p->active_ballot;
                n_inf->ack_mask = paxos_peer_bit(p, p->id);
                n_inf->chosen = false; n_inf->active = true;

                if (paxos_has_quorum(p, n_inf->ack_mask)) {
                    n_inf->chosen = true;
                    uint64_t c_idx = noop_slot / PAXOS_LOG_CHUNK_SIZE;
                    uint64_t c_off = noop_slot % PAXOS_LOG_CHUNK_SIZE;
                    p->log_chunks[c_idx]->slots[c_off].chosen = true;
                    p->local_commit_index = noop_slot; p->leader_commit_hint = noop_slot;
                    n_inf->active = false;
                }

                uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
                for(size_t j = 0; j < p->num_nodes; j++) {
                    if (p->node_directory[j] != p->id && ((1ULL << j) & combined_mask)) {
                        paxos_entry_t* safe_copy = malloc(sizeof(paxos_entry_t));
                        if (safe_copy && paxos_entry_clone_retain(safe_copy, paxos_log_get(p, noop_slot))) {
                            paxos_msg_t acc = { .type = MSG_ACCEPT, .ballot = p->active_ballot, .slot = noop_slot, .commit_index = p->local_commit_index, .entries = safe_copy, .num_entries = 1 };
                            acc.to = p->node_directory[j]; paxos_send_after_persist(p, acc);
                        } else { if (safe_copy) free(safe_copy); p->fatal_error = true; }
                    }
                }
            }
        }
    }
}

void paxos_proposer_campaign(paxos_t* p) {
    p->state = PAXOS_STATE_RECOVERING_PHASE1;
    uint64_t max_b = p->max_generated_ballot;
    if (p->promised_ballot > max_b) max_b = p->promised_ballot;
    if (p->active_ballot > max_b) max_b = p->active_ballot;
    if (p->last_observed_ballot > max_b) max_b = p->last_observed_ballot;

    uint64_t epoch = (max_b >> 16) + 1;
    p->active_ballot = (epoch << 16) | (p->id & 0xFFFF);
    p->max_generated_ballot = p->active_ballot;

    p->promise_mask = paxos_peer_bit(p, p->id);
    p->self_promised = true;
    p->promised_ballot = p->active_ballot;

    if (p->recovery_buffer) {
        for (size_t i = 0; i < p->recovery_cap; i++) if (p->recovery_buffer[i].has_value) paxos_entry_destroy(&p->recovery_buffer[i].recovered_value);
        memset(p->recovery_buffer, 0, p->recovery_cap * sizeof(paxos_recovery_slot_t));
    }

    p->recovery_max_slot = p->local_commit_index;
    size_t count = 0;
    paxos_entry_t* suf = paxos_log_extract_suffix(p, p->local_commit_index + 1, &count);
    for (size_t i = 0; i < count; i++) merge_into_recovery(p, &suf[i]);
    if (suf) { for(size_t i=0; i<count; i++) paxos_entry_destroy(&suf[i]); free(suf); }

    uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
            paxos_msg_t req = { .type = MSG_PREPARE, .to = p->node_directory[i], .ballot = p->active_ballot, .slot = p->local_commit_index + 1 };
            paxos_send_after_persist(p, req);
        }
    }
    check_promise_quorum_and_activate(p);
}

void paxos_proposer_read_barrier_local(paxos_t* p, paxos_msg_t* msg) {
    if (p->state != PAXOS_STATE_ACTIVE) return;
    paxos_entry_t* current_commit = paxos_log_get(p, p->local_commit_index);
    if (!current_commit || current_commit->accepted_ballot != p->active_ballot) return;

    uint64_t read_seq = ++p->current_read_seq;
    paxos_pending_read_t* pr = NULL;
    for (int i = 0; i < MAX_PENDING_READS; i++) {
        if (!p->pending_reads[i].active) { pr = &p->pending_reads[i]; break; }
    }
    if (!pr) return;

    pr->active = true; pr->read_seq = read_seq; pr->client_ctx = msg->read_seq;
    pr->slot = p->local_commit_index; pr->ack_mask = paxos_peer_bit(p, p->id);

    if (paxos_has_quorum(p, pr->ack_mask)) {
        if (p->num_read_states >= p->read_states_cap) {
            size_t new_cap = p->read_states_cap == 0 ? 16 : p->read_states_cap * 2;
            paxos_read_state_t* new_rs = realloc(p->read_states, new_cap * sizeof(paxos_read_state_t));
            if (!new_rs) { p->fatal_error = true; return; }
            p->read_states = new_rs; p->read_states_cap = new_cap;
        }
        p->read_states[p->num_read_states].read_seq = pr->client_ctx;
        p->read_states[p->num_read_states].slot = pr->slot; p->num_read_states++; pr->active = false;
    } else {
        uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
        for (size_t i = 0; i < p->num_nodes; i++) {
            if (p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
                paxos_msg_t req = { .type = MSG_READ_BARRIER, .to = p->node_directory[i], .ballot = p->active_ballot, .read_seq = read_seq };
                paxos_send_immediate(p, req);
            }
        }
    }
}

static void handle_read_barrier_res(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (p->state != PAXOS_STATE_ACTIVE || msg->ballot != p->active_ballot) return;

    uint64_t peer_mask = paxos_peer_bit(p, msg->from);
    if (peer_mask == 0) return;

    for (int i = 0; i < MAX_PENDING_READS; i++) {
        paxos_pending_read_t* pr = &p->pending_reads[i];
        if (pr->active && pr->read_seq == msg->read_seq && !(pr->ack_mask & peer_mask)) {
            pr->ack_mask |= peer_mask;
            if (paxos_has_quorum(p, pr->ack_mask)) {
                if (p->num_read_states >= p->read_states_cap) {
                    size_t new_cap = p->read_states_cap == 0 ? 16 : p->read_states_cap * 2;
                    paxos_read_state_t* new_rs = realloc(p->read_states, new_cap * sizeof(paxos_read_state_t));
                    if (!new_rs) { p->fatal_error = true; return; }
                    p->read_states = new_rs; p->read_states_cap = new_cap;
                }
                p->read_states[p->num_read_states].read_seq = pr->client_ctx;
                p->read_states[p->num_read_states].slot = pr->slot; p->num_read_states++; pr->active = false;
            }
            break;
        }
    }
}

static void handle_promise(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (p->state != PAXOS_STATE_RECOVERING_PHASE1 || msg->ballot != p->active_ballot) return;

    uint64_t peer_mask = paxos_peer_bit(p, msg->from);
    if (peer_mask == 0 || (p->promise_mask & peer_mask)) return;
    p->promise_mask |= peer_mask;

    for (size_t i = 0; i < msg->num_entries; i++) merge_into_recovery(p, &msg->entries[i]);
    check_promise_quorum_and_activate(p);
}

static void handle_accepted(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if ((p->state != PAXOS_STATE_ACTIVE && p->state != PAXOS_STATE_RECOVERING_PHASE2) || msg->ballot != p->active_ballot) return;
    if (msg->slot <= p->local_commit_index) return;

    paxos_entry_t* local = paxos_log_get(p, msg->slot);
    if (!local || local->accepted_ballot != msg->ballot) return;

    uint64_t peer_mask = paxos_peer_bit(p, msg->from);
    if (peer_mask == 0) return;

    paxos_inflight_slot_t* inf = &p->inflight[msg->slot % INFLIGHT_WINDOW];
    if (inf->active && (inf->slot != msg->slot || inf->ballot != msg->ballot)) return;

    if (!inf->active) {
        inf->slot = msg->slot; inf->ballot = msg->ballot;
        inf->ack_mask = 0; inf->chosen = false; inf->active = true;
    }

    if (!(inf->ack_mask & peer_mask)) {
        inf->ack_mask |= peer_mask;
        if (paxos_has_quorum(p, inf->ack_mask)) {
            inf->chosen = true;
            uint64_t highest_contiguous_chosen = p->local_commit_index;
            while (highest_contiguous_chosen + 1 < p->next_slot) {
                paxos_inflight_slot_t* next_inf = &p->inflight[(highest_contiguous_chosen + 1) % INFLIGHT_WINDOW];
                if (next_inf->active && next_inf->slot == highest_contiguous_chosen + 1 && next_inf->ballot == p->active_ballot && next_inf->chosen) {
                    uint64_t s = highest_contiguous_chosen + 1;
                    uint64_t c_idx = s / PAXOS_LOG_CHUNK_SIZE;
                    uint64_t c_off = s % PAXOS_LOG_CHUNK_SIZE;
                    if (c_idx < p->log_chunks_cap && p->log_chunks[c_idx] && p->log_chunks[c_idx]->slots[c_off].has_value) {
                        p->log_chunks[c_idx]->slots[c_off].chosen = true;
                        paxos_entry_t* e = &p->log_chunks[c_idx]->slots[c_off].entry;
                        if (e->type >= ENTRY_CONF_ADD && e->type <= ENTRY_CONF_FINAL) paxos_rebuild_config(p);
                    }
                    highest_contiguous_chosen++; next_inf->active = false;
                } else break;
            }

            if (highest_contiguous_chosen > p->leader_commit_hint) {
                p->leader_commit_hint = highest_contiguous_chosen; paxos_advance_local_commit(p, p->id, p->active_ballot);
            }

            if (p->state == PAXOS_STATE_RECOVERING_PHASE2 && p->local_commit_index >= p->recovery_max_slot) {
                p->state = PAXOS_STATE_ACTIVE;
                uint64_t noop_slot = p->next_slot++;
                if (paxos_log_accept(p, noop_slot, p->active_ballot, ENTRY_NOOP, 0, 0, NULL, 0)) {
                    paxos_inflight_slot_t* n_inf = &p->inflight[noop_slot % INFLIGHT_WINDOW];
                    n_inf->slot = noop_slot; n_inf->ballot = p->active_ballot;
                    n_inf->ack_mask = paxos_peer_bit(p, p->id);
                    n_inf->chosen = false; n_inf->active = true;

                    if (paxos_has_quorum(p, n_inf->ack_mask)) {
                        n_inf->chosen = true;
                        uint64_t c_idx = noop_slot / PAXOS_LOG_CHUNK_SIZE;
                        uint64_t c_off = noop_slot % PAXOS_LOG_CHUNK_SIZE;
                        p->log_chunks[c_idx]->slots[c_off].chosen = true;
                        p->local_commit_index = noop_slot; p->leader_commit_hint = noop_slot;
                        n_inf->active = false;
                    }

                    uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
                    for(size_t j = 0; j < p->num_nodes; j++) {
                        if (p->node_directory[j] != p->id && ((1ULL << j) & combined_mask)) {
                            paxos_entry_t* safe_copy = malloc(sizeof(paxos_entry_t));
                            if (safe_copy && paxos_entry_clone_retain(safe_copy, paxos_log_get(p, noop_slot))) {
                                paxos_msg_t acc = { .type = MSG_ACCEPT, .ballot = p->active_ballot, .slot = noop_slot, .commit_index = p->local_commit_index, .entries = safe_copy, .num_entries = 1 };
                                acc.to = p->node_directory[j]; paxos_send_after_persist(p, acc);
                            } else { if (safe_copy) free(safe_copy); p->fatal_error = true; }
                        }
                    }
                }
            }
        }
    }
}

static void handle_fetch_entries(paxos_t* p, paxos_msg_t* msg) {
    if (p->state != PAXOS_STATE_ACTIVE) return;
    paxos_msg_t res = { .type = MSG_FETCH_ENTRIES_RES, .to = msg->from, .ballot = p->active_ballot, .reject = false };

    if (msg->slot <= p->snapshot_index) {
        uint64_t peer_mask = paxos_peer_bit(p, msg->from);
        if (peer_mask == 0) return;
        size_t peer_idx = 0;
        for (size_t i = 0; i < p->num_nodes; i++) { if (p->node_directory[i] == msg->from) { peer_idx = i; break; } }

        paxos_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = msg->from, .ballot = p->active_ballot, .slot = p->snapshot_index, .snapshot_offset = p->snapshot_offset[peer_idx] };
        paxos_send_immediate(p, snap); return;
    }

    uint64_t end_slot = msg->commit_index;
    if (end_slot > p->local_commit_index) end_slot = p->local_commit_index;
    res.entries = paxos_log_extract_range(p, msg->slot, end_slot, &res.num_entries);
    paxos_send_after_persist(p, res);
}

static void handle_install_snapshot_res(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (p->state != PAXOS_STATE_ACTIVE || msg->ballot != p->active_ballot) return;

    uint64_t peer_mask = paxos_peer_bit(p, msg->from);
    if (peer_mask == 0) return;

    size_t peer_idx = 0;
    for (size_t i = 0; i < p->num_nodes; i++) { if (p->node_directory[i] == msg->from) { peer_idx = i; break; } }

    if (msg->reject) p->snapshot_offset[peer_idx] = msg->slot;
    else p->snapshot_offset[peer_idx] = msg->snapshot_done ? 0 : msg->slot;

    if (!msg->snapshot_done) {
        paxos_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = msg->from, .ballot = p->active_ballot, .slot = p->snapshot_index, .snapshot_offset = p->snapshot_offset[peer_idx] };
        paxos_send_immediate(p, snap);
    }
}

static void handle_nack(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->promised_ballot);
    if (msg->promised_ballot > p->promised_ballot) p->promised_ballot = msg->promised_ballot;
}

void paxos_proposer_step(paxos_t* p, paxos_msg_t* msg) {
    switch(msg->type) {
        case MSG_PROMISE: handle_promise(p, msg); break;
        case MSG_ACCEPTED: handle_accepted(p, msg); break;
        case MSG_NACK: handle_nack(p, msg); break;
        case MSG_FETCH_ENTRIES: handle_fetch_entries(p, msg); break;
        case MSG_INSTALL_SNAPSHOT_RES: handle_install_snapshot_res(p, msg); break;
        case MSG_READ_BARRIER_RES: handle_read_barrier_res(p, msg); break;
        default: break;
    }
}

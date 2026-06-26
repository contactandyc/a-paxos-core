// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

static void check_promise_quorum_and_activate(paxos_t* p) {
    if (paxos_has_quorum(p, p->promise_mask)) {
        p->state = PAXOS_STATE_RECOVERING_PHASE2;
        p->leader_id = p->id;

        uint64_t recovery_start = p->local_commit_index + 1;
        uint64_t deferred_commit_index = p->local_commit_index;
        uint64_t current_s = recovery_start;

        // FAANG: Batched Recovery. Compress thousands of recovered slots into safe network chunks.
        while (current_s <= p->recovery_max_slot) {
            size_t batch_count = 0;
            size_t batch_bytes = 0;
            uint64_t batch_start_slot = current_s;
            paxos_entry_t batch_arr[256];

            while (current_s <= p->recovery_max_slot && batch_count < 256) {
                uint64_t rel_idx = current_s - recovery_start;
                paxos_recovery_slot_t* r_slot = &p->recovery_buffer[rel_idx];

                paxos_entry_t final_val = {0};
                if (r_slot->has_value) final_val = r_slot->recovered_value;
                else { final_val.type = ENTRY_NOOP; final_val.data_len = 0; }

                if (batch_bytes + final_val.data_len > PAXOS_MAX_BATCH_BYTES && batch_count > 0) break;

                paxos_log_accept(p, current_s, p->active_ballot, final_val.type, final_val.client_id, final_val.client_seq, final_val.data, final_val.data_len);

                paxos_inflight_slot_t* r_inf = &p->inflight[current_s % INFLIGHT_WINDOW];
                r_inf->slot = current_s; r_inf->ballot = p->active_ballot;
                r_inf->ack_mask = paxos_peer_bit(p, p->id);
                r_inf->chosen = false; r_inf->active = true;

                if (paxos_has_quorum(p, r_inf->ack_mask)) {
                    paxos_log_learn_chosen(p, current_s, paxos_log_get_accepted(p, current_s));
                    if (current_s > deferred_commit_index) deferred_commit_index = current_s;
                    p->leader_commit_hint = current_s;
                    r_inf->active = false;
                }

                paxos_entry_clone_retain(&batch_arr[batch_count], paxos_log_get(p, current_s));
                batch_bytes += final_val.data_len;
                batch_count++;
                current_s++;
            }

            if (batch_count > 0) {
                uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
                paxos_entry_t* c_entry = paxos_log_get(p, deferred_commit_index);
                uint64_t hash = c_entry ? paxos_entry_hash(c_entry) : 0;

                for(size_t j = 0; j < MAX_PEERS; j++) {
                    if (p->node_directory[j] != 0 && p->node_directory[j] != p->id && ((1ULL << j) & combined_mask)) {
                        paxos_entry_t* safe_copy = malloc(batch_count * sizeof(paxos_entry_t));
                        if (safe_copy) {
                            for (size_t b = 0; b < batch_count; b++) paxos_entry_clone_retain(&safe_copy[b], &batch_arr[b]);
                            paxos_msg_t acc = {
                                .type = MSG_ACCEPT, .ballot = p->active_ballot, .slot = batch_start_slot,
                                .commit_index = deferred_commit_index,
                                .value_hash = hash,
                                .entries = safe_copy, .num_entries = batch_count
                            };
                            acc.to = p->node_directory[j];
                            paxos_send_after_persist(p, acc);
                        } else { p->fatal_error = true; }
                    }
                }
                for (size_t b = 0; b < batch_count; b++) paxos_entry_destroy(&batch_arr[b]);
            }
        }

        p->local_commit_index = deferred_commit_index;
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
                    paxos_log_learn_chosen(p, noop_slot, paxos_log_get_accepted(p, noop_slot));

                    p->local_commit_index = noop_slot; p->leader_commit_hint = noop_slot;
                    n_inf->active = false;
                }

                uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
                for(size_t j = 0; j < MAX_PEERS; j++) {
                    if (p->node_directory[j] != 0 && p->node_directory[j] != p->id && ((1ULL << j) & combined_mask)) {
                        paxos_entry_t* safe_copy = malloc(sizeof(paxos_entry_t));
                        if (safe_copy && paxos_entry_clone_retain(safe_copy, paxos_log_get(p, noop_slot))) {
                            paxos_entry_t* c_entry = paxos_log_get(p, p->local_commit_index);
                            paxos_msg_t acc = {
                                .type = MSG_ACCEPT, .ballot = p->active_ballot, .slot = noop_slot,
                                .commit_index = p->local_commit_index,
                                .value_hash = c_entry ? paxos_entry_hash(c_entry) : 0,
                                .entries = safe_copy, .num_entries = 1
                            };
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
    p->active_ballot = (p->active_ballot == 0) ? p->id : p->active_ballot + MAX_PEERS;
    if (p->active_ballot <= p->promised_ballot) p->active_ballot = p->promised_ballot + MAX_PEERS;
    p->max_generated_ballot = p->active_ballot;
    p->promised_ballot = p->active_ballot;
    p->leader_id = p->id;
    p->promise_mask = paxos_peer_bit(p, p->id);
    p->self_promised = true;
    p->recovery_max_slot = p->highest_slot > p->local_commit_index ? p->highest_slot : p->local_commit_index;

    for (size_t i = 0; i < p->recovery_cap; i++) {
        p->recovery_buffer[i].has_value = false;
        p->recovery_buffer[i].highest_ballot_seen = 0;
        paxos_entry_destroy(&p->recovery_buffer[i].recovered_value);
    }

    uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
    for (size_t i = 0; i < MAX_PEERS; i++) {
        if (p->node_directory[i] != 0 && p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
            paxos_msg_t prep = { .type = MSG_PREPARE, .ballot = p->active_ballot, .slot = p->snapshot_index + 1 };
            prep.to = p->node_directory[i];
            paxos_send_after_persist(p, prep);
        }
    }

    if (paxos_has_quorum(p, p->promise_mask)) {
        check_promise_quorum_and_activate(p);
    }
}

static void handle_promise(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);

    if (msg->ballot != p->active_ballot) return;
    if (p->state != PAXOS_STATE_RECOVERING_PHASE1 && p->state != PAXOS_STATE_RECOVERING_PHASE2) return;

    uint64_t peer_mask = paxos_peer_bit(p, msg->from);
    if (!(p->promise_mask & peer_mask)) {
        p->promise_mask |= peer_mask;

        uint64_t recovery_start = p->local_commit_index + 1;
        for (size_t i = 0; i < msg->num_entries; i++) {
            paxos_entry_t* e = &msg->entries[i];

            if (e->slot >= recovery_start) {
                uint64_t rel_idx = e->slot - recovery_start;

                if (rel_idx >= MAX_RECOVERY_GAP) {
                    p->fatal_error = true;
                    return;
                }

                if (rel_idx >= p->recovery_cap) {
                    size_t new_cap = rel_idx + 1024;
                    paxos_recovery_slot_t* new_buf = realloc(p->recovery_buffer, new_cap * sizeof(paxos_recovery_slot_t));
                    if (!new_buf) { p->fatal_error = true; return; }
                    for (size_t r = p->recovery_cap; r < new_cap; r++) {
                        new_buf[r].has_value = false;
                        new_buf[r].highest_ballot_seen = 0;
                        new_buf[r].recovered_value = (paxos_entry_t){0};
                    }
                    p->recovery_buffer = new_buf;
                    p->recovery_cap = new_cap;
                }

                if (e->slot > p->recovery_max_slot) p->recovery_max_slot = e->slot;

                paxos_recovery_slot_t* r_slot = &p->recovery_buffer[rel_idx];

                if (!r_slot->has_value || e->accepted_ballot > r_slot->highest_ballot_seen) {
                    r_slot->has_value = true;
                    r_slot->highest_ballot_seen = e->accepted_ballot;
                    paxos_entry_destroy(&r_slot->recovered_value);
                    paxos_entry_clone_deep(&r_slot->recovered_value, e);
                }
            }
        }

        if (p->state == PAXOS_STATE_RECOVERING_PHASE1 && paxos_has_quorum(p, p->promise_mask)) {
            check_promise_quorum_and_activate(p);
        }
    }
}

static void handle_accepted(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if ((p->state != PAXOS_STATE_ACTIVE && p->state != PAXOS_STATE_RECOVERING_PHASE2) || msg->ballot != p->active_ballot) return;

    size_t count = msg->num_entries > 0 ? msg->num_entries : 1;
    uint64_t highest_slot = msg->slot + count - 1;

    // FAANG: Cryptographically Validate the FULL BATCH ACK with ZERO heap allocations!
    if (msg->value_hash != 0) {
        uint64_t cumulative_hash = 14695981039346656037ULL;
        bool valid_batch = true;

        for (size_t k = 0; k < count; k++) {
            paxos_entry_t* e = paxos_log_get(p, msg->slot + k);
            if (!e) {
                valid_batch = false;
                break;
            }
            cumulative_hash ^= paxos_entry_hash(e);
            cumulative_hash *= 1099511628211ULL;
        }

        if (!valid_batch || cumulative_hash != msg->value_hash) {
            return; // Drop! Split-brain or corrupted ACK!
        }
    }

    size_t peer_idx = 0;
    for (size_t i = 0; i < MAX_PEERS; i++) {
        if (p->node_directory[i] == msg->from) { peer_idx = i; break; }
    }

    if (highest_slot > p->learner_state[peer_idx].caught_up_through) {
        p->learner_state[peer_idx].caught_up_through = highest_slot;
    }
    if (p->learner_state[peer_idx].caught_up_through >= p->local_commit_index) {
        p->learner_state[peer_idx].eligible_to_vote = true;
        p->learner_state[peer_idx].hard_state_initialized = true;
    }

    uint64_t peer_mask = paxos_peer_bit(p, msg->from);
    if (peer_mask == 0) return;

    bool newly_chosen = false;

    for (size_t i = 0; i < count; i++) {
        uint64_t s = msg->slot + i;
        if (s <= p->local_commit_index) continue;

        paxos_entry_t* local = paxos_log_get(p, s);
        if (!local || local->accepted_ballot != msg->ballot) continue;

        paxos_inflight_slot_t* inf = &p->inflight[s % INFLIGHT_WINDOW];
        if (inf->active && (inf->slot != s || inf->ballot != msg->ballot)) continue;

        if (!inf->active) {
            inf->slot = s; inf->ballot = msg->ballot;
            inf->ack_mask = 0; inf->chosen = false; inf->active = true;
        }

        if (!(inf->ack_mask & peer_mask)) {
            inf->ack_mask |= peer_mask;
            if (paxos_has_quorum(p, inf->ack_mask)) {
                inf->chosen = true;
                newly_chosen = true;
            }
        }
    }

    if (newly_chosen) {
        uint64_t highest_contiguous_chosen = p->local_commit_index;
        while (highest_contiguous_chosen + 1 < p->next_slot) {
            paxos_inflight_slot_t* next_inf = &p->inflight[(highest_contiguous_chosen + 1) % INFLIGHT_WINDOW];
            if (next_inf->active && next_inf->slot == highest_contiguous_chosen + 1 && next_inf->ballot == p->active_ballot && next_inf->chosen) {
                uint64_t s = highest_contiguous_chosen + 1;

                paxos_log_learn_chosen(p, s, paxos_log_get_accepted(p, s));

                highest_contiguous_chosen++; next_inf->active = false;
            } else break;
        }

        if (highest_contiguous_chosen > p->leader_commit_hint) {
            p->leader_commit_hint = highest_contiguous_chosen;
            paxos_advance_local_commit(p, p->id, p->active_ballot);
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
                    paxos_log_learn_chosen(p, noop_slot, paxos_log_get_accepted(p, noop_slot));

                    p->local_commit_index = noop_slot; p->leader_commit_hint = noop_slot;
                    n_inf->active = false;
                }

                uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
                for(size_t j = 0; j < MAX_PEERS; j++) {
                    if (p->node_directory[j] != 0 && p->node_directory[j] != p->id && ((1ULL << j) & combined_mask)) {
                        paxos_entry_t* safe_copy = malloc(sizeof(paxos_entry_t));
                        if (safe_copy && paxos_entry_clone_retain(safe_copy, paxos_log_get(p, noop_slot))) {
                            paxos_entry_t* c_entry = paxos_log_get(p, p->local_commit_index);
                            paxos_msg_t acc = {
                                .type = MSG_ACCEPT, .ballot = p->active_ballot, .slot = noop_slot,
                                .commit_index = p->local_commit_index,
                                .value_hash = c_entry ? paxos_entry_hash(c_entry) : 0,
                                .entries = safe_copy, .num_entries = 1
                            };
                            acc.to = p->node_directory[j]; paxos_send_after_persist(p, acc);
                        } else { if (safe_copy) free(safe_copy); p->fatal_error = true; }
                    }
                }
            }
        }
    }
}

static void handle_nack(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->promised_ballot);
}

static void handle_fetch_entries(paxos_t* p, paxos_msg_t* msg) {
    if (msg->ballot < p->active_ballot || p->state != PAXOS_STATE_ACTIVE) return;

    paxos_msg_t res = { .type = MSG_FETCH_ENTRIES_RES, .to = msg->from, .ballot = p->active_ballot, .reject = false, .commit_index = p->local_commit_index };

    if (msg->slot <= p->snapshot_index) {
        paxos_set_snapshot_chunk(p, msg->from, NULL, 0, 0, false);
        return;
    }

    uint64_t end_slot = msg->slot + 100;
    if (end_slot > p->highest_slot) end_slot = p->highest_slot;

    res.entries = paxos_log_extract_range(p, msg->slot, end_slot, &res.num_entries);
    if (!res.entries && res.num_entries > 0) return;

    paxos_send_immediate(p, res);
}

static void handle_install_snapshot_res(paxos_t* p, paxos_msg_t* msg) {
    if (msg->ballot < p->active_ballot || p->state != PAXOS_STATE_ACTIVE) return;

    size_t peer_idx = 0; bool found = false;
    for (size_t i = 0; i < MAX_PEERS; i++) {
        if (p->node_directory[i] == msg->from) { peer_idx = i; found = true; break; }
    }
    if (!found) return;

    if (msg->snapshot_done) {
        p->snapshot_offset[peer_idx] = p->snapshot_index + 1;

        p->learner_state[peer_idx].snapshot_installed = true;
        p->learner_state[peer_idx].hard_state_initialized = true;

        if (p->snapshot_index > p->learner_state[peer_idx].caught_up_through) {
            p->learner_state[peer_idx].caught_up_through = p->snapshot_index;
        }
        if (p->learner_state[peer_idx].caught_up_through >= p->local_commit_index) {
            p->learner_state[peer_idx].eligible_to_vote = true;
        }
    } else {
        p->snapshot_offset[peer_idx] = msg->slot;
        paxos_set_snapshot_chunk(p, msg->from, NULL, 0, msg->slot, false);
    }
}

static void handle_read_barrier_res(paxos_t* p, paxos_msg_t* msg) {
    if (p->state != PAXOS_STATE_ACTIVE) return;

    for (size_t i = 0; i < MAX_PENDING_READS; i++) {
        paxos_pending_read_t* r = &p->pending_reads[i];
        if (r->active && r->read_seq == msg->read_seq) {
            r->ack_mask |= paxos_peer_bit(p, msg->from);
            if (paxos_has_quorum(p, r->ack_mask)) {
                r->active = false;

                if (p->num_read_states >= p->read_states_cap) {
                    size_t new_cap = p->read_states_cap == 0 ? 16 : p->read_states_cap * 2;
                    paxos_read_state_t* new_buf = realloc(p->read_states, new_cap * sizeof(paxos_read_state_t));
                    if (!new_buf) { p->fatal_error = true; return; }
                    p->read_states = new_buf;
                    p->read_states_cap = new_cap;
                }

                p->read_states[p->num_read_states].read_seq = r->read_seq;
                p->read_states[p->num_read_states].slot = r->slot;
                p->num_read_states++;
            }
            break;
        }
    }
}

void paxos_proposer_read_barrier_local(paxos_t* p, paxos_msg_t* msg) {
    if (p->state != PAXOS_STATE_ACTIVE) return;

    size_t r_idx = p->current_read_seq % MAX_PENDING_READS;
    p->current_read_seq++;

    paxos_pending_read_t* r = &p->pending_reads[r_idx];
    r->read_seq = msg->read_seq;
    r->slot = p->local_commit_index;
    r->ack_mask = paxos_peer_bit(p, p->id);
    r->active = true;

    if (paxos_has_quorum(p, r->ack_mask)) {
        r->active = false;
        if (p->num_read_states >= p->read_states_cap) {
            size_t new_cap = p->read_states_cap == 0 ? 16 : p->read_states_cap * 2;
            paxos_read_state_t* new_buf = realloc(p->read_states, new_cap * sizeof(paxos_read_state_t));
            if (!new_buf) { p->fatal_error = true; return; }
            p->read_states = new_buf;
            p->read_states_cap = new_cap;
        }
        p->read_states[p->num_read_states].read_seq = r->read_seq;
        p->read_states[p->num_read_states].slot = r->slot;
        p->num_read_states++;
    } else {
        uint64_t combined_mask = p->active_config_mask | p->joint_config_mask;
        for (size_t i = 0; i < MAX_PEERS; i++) {
            if (p->node_directory[i] != 0 && p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
                paxos_msg_t rb = { .type = MSG_READ_BARRIER, .to = p->node_directory[i], .ballot = p->active_ballot, .read_seq = r->read_seq };
                paxos_send_immediate(p, rb);
            }
        }
    }
}

void paxos_proposer_step(paxos_t* p, paxos_msg_t* msg) {
    switch (msg->type) {
        case MSG_PROMISE: handle_promise(p, msg); break;
        case MSG_ACCEPTED: handle_accepted(p, msg); break;
        case MSG_NACK: handle_nack(p, msg); break;
        case MSG_FETCH_ENTRIES: handle_fetch_entries(p, msg); break;
        case MSG_INSTALL_SNAPSHOT_RES: handle_install_snapshot_res(p, msg); break;
        case MSG_READ_BARRIER_RES: handle_read_barrier_res(p, msg); break;
        default: break;
    }
}

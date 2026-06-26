// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

static void check_promise_quorum_and_activate(paxos_t* p) {
    if (paxos_has_quorum(p, p->promise_mask)) {
        p->state = PAXOS_STATE_RECOVERING_PHASE2;
        p->leader_id = p->id;

        uint64_t recovery_start = p->local_commit_index + 1;
        uint64_t deferred_commit_index = p->local_commit_index;

        for (uint64_t s = recovery_start; s <= p->recovery_max_slot; s++) {
            uint64_t rel_idx = s - recovery_start;
            paxos_recovery_slot_t* r_slot = &p->recovery_buffer[rel_idx];

            paxos_entry_t final_val = {0};
            if (r_slot->has_value) final_val = r_slot->recovered_value;
            else { final_val.type = ENTRY_NOOP; final_val.data_len = 0; }

            paxos_log_accept(p, s, p->active_ballot, final_val.type, final_val.client_id, final_val.client_seq, final_val.data, final_val.data_len);

            paxos_inflight_slot_t* r_inf = &p->inflight[s % INFLIGHT_WINDOW];
            r_inf->slot = s; r_inf->ballot = p->active_ballot;
            r_inf->ack_mask = paxos_peer_bit(p, p->id);
            r_inf->chosen = false; r_inf->active = true;

            if (paxos_has_quorum(p, r_inf->ack_mask)) {
                paxos_log_learn_chosen(p, s, paxos_log_get_accepted(p, s));

                if (s > deferred_commit_index) deferred_commit_index = s;
                p->leader_commit_hint = s;
                r_inf->active = false;
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
    p->active_ballot = (p->active_ballot == 0) ? p->id : p->active_ballot + p->num_nodes;
    if (p->active_ballot <= p->promised_ballot) p->active_ballot = p->promised_ballot + p->num_nodes;
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
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
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
    if (p->state != PAXOS_STATE_RECOVERING_PHASE1 || msg->ballot != p->active_ballot) return;

    uint64_t peer_mask = paxos_peer_bit(p, msg->from);
    if (!(p->promise_mask & peer_mask)) {
        p->promise_mask |= peer_mask;

        uint64_t recovery_start = p->local_commit_index + 1;
        for (size_t i = 0; i < msg->num_entries; i++) {
            paxos_entry_t* e = &msg->entries[i];
            if (e->slot > p->recovery_max_slot) {

                // FAANG: Restore the DOS Firewall
                if (e->slot - recovery_start >= MAX_RECOVERY_GAP) {
                    p->fatal_error = true;
                    return;
                }

                p->recovery_max_slot = e->slot;
                if (p->recovery_max_slot - recovery_start >= p->recovery_cap) {
                    size_t new_cap = (p->recovery_max_slot - recovery_start) + 1024;
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
            }

            if (e->slot >= recovery_start) {
                uint64_t rel_idx = e->slot - recovery_start;
                paxos_recovery_slot_t* r_slot = &p->recovery_buffer[rel_idx];
                if (!r_slot->has_value || e->accepted_ballot > r_slot->highest_ballot_seen) {
                    r_slot->has_value = true;
                    r_slot->highest_ballot_seen = e->accepted_ballot;
                    paxos_entry_destroy(&r_slot->recovered_value);
                    paxos_entry_clone_deep(&r_slot->recovered_value, e);
                }
            }
        }
        check_promise_quorum_and_activate(p);
    }
}

static void handle_accepted(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if ((p->state != PAXOS_STATE_ACTIVE && p->state != PAXOS_STATE_RECOVERING_PHASE2) || msg->ballot != p->active_ballot) return;

    size_t count = msg->num_entries > 0 ? msg->num_entries : 1;
    uint64_t highest_slot = msg->slot + count - 1;

    size_t peer_idx = 0;
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] == msg->from) { peer_idx = i; break; }
    }

    if (highest_slot > p->peer_match_index[peer_idx]) {
        p->peer_match_index[peer_idx] = highest_slot;
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
    for (size_t i = 0; i < p->num_nodes; i++) {
        if (p->node_directory[i] == msg->from) { peer_idx = i; found = true; break; }
    }
    if (!found) return;

    if (msg->snapshot_done) {
        p->snapshot_offset[peer_idx] = p->snapshot_index + 1;
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

    p->current_read_seq++;
    size_t r_idx = p->current_read_seq % MAX_PENDING_READS;

    paxos_pending_read_t* r = &p->pending_reads[r_idx];

    // FAANG: Safely route the host application's sequence ID context
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
        for (size_t i = 0; i < p->num_nodes; i++) {
            if (p->node_directory[i] != p->id && ((1ULL << i) & combined_mask)) {
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

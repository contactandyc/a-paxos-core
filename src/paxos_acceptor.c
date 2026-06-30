// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

static void reply_nack(paxos_t* p, paxos_msg_t* msg) {
    paxos_msg_t nack = {
        .type = PAXOS_MSG_NACK, .to = msg->from, .ballot = p->active_ballot,
        .promised_ballot = p->last_observed_ballot > p->promised_ballot ? p->last_observed_ballot : p->promised_ballot
    };
    paxos_send_immediate(p, nack);
}

static void check_and_fetch_gaps(paxos_t* p) {
    if (p->local_commit_index < p->leader_commit_hint) {
        if (p->ticks_since_last_fetch > 10) {
            paxos_msg_t fetch = {
                .type = PAXOS_MSG_FETCH_ENTRIES,
                .to = p->leader_id,
                .ballot = p->promised_ballot,
                .slot = p->local_commit_index + 1,
                .commit_index = p->leader_commit_hint
            };
            paxos_send_immediate(p, fetch);
            p->ticks_since_last_fetch = 0;
        }
    }
}

static void handle_prepare(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);

    if (msg->ballot < p->promised_ballot) {
        reply_nack(p, msg); return;
    }

    p->promised_ballot = msg->ballot;

    paxos_msg_t prom = { .type = PAXOS_MSG_PROMISE, .to = msg->from, .ballot = msg->ballot };

    if (p->highest_slot > p->snapshot_index) {
        prom.entries = paxos_log_extract_suffix(p, p->snapshot_index + 1, &prom.num_entries);
        if (!prom.entries && prom.num_entries > 0) return;
    }

    if (p->promised_ballot == p->prev_hard_state.promised_ballot) paxos_send_immediate(p, prom);
    else paxos_send_after_persist(p, prom);
}

static void handle_accept(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (msg->ballot < p->promised_ballot) {
        reply_nack(p, msg); return;
    }

    size_t successful_accepts = 0;

    for (size_t i = 0; i < msg->num_entries; i++) {
        paxos_entry_t* e = &msg->entries[i];
        uint64_t target_slot = msg->slot + i;

#if !PAXOS_ENABLE_RECONFIG
        if (e->type >= PAXOS_ENTRY_CONF_ADD && e->type <= PAXOS_ENTRY_CONF_FINAL) continue;
#endif

        paxos_entry_t* existing = paxos_log_get_accepted(p, target_slot);
        if (existing) {
            if (existing->accepted_ballot > msg->ballot) break;
            if (existing->accepted_ballot == msg->ballot && !paxos_entry_value_equal(existing, e)) {
                p->fatal_error = true; return;
            }
        }

        if (!paxos_log_accept(p, target_slot, msg->ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len)) break;
        successful_accepts++;
    }

    if (successful_accepts > 0) {
        p->promised_ballot = msg->ballot;
        p->leader_id = msg->from;

        paxos_msg_t res = {
            .type = PAXOS_MSG_ACCEPTED,
            .to = msg->from,
            .ballot = msg->ballot,
            .slot = msg->slot,
            .num_entries = successful_accepts,
            .value_hash = paxos_batch_hash(msg->entries, successful_accepts)
        };
        paxos_send_after_persist(p, res);
    }

    if (msg->commit_index > 0 && msg->value_hash != 0) {
        paxos_entry_t* local = paxos_log_get_accepted(p, msg->commit_index);
        if (local && paxos_entry_hash(local) == msg->value_hash) {
            paxos_log_learn_chosen(p, msg->commit_index, local);
        }
    }

    if (msg->commit_index > p->leader_commit_hint) p->leader_commit_hint = msg->commit_index;
    paxos_advance_local_commit(p, msg->from, msg->ballot);
    check_and_fetch_gaps(p);
}

static void handle_commit_notice(paxos_t* p, paxos_msg_t* msg) {
    if (msg->ballot < p->promised_ballot) return;

    if (msg->commit_index > 0 && msg->value_hash != 0) {
        paxos_entry_t* local = paxos_log_get_accepted(p, msg->commit_index);
        if (local && paxos_entry_hash(local) == msg->value_hash) {
            paxos_log_learn_chosen(p, msg->commit_index, local);
        }
    }

    if (msg->commit_index > p->leader_commit_hint) p->leader_commit_hint = msg->commit_index;
    paxos_advance_local_commit(p, msg->from, msg->ballot);
    check_and_fetch_gaps(p);
}

static void handle_fetch_entries_res(paxos_t* p, paxos_msg_t* msg) {
    if (msg->reject || msg->ballot < p->promised_ballot || msg->from != p->leader_id) return;

    for (size_t i = 0; i < msg->num_entries; i++) {
        paxos_entry_t* e = &msg->entries[i];
        paxos_log_learn_chosen(p, e->slot, e);
    }

    if (msg->commit_index > p->leader_commit_hint) p->leader_commit_hint = msg->commit_index;

    while (p->local_commit_index < p->leader_commit_hint) {
        uint64_t s = p->local_commit_index + 1;
        uint64_t c_idx = paxos_chunk_idx(p, s);
        uint64_t c_off = paxos_chunk_off(s);
        if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx] || !p->log_chunks[c_idx]->slots[c_off].is_chosen) break;

        p->local_commit_index++;

#if PAXOS_ENABLE_RECONFIG
        if (p->log_chunks[c_idx]->slots[c_off].chosen_entry.type >= PAXOS_ENTRY_CONF_ADD &&
            p->log_chunks[c_idx]->slots[c_off].chosen_entry.type <= PAXOS_ENTRY_CONF_FINAL) {
            paxos_rebuild_config(p);
        }
#endif

        if (p->log_chunks[c_idx]->slots[c_off].chosen_entry.type == PAXOS_ENTRY_CONF_FINAL ||
            p->log_chunks[c_idx]->slots[c_off].chosen_entry.type == PAXOS_ENTRY_CONF_REMOVE) {
            if (!(p->active_config_mask & paxos_peer_bit(p, p->id))) {
                p->state = PAXOS_STATE_LEARNER;
                p->leader_id = 0;
            }
        }
    }

    check_and_fetch_gaps(p);
}

static void handle_install_snapshot(paxos_t* p, paxos_msg_t* msg) {
    if (msg->ballot < p->promised_ballot) return;

    if (msg->slot <= p->snapshot_index || msg->slot <= p->last_applied) {
        paxos_msg_t res = { .type = PAXOS_MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = false, .slot = msg->slot, .snapshot_done = true };
        paxos_send_immediate(p, res);
        return;
    }

    if (p->pending_snapshot_chunk_ready) {
        paxos_msg_t res = { .type = PAXOS_MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = true, .slot = p->expected_snapshot_offset - p->pending_snapshot_len };
        paxos_send_immediate(p, res);
        return;
    }

    if (msg->snapshot_len > 0 && !msg->snapshot_data) {
        paxos_msg_t res = { .type = PAXOS_MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = true, .slot = p->expected_snapshot_offset };
        paxos_send_immediate(p, res);
        return;
    }

    if (msg->snapshot_offset == 0) {
        if (p->pending_snapshot && p->pending_snapshot_msg_slot == msg->slot && p->expected_snapshot_offset > 0) {
            paxos_msg_t res = { .type = PAXOS_MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = true, .slot = p->expected_snapshot_offset };
            paxos_send_immediate(p, res);
            return;
        }
        p->expected_snapshot_offset = 0;
        p->pending_snapshot = true;
        p->pending_snapshot_from = msg->from;
        p->pending_snapshot_msg_slot = msg->slot;
        p->pending_snapshot_msg_ballot = msg->ballot;

        if (p->pending_snapshot_data) {
            free(p->pending_snapshot_data);
            p->pending_snapshot_data = NULL;
            p->pending_snapshot_len = 0;
        }
    } else if (!p->pending_snapshot || msg->snapshot_offset != p->expected_snapshot_offset) {
        paxos_msg_t res = { .type = PAXOS_MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = true, .slot = p->expected_snapshot_offset };
        paxos_send_immediate(p, res);
        return;
    }

    if (msg->snapshot_len > 0 && msg->snapshot_data) {
        uint8_t* new_buf = realloc(p->pending_snapshot_data, p->pending_snapshot_len + msg->snapshot_len);
        if (!new_buf) { p->fatal_error = true; return; }
        p->pending_snapshot_data = new_buf;
        memcpy(p->pending_snapshot_data + p->pending_snapshot_len, msg->snapshot_data, msg->snapshot_len);
        p->pending_snapshot_len += msg->snapshot_len;
    }

    p->expected_snapshot_offset += msg->snapshot_len;
    p->pending_snapshot_done = msg->snapshot_done;
    p->pending_snapshot_chunk_ready = true;
}

static void handle_read_barrier(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (msg->ballot < p->promised_ballot) return;

    paxos_msg_t res = { .type = PAXOS_MSG_READ_BARRIER_RES, .to = msg->from, .ballot = p->promised_ballot, .read_seq = msg->read_seq };

    if (p->promised_ballot == p->prev_hard_state.promised_ballot) paxos_send_immediate(p, res);
    else paxos_send_after_persist(p, res);
}

void paxos_acceptor_step(paxos_t* p, paxos_msg_t* msg) {
    switch (msg->type) {
        case PAXOS_MSG_PREPARE: handle_prepare(p, msg); break;
        case PAXOS_MSG_ACCEPT: handle_accept(p, msg); break;
        case PAXOS_MSG_COMMIT_NOTICE: handle_commit_notice(p, msg); break;
        case PAXOS_MSG_FETCH_ENTRIES_RES: handle_fetch_entries_res(p, msg); break;
        case PAXOS_MSG_INSTALL_SNAPSHOT: handle_install_snapshot(p, msg); break;
        case PAXOS_MSG_READ_BARRIER: handle_read_barrier(p, msg); break;
        case PAXOS_MSG_HEARTBEAT: handle_commit_notice(p, msg); break;
        default: break;
    }
}

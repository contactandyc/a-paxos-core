// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

static void reply_nack(paxos_t* p, paxos_msg_t* req) {
    paxos_msg_t res = { .type = MSG_NACK, .to = req->from, .ballot = req->ballot, .promised_ballot = p->promised_ballot };
    paxos_send_immediate(p, res);
}

static void check_and_fetch_gaps(paxos_t* p) {
    if (p->local_commit_index < p->leader_commit_hint && p->leader_id != 0) {
        if (p->ticks_since_last_fetch < 5) return;
        p->ticks_since_last_fetch = 0;

        paxos_msg_t fetch = {
            .type = MSG_FETCH_ENTRIES,
            .to = p->leader_id,
            .ballot = p->promised_ballot,
            .slot = p->local_commit_index + 1,
            .commit_index = p->leader_commit_hint
        };
        paxos_send_immediate(p, fetch);
    }
}

static void handle_fetch_entries_res(paxos_t* p, paxos_msg_t* msg) {
    if (msg->reject || msg->ballot < p->promised_ballot || msg->from != p->leader_id) return;

    for (size_t i = 0; i < msg->num_entries; i++) {
        paxos_entry_t* e = &msg->entries[i];
        paxos_log_accept(p, e->slot, e->accepted_ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len);

        uint64_t c_idx = e->slot / PAXOS_LOG_CHUNK_SIZE;
        uint64_t c_off = e->slot % PAXOS_LOG_CHUNK_SIZE;
        if (c_idx < p->log_chunks_cap && p->log_chunks[c_idx] && p->log_chunks[c_idx]->slots[c_off].has_value) {
            p->log_chunks[c_idx]->slots[c_off].chosen = true;
            if (e->type >= ENTRY_CONF_ADD && e->type <= ENTRY_CONF_FINAL) paxos_rebuild_config(p);
        }
    }
    paxos_advance_local_commit(p, msg->from, msg->ballot);
    check_and_fetch_gaps(p);
}

static void handle_install_snapshot(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (msg->ballot < p->promised_ballot) {
        reply_nack(p, msg); return;
    }

    p->promised_ballot = msg->ballot;
    p->leader_id = msg->from;

    if (msg->slot <= p->last_applied) {
        paxos_msg_t res = { .type = MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = false, .slot = msg->slot, .snapshot_done = true };
        paxos_send_immediate(p, res); return;
    }

    if (msg->slot > p->snapshot_index) {
        if (p->pending_snapshot_chunk_ready) {
            paxos_msg_t res = { .type = MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = true, .slot = p->expected_snapshot_offset - p->pending_snapshot_len };
            paxos_send_immediate(p, res); return;
        }
        if (msg->snapshot_len > 0 && !msg->snapshot_data) {
            paxos_msg_t res = { .type = MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = true, .slot = p->expected_snapshot_offset };
            paxos_send_immediate(p, res); return;
        }
        if (msg->snapshot_offset == 0) {
            p->expected_snapshot_offset = 0; p->pending_snapshot = true; p->pending_snapshot_from = msg->from;
            p->pending_snapshot_msg_slot = msg->slot; p->pending_snapshot_msg_ballot = msg->ballot;
        } else if (!p->pending_snapshot || msg->snapshot_offset != p->expected_snapshot_offset) {
            paxos_msg_t res = { .type = MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = true, .slot = p->expected_snapshot_offset };
            paxos_send_immediate(p, res); return;
        }

        uint8_t* chunk = NULL;
        if (msg->snapshot_len > 0) {
            chunk = malloc(msg->snapshot_len);
            if (!chunk) { p->fatal_error = true; return; }
            memcpy(chunk, msg->snapshot_data, msg->snapshot_len);
        }

        if (p->pending_snapshot_data) free(p->pending_snapshot_data);
        p->pending_snapshot_data = chunk;
        p->expected_snapshot_offset += msg->snapshot_len;
        p->pending_snapshot_len = msg->snapshot_len;
        p->pending_snapshot_offset = msg->snapshot_offset;
        p->pending_snapshot_done = msg->snapshot_done;
        p->pending_snapshot_chunk_ready = true;
    } else {
        paxos_msg_t res = { .type = MSG_INSTALL_SNAPSHOT_RES, .to = msg->from, .ballot = p->promised_ballot, .reject = false, .slot = msg->slot };
        paxos_send_immediate(p, res);
    }
}

static void handle_read_barrier(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (msg->ballot < p->promised_ballot) {
        reply_nack(p, msg); return;
    }
    paxos_msg_t res = { .type = MSG_READ_BARRIER_RES, .to = msg->from, .ballot = msg->ballot, .read_seq = msg->read_seq };
    if (msg->ballot > p->promised_ballot) {
        p->promised_ballot = msg->ballot; paxos_send_after_persist(p, res);
    } else paxos_send_immediate(p, res);
}

static void handle_prepare(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (msg->ballot < p->promised_ballot) {
        reply_nack(p, msg); return;
    }

    uint64_t start_slot = msg->slot <= p->snapshot_index ? p->snapshot_index + 1 : msg->slot;
    size_t suffix_count = 0;
    paxos_entry_t* suffix = paxos_log_extract_suffix(p, start_slot, &suffix_count);
    if (suffix_count > 0 && !suffix) { p->fatal_error = true; return; }

    paxos_msg_t res = { .type = MSG_PROMISE, .to = msg->from, .ballot = msg->ballot, .entries = suffix, .num_entries = suffix_count };
    if (msg->ballot == p->promised_ballot) paxos_send_immediate(p, res);
    else { p->promised_ballot = msg->ballot; paxos_send_after_persist(p, res); }
}

static void handle_accept(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (msg->ballot < p->promised_ballot) {
        reply_nack(p, msg); return;
    }

    if (msg->num_entries != 1) return;
    paxos_entry_t* e = &msg->entries[0];
    if (e->data_len > PAXOS_MAX_PAYLOAD_SIZE) return;

    paxos_entry_t* existing = paxos_log_get(p, msg->slot);
    if (existing) {
        if (existing->accepted_ballot > msg->ballot) { reply_nack(p, msg); return; }
        if (existing->accepted_ballot == msg->ballot && !paxos_entry_value_equal(existing, e)) { p->fatal_error = true; return; }
    }

    p->promised_ballot = msg->ballot; p->leader_id = msg->from;
    if (!paxos_log_accept(p, msg->slot, msg->ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len)) return;

    if (msg->commit_index > p->leader_commit_hint) p->leader_commit_hint = msg->commit_index;

    paxos_msg_t res = { .type = MSG_ACCEPTED, .to = msg->from, .ballot = msg->ballot, .slot = msg->slot };
    paxos_send_after_persist(p, res);
    paxos_advance_local_commit(p, msg->from, msg->ballot);
    check_and_fetch_gaps(p);
}

static void handle_tick(paxos_t* p, paxos_msg_t* msg) {
    observe_higher_ballot(p, msg->ballot);
    if (msg->ballot < p->promised_ballot) { reply_nack(p, msg); return; }

    p->leader_id = msg->from; p->current_tick = 0;
    if (msg->commit_index > p->leader_commit_hint) {
        p->leader_commit_hint = msg->commit_index;
        paxos_advance_local_commit(p, msg->from, msg->ballot);
        check_and_fetch_gaps(p);
    }
}

void paxos_acceptor_step(paxos_t* p, paxos_msg_t* msg) {
    switch (msg->type) {
        case MSG_TICK: handle_tick(p, msg); break;
        case MSG_PREPARE: handle_prepare(p, msg); break;
        case MSG_ACCEPT: handle_accept(p, msg); break;
        case MSG_COMMIT_NOTICE:
            observe_higher_ballot(p, msg->ballot);
            // FAANG: Ignore commit notices that violate safety invariants
            if (msg->ballot < p->promised_ballot || (p->leader_id != 0 && msg->from != p->leader_id)) return;

            p->leader_id = msg->from;
            if (msg->commit_index > p->leader_commit_hint) {
                p->leader_commit_hint = msg->commit_index;
                paxos_advance_local_commit(p, msg->from, msg->ballot);
                check_and_fetch_gaps(p);
            }
            break;
        case MSG_FETCH_ENTRIES_RES: handle_fetch_entries_res(p, msg); break;
        case MSG_INSTALL_SNAPSHOT: handle_install_snapshot(p, msg); break;
        case MSG_READ_BARRIER: handle_read_barrier(p, msg); break;
        default: break;
    }
}

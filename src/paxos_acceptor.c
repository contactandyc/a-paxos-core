// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "paxos_internal.h"
#include <string.h>
#include <stdlib.h>

static void reply_nack(paxos_t* p, paxos_msg_t* req) {
    paxos_msg_t res = {
        .type = MSG_NACK,
        .to = req->from,
        .promised_ballot = p->promised_ballot
    };
    paxos_send_immediate(p, res); // NACKs are safe to send immediately
}

static void check_and_fetch_gaps(paxos_t* p) {
    if (p->local_commit_index < p->leader_commit_hint && p->leader_id != 0) {
        paxos_msg_t fetch = {
            .type = MSG_FETCH_ENTRIES,
            .to = p->leader_id,
            .slot = p->local_commit_index + 1,    // <--- FIXED: changed .index to .slot
            .commit_index = p->leader_commit_hint
        };
        paxos_send_immediate(p, fetch);
    }
}

static void handle_fetch_entries_res(paxos_t* p, paxos_msg_t* msg) {
    if (msg->reject) return; // Wait for snapshot if rejected

    for (size_t i = 0; i < msg->num_entries; i++) {
        paxos_entry_t* e = &msg->entries[i];
        paxos_log_accept(p, e->slot, e->accepted_ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len);
    }

    paxos_advance_local_commit(p);
    check_and_fetch_gaps(p);
}

static void handle_prepare(paxos_t* p, paxos_msg_t* msg) {
    if (msg->ballot > p->last_observed_ballot) p->last_observed_ballot = msg->ballot;

    if (msg->ballot <= p->promised_ballot) {
        reply_nack(p, msg);
        return;
    }

    p->promised_ballot = msg->ballot;

    uint64_t start_slot = msg->slot;
    if (start_slot <= p->snapshot_index) start_slot = p->snapshot_index + 1;

    size_t suffix_count = 0;
    paxos_entry_t* suffix = paxos_log_extract_suffix(p, start_slot, &suffix_count);

    if (suffix_count > 0 && !suffix) {
        p->fatal_error = true;
        return;
    }

    paxos_msg_t res = {
        .type = MSG_PROMISE,
        .to = msg->from,
        .ballot = msg->ballot,
        .entries = suffix,
        .num_entries = suffix_count
    };
    // MUST persist promised_ballot before sending
    paxos_send_after_persist(p, res);
}

static void handle_accept(paxos_t* p, paxos_msg_t* msg) {
    if (msg->ballot > p->last_observed_ballot) p->last_observed_ballot = msg->ballot;

    if (msg->ballot < p->promised_ballot) {
        reply_nack(p, msg);
        return;
    }

    if (msg->commit_index > p->leader_commit_hint) {
        p->leader_commit_hint = msg->commit_index;
        paxos_advance_local_commit(p);
    }

    if (msg->num_entries != 1) return;
    paxos_entry_t* e = &msg->entries[0];

    paxos_entry_t* existing = paxos_log_get(p, msg->slot);
    if (existing) {
        if (existing->accepted_ballot > msg->ballot) {
            reply_nack(p, msg);
            return;
        }
        if (existing->accepted_ballot == msg->ballot) {
            if (existing->data_len != e->data_len || (e->data_len > 0 && memcmp(existing->data, e->data, e->data_len) != 0)) {
                p->fatal_error = true;
                return;
            }
        }
    }

    p->promised_ballot = msg->ballot;

    if (!paxos_log_accept(p, msg->slot, msg->ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len)) {
        return;
    }

    paxos_msg_t res = {
        .type = MSG_ACCEPTED,
        .to = msg->from,
        .ballot = msg->ballot,
        .slot = msg->slot
    };

    // MUST persist accepted_ballot and accepted_value before sending
    paxos_send_after_persist(p, res);
    paxos_advance_local_commit(p);
}

void paxos_acceptor_step(paxos_t* p, paxos_msg_t* msg) {
    switch (msg->type) {
        case MSG_PREPARE:
            handle_prepare(p, msg);
            break;
        case MSG_ACCEPT:
            p->leader_id = msg->from;
            handle_accept(p, msg);
            check_and_fetch_gaps(p);
            break;
        case MSG_COMMIT_NOTICE:
            p->leader_id = msg->from;
            if (msg->commit_index > p->leader_commit_hint) {
                p->leader_commit_hint = msg->commit_index;
                paxos_advance_local_commit(p);
                check_and_fetch_gaps(p);
            }
            break;
        case MSG_FETCH_ENTRIES_RES:
            handle_fetch_entries_res(p, msg);
            break;
        default:
            break;
    }
}

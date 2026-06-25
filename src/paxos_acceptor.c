// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "paxos_internal.h"
#include <string.h>
#include <stdlib.h>

// Generates a NACK message containing the highest promised ballot
static void reply_nack(paxos_t* p, paxos_msg_t* req) {
    paxos_msg_t res = {
        .type = MSG_NACK,
        .to = req->from,
        .promised_ballot = p->promised_ballot
    };
    paxos_send_msg(p, res);
}

// ----------------------------------------------------------------------------
// PHASE 1: PREPARE
// ----------------------------------------------------------------------------
static void handle_prepare(paxos_t* p, paxos_msg_t* msg) {
    if (msg->ballot <= p->promised_ballot) {
        reply_nack(p, msg);
        return;
    }

    // Bind the Acceptor to the new higher ballot
    p->promised_ballot = msg->ballot;

    // Generate the Promise containing the accepted unchosen suffix
    uint64_t start_slot = msg->slot; // first_uncommitted_slot requested by leader
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
    paxos_send_msg(p, res);
}

// ----------------------------------------------------------------------------
// PHASE 2: ACCEPT
// ----------------------------------------------------------------------------
static void handle_accept(paxos_t* p, paxos_msg_t* msg) {
    if (msg->ballot < p->promised_ballot) {
        reply_nack(p, msg);
        return;
    }

    // Acceptors must learn the leader's commit horizon
    if (msg->commit_index > p->commit_index) {
        p->commit_index = msg->commit_index;
    }

    p->promised_ballot = msg->ballot;

    // Extract the single entry being proposed
    if (msg->num_entries != 1) return;
    paxos_entry_t* e = &msg->entries[0];

    // Persist the Accepted Value to the sparse log
    if (!paxos_log_accept(p, msg->slot, msg->ballot, e->type, e->client_id, e->client_seq, e->data, e->data_len)) {
        return;
    }

    paxos_msg_t res = {
        .type = MSG_ACCEPTED,
        .to = msg->from,
        .ballot = msg->ballot,
        .slot = msg->slot
    };
    paxos_send_msg(p, res);
}

void paxos_acceptor_step(paxos_t* p, paxos_msg_t* msg) {
    switch (msg->type) {
        case MSG_PREPARE:
            handle_prepare(p, msg);
            break;
        case MSG_ACCEPT:
            handle_accept(p, msg);
            break;
        case MSG_COMMIT_NOTICE:
            if (msg->commit_index > p->commit_index) p->commit_index = msg->commit_index;
            break;
        default:
            break;
    }
}

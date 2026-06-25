// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "paxos_internal.h"
#include <string.h>
#include <stdlib.h>

// Evaluates if a quorum is reached (Total Voting Peers + Self)
static bool has_quorum(paxos_t* p, size_t count) {
    size_t required = ((p->num_peers + 1) / 2) + 1;
    return count >= required;
}

// ----------------------------------------------------------------------------
// PHASE 1: LEADERSHIP ACQUISITION & THE MERGE
// ----------------------------------------------------------------------------

void paxos_proposer_campaign(paxos_t* p) {
    p->state = PAXOS_STATE_RECOVERING;

    // Generate a mathematically unique monotonically increasing ballot
    uint64_t epoch = (p->max_generated_ballot >> 16) + 1;
    p->max_generated_ballot = (epoch << 16) | (p->id & 0xFFFF);
    p->active_ballot = p->max_generated_ballot;

    p->promises_received = 1; // Vote for self
    memset(p->promised_by, 0, sizeof(p->promised_by));
    p->recovery_max_slot = p->commit_index;

    // Clear Recovery Buffer
    if (p->recovery_buffer) {
        memset(p->recovery_buffer, 0, p->recovery_cap * sizeof(paxos_recovery_slot_t));
    }

    // Broadcast Prepare
    for (size_t i = 0; i < p->num_peers; i++) {
        paxos_msg_t req = {
            .type = MSG_PREPARE,
            .to = p->peers[i],
            .ballot = p->active_ballot,
            .slot = p->commit_index + 1 // Request suffix starting here
        };
        paxos_send_msg(p, req);
    }
}

static void handle_promise(paxos_t* p, paxos_msg_t* msg) {
    if (p->state != PAXOS_STATE_RECOVERING || msg->ballot != p->active_ballot) return;

    // Merge the Acceptor's Suffix into the Recovery Buffer
    for (size_t i = 0; i < msg->num_entries; i++) {
        paxos_entry_t* e = &msg->entries[i];

        // Dynamically resize recovery buffer if a high slot is discovered
        if (e->slot >= p->recovery_cap) {
            size_t new_cap = e->slot + 128;
            paxos_recovery_slot_t* new_buf = realloc(p->recovery_buffer, new_cap * sizeof(paxos_recovery_slot_t));
            if (!new_buf) { p->fatal_error = true; return; }
            memset(new_buf + p->recovery_cap, 0, (new_cap - p->recovery_cap) * sizeof(paxos_recovery_slot_t));
            p->recovery_buffer = new_buf;
            p->recovery_cap = new_cap;
        }

        if (e->slot > p->recovery_max_slot) p->recovery_max_slot = e->slot;

        // Paxos Core Safety Rule: ONLY adopt the value with the HIGHEST accepted ballot per slot.
        paxos_recovery_slot_t* r_slot = &p->recovery_buffer[e->slot];
        if (!r_slot->has_value || e->accepted_ballot > r_slot->highest_ballot_seen) {

            // Free the previous payload if we are overwriting it with a higher ballot's value
            if (r_slot->has_value && r_slot->recovered_value.data) {
                free(r_slot->recovered_value.data);
            }

            r_slot->highest_ballot_seen = e->accepted_ballot;
            r_slot->has_value = true;
            r_slot->recovered_value = *e;

            // Deep copy the payload so paxos_destroy can safely free it
            if (e->data_len > 0) {
                r_slot->recovered_value.data = malloc(e->data_len);
                if (r_slot->recovered_value.data) {
                    memcpy(r_slot->recovered_value.data, e->data, e->data_len);
                }
            } else {
                r_slot->recovered_value.data = NULL;
            }
        }
    }

    p->promises_received++;

    // Check if Phase 1 Quorum is achieved
    if (has_quorum(p, p->promises_received)) {
        p->state = PAXOS_STATE_ACTIVE;
        p->leader_id = p->id;

        // Execute the Final Merge and broadcast Phase 2 Accept overrides
        for (uint64_t s = p->commit_index + 1; s <= p->recovery_max_slot; s++) {
            paxos_recovery_slot_t* r_slot = &p->recovery_buffer[s];

            paxos_entry_t final_val = {0};
            if (r_slot->has_value) {
                final_val = r_slot->recovered_value;
            } else {
                // The Paxos Gap Resolution: If NO acceptor had a value, it is safely impossible
                // for this slot to have ever been chosen. We force a NoOp.
                final_val.type = ENTRY_NOOP;
                final_val.data_len = 0;
            }

            // Re-propose the resolved value under OUR new active ballot
            paxos_log_accept(p, s, p->active_ballot, final_val.type, final_val.client_id, final_val.client_seq, final_val.data, final_val.data_len);

            paxos_msg_t acc = {
                .type = MSG_ACCEPT,
                .ballot = p->active_ballot,
                .slot = s,
                .commit_index = p->commit_index,
                .entries = paxos_log_get(p, s),
                .num_entries = 1
            };

            for(size_t j = 0; j < p->num_peers; j++) {
                acc.to = p->peers[j];
                paxos_send_msg(p, acc);
            }
        }
        p->next_slot = p->recovery_max_slot + 1;
    }
}

// ----------------------------------------------------------------------------
// PHASE 2: NORMAL OPERATION & COMMIT MATH
// ----------------------------------------------------------------------------

static void handle_accepted(paxos_t* p, paxos_msg_t* msg) {
    if (p->state != PAXOS_STATE_ACTIVE || msg->ballot != p->active_ballot) return;

    // Fast path: ignore if already committed
    if (msg->slot <= p->commit_index) return;

    // Find peer index
    size_t peer_idx = 0;
    bool found = false;
    for (size_t i = 0; i < p->num_peers; i++) {
        if (p->peers[i] == msg->from) { peer_idx = i; found = true; break; }
    }
    if (!found) return;

    paxos_inflight_slot_t* inf = &p->inflight[msg->slot % 4096]; // Ring buffer index

    if (!inf->acked_by[peer_idx]) {
        inf->acked_by[peer_idx] = true;
        inf->acks++;

        // If quorum reached, this slot is mathematically CHOSEN.
        if (has_quorum(p, inf->acks + 1)) { // +1 for self
            inf->chosen = true;

            // Advance commit_index continuously
            while (p->commit_index + 1 < p->next_slot && p->inflight[(p->commit_index + 1) % 4096].chosen) {
                p->commit_index++;
                p->inflight[p->commit_index % 4096].chosen = false; // Reset for next wrap
                memset(p->inflight[p->commit_index % 4096].acked_by, 0, MAX_PEERS);
                p->inflight[p->commit_index % 4096].acks = 0;
            }
        }
    }
}

static void handle_nack(paxos_t* p, paxos_msg_t* msg) {
    if (msg->promised_ballot > p->active_ballot) {
        p->state = PAXOS_STATE_LEARNER;
        p->promised_ballot = msg->promised_ballot;
        p->leader_id = 0;
    }
}

void paxos_proposer_step(paxos_t* p, paxos_msg_t* msg) {
    switch(msg->type) {
        case MSG_PROMISE: handle_promise(p, msg); break;
        case MSG_ACCEPTED: handle_accepted(p, msg); break;
        case MSG_NACK: handle_nack(p, msg); break;
        default: break;
    }
}

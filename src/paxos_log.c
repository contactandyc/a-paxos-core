// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "paxos_internal.h"
#include <stdlib.h>
#include <string.h>

// Allows out-of-order sparse insertions (Crucial for Multi-Paxos Phase 2)
bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len) {
    if (p->fatal_error || slot <= p->snapshot_index) return false;

    // Dynamically resize log if the accepted slot causes a gap overflow
    uint64_t target_idx = slot - p->log_base_slot;
    if (target_idx >= p->log_cap) {
        size_t new_cap = target_idx + 1024;
        paxos_entry_t* new_log = realloc(p->log, new_cap * sizeof(paxos_entry_t));
        bool* new_flags = realloc(p->log_has_value, new_cap * sizeof(bool));

        if (!new_log || !new_flags) {
            p->fatal_error = true;
            return false;
        }

        memset(new_flags + p->log_cap, 0, (new_cap - p->log_cap) * sizeof(bool));
        p->log = new_log;
        p->log_has_value = new_flags;
        p->log_cap = new_cap;
    }

    if (data_len > 0 && !data) return false;

    // Clear old payload if overwriting
    if (p->log_has_value[target_idx] && p->log[target_idx].data) {
        free(p->log[target_idx].data);
    }

    uint8_t* payload = NULL;
    if (data_len > 0) {
        payload = malloc(data_len);
        if (!payload) { p->fatal_error = true; return false; }
        memcpy(payload, data, data_len);
    }

    p->log[target_idx].slot = slot;
    p->log[target_idx].accepted_ballot = ballot;
    p->log[target_idx].type = type;
    p->log[target_idx].client_id = cid;
    p->log[target_idx].client_seq = cseq;
    p->log[target_idx].data = payload;
    p->log[target_idx].data_len = data_len;

    p->log_has_value[target_idx] = true;
    return true;
}

paxos_entry_t* paxos_log_get(paxos_t* p, uint64_t slot) {
    if (slot <= p->snapshot_index || slot < p->log_base_slot) return NULL;
    uint64_t target_idx = slot - p->log_base_slot;
    if (target_idx >= p->log_cap || !p->log_has_value[target_idx]) return NULL;
    return &p->log[target_idx];
}

paxos_entry_t* paxos_log_extract_suffix(paxos_t* p, uint64_t start_slot, size_t* out_count) {
    *out_count = 0;
    if (start_slot < p->log_base_slot) start_slot = p->log_base_slot;

    size_t count = 0;
    for (size_t i = start_slot - p->log_base_slot; i < p->log_cap; i++) {
        if (p->log_has_value[i]) count++;
    }

    if (count == 0) return NULL;

    paxos_entry_t* suffix = malloc(count * sizeof(paxos_entry_t));
    if (!suffix) return NULL;

    size_t j = 0;
    for (size_t i = start_slot - p->log_base_slot; i < p->log_cap; i++) {
        if (p->log_has_value[i]) {
            suffix[j] = p->log[i];

            // Deep copy the payload to prevent Use-After-Free during async transmission
            if (p->log[i].data_len > 0) {
                suffix[j].data = malloc(p->log[i].data_len);
                memcpy(suffix[j].data, p->log[i].data, p->log[i].data_len);
            } else {
                suffix[j].data = NULL;
            }
            j++;
        }
    }

    *out_count = count;
    return suffix;
}

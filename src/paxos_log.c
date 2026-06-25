// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"
#include <stdlib.h>
#include <string.h>

bool paxos_entry_clone(paxos_entry_t* dst, const paxos_entry_t* src) {
    memset(dst, 0, sizeof(*dst));
    *dst = *src;
    dst->data = NULL;

    if (src->data_len == 0) return true;

    dst->data = malloc(src->data_len);
    if (!dst->data) return false;

    memcpy(dst->data, src->data, src->data_len);
    return true;
}

void paxos_entry_destroy(paxos_entry_t* e) {
    if (e && e->data) {
        free(e->data);
        e->data = NULL;
    }
}

bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len) {
    if (p->fatal_error || slot <= p->snapshot_index) return false;
    if (data_len > PAXOS_MAX_PAYLOAD_SIZE) return false;

    uint64_t target_idx = slot - p->log_base_slot;
    if (target_idx >= p->log_cap) {
        size_t new_cap = target_idx + 1024;
        paxos_log_slot_t* new_log = realloc(p->log, new_cap * sizeof(paxos_log_slot_t));
        if (!new_log) {
            p->fatal_error = true;
            return false;
        }
        memset(new_log + p->log_cap, 0, (new_cap - p->log_cap) * sizeof(paxos_log_slot_t));
        p->log = new_log;
        p->log_cap = new_cap;
    }

    uint8_t* new_payload = NULL;
    if (data_len > 0) {
        new_payload = malloc(data_len);
        if (!new_payload) { p->fatal_error = true; return false; }
        memcpy(new_payload, data, data_len);
    }

    if (p->log[target_idx].has_value) {
        paxos_entry_destroy(&p->log[target_idx].entry);
    }

    p->log[target_idx].entry.slot = slot;
    p->log[target_idx].entry.accepted_ballot = ballot;
    p->log[target_idx].entry.type = type;
    p->log[target_idx].entry.client_id = cid;
    p->log[target_idx].entry.client_seq = cseq;
    p->log[target_idx].entry.data = new_payload;
    p->log[target_idx].entry.data_len = data_len;

    p->log[target_idx].has_value = true;
    p->log[target_idx].unstable = true;

    return true;
}

paxos_entry_t* paxos_log_get(paxos_t* p, uint64_t slot) {
    if (slot <= p->snapshot_index || slot < p->log_base_slot) return NULL;
    uint64_t target_idx = slot - p->log_base_slot;
    if (target_idx >= p->log_cap || !p->log[target_idx].has_value) return NULL;
    return &p->log[target_idx].entry;
}

paxos_entry_t* paxos_log_extract_unstable(paxos_t* p, size_t* out_count) {
    *out_count = 0;
    size_t count = 0;
    for (size_t i = 0; i < p->log_cap; i++) {
        if (p->log[i].has_value && p->log[i].unstable) count++;
    }

    if (count == 0) return NULL;

    paxos_entry_t* unstable_arr = calloc(count, sizeof(paxos_entry_t));
    if (!unstable_arr) { p->fatal_error = true; return NULL; }

    size_t j = 0;
    for (size_t i = 0; i < p->log_cap; i++) {
        if (p->log[i].has_value && p->log[i].unstable) {
            if (!paxos_entry_clone(&unstable_arr[j], &p->log[i].entry)) {
                for(size_t k = 0; k < j; k++) paxos_entry_destroy(&unstable_arr[k]);
                free(unstable_arr);
                p->fatal_error = true;
                return NULL;
            }
            j++;
        }
    }

    *out_count = count;
    return unstable_arr;
}

paxos_entry_t* paxos_log_extract_suffix(paxos_t* p, uint64_t start_slot, size_t* out_count) {
    *out_count = 0;
    if (start_slot < p->log_base_slot) start_slot = p->log_base_slot;

    size_t count = 0;
    for (size_t i = start_slot - p->log_base_slot; i < p->log_cap; i++) {
        if (p->log[i].has_value) count++;
    }

    if (count == 0) return NULL;

    paxos_entry_t* suffix = calloc(count, sizeof(paxos_entry_t));
    if (!suffix) { p->fatal_error = true; return NULL; }

    size_t j = 0;
    for (size_t i = start_slot - p->log_base_slot; i < p->log_cap; i++) {
        if (p->log[i].has_value) {
            if (!paxos_entry_clone(&suffix[j], &p->log[i].entry)) {
                for(size_t k = 0; k < j; k++) paxos_entry_destroy(&suffix[k]);
                free(suffix);
                p->fatal_error = true;
                return NULL;
            }
            j++;
        }
    }
    *out_count = count;
    return suffix;
}

paxos_entry_t* paxos_log_extract_range(paxos_t* p, uint64_t start_slot, uint64_t end_slot, size_t* out_count) {
    *out_count = 0;
    if (start_slot < p->log_base_slot) start_slot = p->log_base_slot;
    if (end_slot < start_slot) return NULL;

    size_t count = 0;
    for (uint64_t s = start_slot; s <= end_slot; s++) {
        uint64_t target_idx = s - p->log_base_slot;
        if (target_idx < p->log_cap && p->log[target_idx].has_value) count++;
    }

    if (count == 0) return NULL;

    paxos_entry_t* range = calloc(count, sizeof(paxos_entry_t));
    if (!range) { p->fatal_error = true; return NULL; }

    size_t j = 0;
    for (uint64_t s = start_slot; s <= end_slot; s++) {
        uint64_t target_idx = s - p->log_base_slot;
        if (target_idx < p->log_cap && p->log[target_idx].has_value) {
            if (!paxos_entry_clone(&range[j], &p->log[target_idx].entry)) {
                for(size_t k = 0; k < j; k++) paxos_entry_destroy(&range[k]);
                free(range);
                p->fatal_error = true;
                return NULL;
            }
            j++;
        }
    }
    *out_count = count;
    return range;
}

void paxos_advance_local_commit(paxos_t* p) {
    while (p->local_commit_index < p->leader_commit_hint) {
        uint64_t check_slot = p->local_commit_index + 1;
        paxos_entry_t* e = paxos_log_get(p, check_slot);

        if (!e) break;

        // Stale Commit Guard: Never blindly apply un-epoched data
        if (e->accepted_ballot != p->promised_ballot) {
            paxos_msg_t fetch = {
                .type = MSG_FETCH_ENTRIES,
                .to = p->leader_id,
                .slot = check_slot,
                .commit_index = check_slot
            };
            paxos_send_immediate(p, fetch);
            break;
        }

        if (e->type == ENTRY_CONF_ADD || e->type == ENTRY_CONF_REMOVE) {
            if (e->data_len == sizeof(uint64_t)) {
                uint64_t target_node;
                memcpy(&target_node, e->data, sizeof(uint64_t));

                if (e->type == ENTRY_CONF_ADD) {
                    bool exists = (p->id == target_node);
                    for (size_t i = 0; i < p->num_peers; i++) {
                        if (p->peers[i] == target_node) exists = true;
                    }
                    if (!exists && p->num_peers < MAX_REMOTE_PEERS) {
                        p->peers[p->num_peers++] = target_node;
                    }
                } else if (e->type == ENTRY_CONF_REMOVE) {
                    if (p->id == target_node) {
                        p->state = PAXOS_STATE_LEARNER;
                        p->leader_id = 0;
                    } else {
                        for (size_t i = 0; i < p->num_peers; i++) {
                            if (p->peers[i] == target_node) {
                                p->peers[i] = p->peers[p->num_peers - 1];
                                p->num_peers--;
                                break;
                            }
                        }
                    }
                }
            }
        }

        p->local_commit_index++;
    }
}

void paxos_compact(paxos_t* p, uint64_t compact_slot) {
    if (p->fatal_error || compact_slot <= p->snapshot_index || compact_slot > p->last_applied) return;
    if (compact_slot >= p->log_base_slot + p->log_cap) return;

    uint64_t target_idx = compact_slot - p->log_base_slot;
    for (uint64_t s = p->log_base_slot; s <= compact_slot; s++) {
        uint64_t idx = s - p->log_base_slot;
        if (idx < p->log_cap && p->log[idx].has_value) {
            paxos_entry_destroy(&p->log[idx].entry);
            p->log[idx].has_value = false;
        }
    }

    size_t shift_count = target_idx + 1;
    size_t keep_count = p->log_cap - shift_count;
    if (keep_count > 0) memmove(p->log, &p->log[shift_count], keep_count * sizeof(paxos_log_slot_t));
    memset(&p->log[keep_count], 0, shift_count * sizeof(paxos_log_slot_t));

    p->log_base_slot = compact_slot + 1;
    p->snapshot_index = compact_slot;
    p->snapshot_ballot = p->active_ballot;

    if (p->stable_accepted_through < compact_slot) p->stable_accepted_through = compact_slot;
}

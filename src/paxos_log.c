// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

bool paxos_entry_clone_deep(paxos_entry_t* dst, const paxos_entry_t* src) {
    *dst = *src;
    dst->data = paxos_payload_alloc(src->data, src->data_len);
    if (src->data_len > 0 && !dst->data) return false;
    return true;
}

bool paxos_entry_clone_retain(paxos_entry_t* dst, const paxos_entry_t* src) {
    *dst = *src;
    paxos_payload_retain(dst->data);
    return true;
}

void paxos_entry_destroy(paxos_entry_t* e) {
    if (e) {
        paxos_payload_release(e->data);
        e->data = NULL;
    }
}

void paxos_rebuild_config(paxos_t* p) {
    p->active_config_mask = p->base_config_mask;
    p->joint_config_mask = 0;
    p->in_joint_consensus = false;

    for (size_t c = 0; c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            paxos_log_slot_t* slot_data = &p->log_chunks[c]->slots[o];

            if (!slot_data->has_value || !slot_data->chosen) continue;

            paxos_entry_t* e = &slot_data->entry;

            if (e->type == ENTRY_CONF_ADD && e->data_len == sizeof(uint64_t)) {
                uint64_t target_node; memcpy(&target_node, e->data, sizeof(uint64_t));
                p->active_config_mask |= paxos_peer_bit(p, target_node);
            } else if (e->type == ENTRY_CONF_REMOVE && e->data_len == sizeof(uint64_t)) {
                uint64_t target_node; memcpy(&target_node, e->data, sizeof(uint64_t));
                p->active_config_mask &= ~paxos_peer_bit(p, target_node);
            } else if (e->type == ENTRY_CONF_JOINT) {
                uint64_t new_mask = 0; uint64_t* new_nodes = (uint64_t*)e->data;
                size_t count = e->data_len / sizeof(uint64_t);
                for(size_t i = 0; i < count; i++) new_mask |= paxos_peer_bit(p, new_nodes[i]);
                p->joint_config_mask = new_mask;
                p->in_joint_consensus = true;
            } else if (e->type == ENTRY_CONF_FINAL) {
                 p->active_config_mask = p->joint_config_mask;
                 p->in_joint_consensus = false;
            }
        }
    }
}

bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len) {
    if (p->fatal_error || slot <= p->snapshot_index || slot < p->log_base_slot) return false;
    if (data_len > PAXOS_MAX_PAYLOAD_SIZE) return false;
    if (data_len > 0 && !data) return false;

    uint64_t c_idx = paxos_chunk_idx(p, slot);
    uint64_t c_off = paxos_chunk_off(slot);

    if (c_idx >= p->log_chunks_cap) {
        size_t new_cap = c_idx + 128;
        paxos_log_chunk_t** new_chunks = realloc(p->log_chunks, new_cap * sizeof(paxos_log_chunk_t*));
        if (!new_chunks) { p->fatal_error = true; return false; }
        memset(new_chunks + p->log_chunks_cap, 0, (new_cap - p->log_chunks_cap) * sizeof(paxos_log_chunk_t*));
        p->log_chunks = new_chunks;
        p->log_chunks_cap = new_cap;
    }

    if (!p->log_chunks[c_idx]) {
        p->log_chunks[c_idx] = calloc(1, sizeof(paxos_log_chunk_t));
        if (!p->log_chunks[c_idx]) { p->fatal_error = true; return false; }
    }

    paxos_log_slot_t* slot_data = &p->log_chunks[c_idx]->slots[c_off];

    bool was_chosen = slot_data->chosen;
    paxos_entry_t old_entry = slot_data->entry;
    bool had_value = slot_data->has_value;

    if (had_value && was_chosen) {
        paxos_entry_t temp_in = { .type = type, .client_id = cid, .client_seq = cseq, .data = (uint8_t*)data, .data_len = data_len };
        if (!paxos_entry_value_equal(&old_entry, &temp_in)) {
            p->fatal_error = true;
            return false;
        }
    }

    uint8_t* new_payload = NULL;
    if (data_len > 0) {
        new_payload = paxos_payload_alloc(data, data_len);
        if (!new_payload) { p->fatal_error = true; return false; }
    }

    slot_data->entry.slot = slot;
    slot_data->entry.accepted_ballot = ballot;
    slot_data->entry.type = type;
    slot_data->entry.client_id = cid;
    slot_data->entry.client_seq = cseq;
    slot_data->entry.data_len = data_len;
    slot_data->entry.data = new_payload;

    slot_data->has_value = true;
    slot_data->unstable = true;
    slot_data->chosen = was_chosen;

    if (had_value) paxos_entry_destroy(&old_entry);

    return true;
}

paxos_entry_t* paxos_log_get(paxos_t* p, uint64_t slot) {
    if (slot <= p->snapshot_index || slot < p->log_base_slot) return NULL;
    uint64_t c_idx = paxos_chunk_idx(p, slot);
    uint64_t c_off = paxos_chunk_off(slot);
    if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx] || !p->log_chunks[c_idx]->slots[c_off].has_value) return NULL;
    return &p->log_chunks[c_idx]->slots[c_off].entry;
}

paxos_entry_t* paxos_log_extract_unstable(paxos_t* p, size_t* out_count) {
    *out_count = 0;
    size_t count = 0;
    for (size_t c = 0; c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            if (p->log_chunks[c]->slots[o].has_value && p->log_chunks[c]->slots[o].unstable) count++;
        }
    }
    if (count == 0) return NULL;

    paxos_entry_t* unstable_arr = calloc(count, sizeof(paxos_entry_t));
    if (!unstable_arr) { p->fatal_error = true; return NULL; }

    size_t j = 0;
    for (size_t c = 0; c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            if (p->log_chunks[c]->slots[o].has_value && p->log_chunks[c]->slots[o].unstable) {
                if (!paxos_entry_clone_retain(&unstable_arr[j++], &p->log_chunks[c]->slots[o].entry)) {
                    p->fatal_error = true; return NULL;
                }
            }
        }
    }
    *out_count = count;
    return unstable_arr;
}

paxos_entry_t* paxos_log_extract_suffix(paxos_t* p, uint64_t start_slot, size_t* out_count) {
    *out_count = 0;
    if (start_slot < p->log_base_slot) start_slot = p->log_base_slot;
    uint64_t start_c = paxos_chunk_idx(p, start_slot);

    size_t count = 0;
    for (size_t c = start_c; c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            paxos_log_slot_t* slot_data = &p->log_chunks[c]->slots[o];
            if (slot_data->has_value && slot_data->entry.slot >= start_slot) count++;
        }
    }
    if (count == 0) return NULL;

    paxos_entry_t* suffix = calloc(count, sizeof(paxos_entry_t));
    if (!suffix) { p->fatal_error = true; return NULL; }

    size_t j = 0;
    for (size_t c = start_c; c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            paxos_log_slot_t* slot_data = &p->log_chunks[c]->slots[o];
            if (slot_data->has_value && slot_data->entry.slot >= start_slot) {
                if (!paxos_entry_clone_retain(&suffix[j++], &slot_data->entry)) { p->fatal_error = true; return NULL; }
            }
        }
    }
    *out_count = count;
    return suffix;
}

paxos_entry_t* paxos_log_extract_range(paxos_t* p, uint64_t start_slot, uint64_t end_slot, size_t* out_count) {
    *out_count = 0;
    if (start_slot < p->log_base_slot) start_slot = p->log_base_slot;
    if (end_slot < start_slot) return NULL;

    uint64_t start_c = paxos_chunk_idx(p, start_slot);
    uint64_t end_c = paxos_chunk_idx(p, end_slot);
    if (end_c >= p->log_chunks_cap) end_c = p->log_chunks_cap - 1;

    size_t count = 0;
    for (size_t c = start_c; c <= end_c; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            paxos_log_slot_t* s_data = &p->log_chunks[c]->slots[o];
            if (s_data->has_value && s_data->entry.slot >= start_slot && s_data->entry.slot <= end_slot) count++;
        }
    }
    if (count == 0) return NULL;

    paxos_entry_t* range = calloc(count, sizeof(paxos_entry_t));
    if (!range) { p->fatal_error = true; return NULL; }

    size_t j = 0;
    for (size_t c = start_c; c <= end_c; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            paxos_log_slot_t* s_data = &p->log_chunks[c]->slots[o];
            if (s_data->has_value && s_data->entry.slot >= start_slot && s_data->entry.slot <= end_slot) {
                if (!paxos_entry_clone_retain(&range[j++], &s_data->entry)) { p->fatal_error = true; return NULL; }
            }
        }
    }
    *out_count = count;
    return range;
}

void paxos_advance_local_commit(paxos_t* p, uint64_t author_id, uint64_t author_ballot) {
    while (p->local_commit_index < p->leader_commit_hint) {
        uint64_t check_slot = p->local_commit_index + 1;
        if (check_slot < p->log_base_slot) break;

        uint64_t c_idx = paxos_chunk_idx(p, check_slot);
        uint64_t c_off = paxos_chunk_off(check_slot);

        if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx] || !p->log_chunks[c_idx]->slots[c_off].has_value) break;

        paxos_log_slot_t* slot_data = &p->log_chunks[c_idx]->slots[c_off];

        if (author_id == p->leader_id && author_ballot == p->promised_ballot && slot_data->entry.accepted_ballot == author_ballot) {
            slot_data->chosen = true;
            if (slot_data->entry.type >= ENTRY_CONF_ADD && slot_data->entry.type <= ENTRY_CONF_FINAL) paxos_rebuild_config(p);
        }

        if (!slot_data->chosen) break;

        if (slot_data->entry.type == ENTRY_CONF_FINAL || slot_data->entry.type == ENTRY_CONF_REMOVE) {
            if (!(p->active_config_mask & paxos_peer_bit(p, p->id))) {
                p->state = PAXOS_STATE_LEARNER;
                p->leader_id = 0;
            }
        }
        p->local_commit_index++;
    }
}

void paxos_compact(paxos_t* p, uint64_t compact_slot) {
    if (p->fatal_error || compact_slot <= p->snapshot_index || compact_slot > p->last_applied || compact_slot < p->log_base_slot) return;

    uint64_t end_c = paxos_chunk_idx(p, compact_slot);

    for (size_t c = 0; c <= end_c && c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            paxos_log_slot_t* s_data = &p->log_chunks[c]->slots[o];
            if (!s_data->has_value || s_data->entry.slot > compact_slot) continue;

            paxos_entry_t* e = &s_data->entry;
            if (e->type == ENTRY_CONF_ADD && e->data_len == sizeof(uint64_t)) {
                uint64_t target; memcpy(&target, e->data, sizeof(uint64_t));
                p->base_config_mask |= paxos_peer_bit(p, target);
            } else if (e->type == ENTRY_CONF_REMOVE && e->data_len == sizeof(uint64_t)) {
                uint64_t target; memcpy(&target, e->data, sizeof(uint64_t));
                p->base_config_mask &= ~paxos_peer_bit(p, target);
            } else if (e->type == ENTRY_CONF_FINAL) {
                p->base_config_mask = p->joint_config_mask;
            }
        }
    }

    for (size_t c = 0; c <= end_c && c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_LOG_CHUNK_SIZE; o++) {
            paxos_log_slot_t* s_data = &p->log_chunks[c]->slots[o];
            if (s_data->has_value && s_data->entry.slot <= compact_slot) {
                paxos_entry_destroy(&s_data->entry);
                s_data->has_value = false;
            }
        }
    }

    // FAANG: Safely shift the chunk array so relative indexing is preserved!
    uint64_t chunks_to_shift = end_c;
    for (size_t c = 0; c < chunks_to_shift && c < p->log_chunks_cap; c++) {
        if (p->log_chunks[c]) {
            free(p->log_chunks[c]);
            p->log_chunks[c] = NULL;
        }
    }

    if (chunks_to_shift > 0 && chunks_to_shift < p->log_chunks_cap) {
        size_t keep = p->log_chunks_cap - chunks_to_shift;
        memmove(p->log_chunks, p->log_chunks + chunks_to_shift, keep * sizeof(paxos_log_chunk_t*));
        memset(p->log_chunks + keep, 0, chunks_to_shift * sizeof(paxos_log_chunk_t*));
    }

    p->log_base_slot = compact_slot + 1;
    p->snapshot_index = compact_slot;
    p->snapshot_ballot = p->active_ballot;
    if (p->stable_accepted_through < compact_slot) p->stable_accepted_through = compact_slot;

    paxos_rebuild_config(p);
}

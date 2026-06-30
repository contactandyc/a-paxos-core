// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "paxos_internal.h"

bool paxos_entry_clone_deep(paxos_entry_t* dst, const paxos_entry_t* src) {
    *dst = *src;
    if (src->data_len > 0 && src->data) {
        dst->data = paxos_payload_alloc(src->data, src->data_len);
        if (!dst->data) return false;
    }
    return true;
}

bool paxos_entry_clone_retain(paxos_entry_t* dst, const paxos_entry_t* src) {
    *dst = *src;
    paxos_payload_retain(dst->data);
    return true;
}

void paxos_rebuild_config(paxos_t* p) {
#if PAXOS_ENABLE_RECONFIG
    p->active_config_mask = p->base_config_mask;
    p->joint_config_mask = 0;
    p->in_joint_consensus = false;
    p->pending_reconfig = false;

    uint64_t start_slot = p->snapshot_index + 1;
    for (uint64_t i = start_slot; i <= p->local_commit_index; i++) {
        uint64_t c_idx = paxos_chunk_idx(p, i);
        uint64_t c_off = paxos_chunk_off(i);

        if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx]) break;

        paxos_log_slot_t* slot_data = &p->log_chunks[c_idx]->slots[c_off];
        if (!slot_data->is_chosen) break;

        paxos_entry_t* e = &slot_data->chosen_entry;

        if (e->type == PAXOS_ENTRY_CONF_ADD && e->data_len == sizeof(uint64_t)) {
            uint64_t target_node; memcpy(&target_node, e->data, sizeof(uint64_t));
            p->active_config_mask |= paxos_peer_register(p, target_node);
        } else if (e->type == PAXOS_ENTRY_CONF_REMOVE && e->data_len == sizeof(uint64_t)) {
            uint64_t target_node; memcpy(&target_node, e->data, sizeof(uint64_t));
            p->active_config_mask &= ~paxos_peer_bit(p, target_node);
            paxos_peer_deregister(p, target_node);
        } else if (e->type == PAXOS_ENTRY_CONF_JOINT) {
            uint64_t new_mask = 0; uint64_t* new_nodes = (uint64_t*)e->data;
            size_t count = e->data_len / sizeof(uint64_t);
            for(size_t j = 0; j < count; j++) new_mask |= paxos_peer_register(p, new_nodes[j]);
            p->joint_config_mask = new_mask;
            p->in_joint_consensus = true;
            p->pending_reconfig = true;
        } else if (e->type == PAXOS_ENTRY_CONF_FINAL) {
             p->active_config_mask = p->joint_config_mask;
             p->in_joint_consensus = false;
             p->pending_reconfig = false;
        }
    }
#endif
}

bool paxos_log_accept(paxos_t* p, uint64_t slot, uint64_t ballot, paxos_entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len) {
    if (p->fatal_error || slot <= p->snapshot_index || slot < p->log_base_slot) return false;
    if (data_len > p->max_payload_size) return false;
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

    paxos_log_slot_t* s = &p->log_chunks[c_idx]->slots[c_off];
    paxos_entry_t temp = { .type = type, .client_id = cid, .client_seq = cseq, .data = (uint8_t*)data, .data_len = data_len };

    if (s->is_chosen) {
        if (!paxos_entry_value_equal(&s->chosen_entry, &temp)) {
            p->fatal_error = true;
            return false;
        }

        if (ballot > s->accepted_entry.accepted_ballot) {
            if (s->has_accepted) paxos_entry_destroy(&s->accepted_entry);
            s->accepted_entry = s->chosen_entry;
            s->accepted_entry.accepted_ballot = ballot;
            paxos_payload_retain(s->accepted_entry.data);
            s->has_accepted = true;
        }
        return true;
    }

    if (s->has_accepted && paxos_entry_value_equal(&s->accepted_entry, &temp)) {
        if (ballot > s->accepted_entry.accepted_ballot) {
            s->accepted_entry.accepted_ballot = ballot;
            if (!s->unstable) {
                if (p->num_unstable_slots >= p->unstable_slots_cap) {
                    size_t new_cap = p->unstable_slots_cap == 0 ? 1024 : p->unstable_slots_cap * 2;
                    uint64_t* new_vec = realloc(p->unstable_slots, new_cap * sizeof(uint64_t));
                    if (!new_vec) { p->fatal_error = true; return false; }
                    p->unstable_slots = new_vec;
                    p->unstable_slots_cap = new_cap;
                }
                p->unstable_slots[p->num_unstable_slots++] = slot;
            }
            s->unstable = true;
        }
        return true;
    }

    uint8_t* new_payload = NULL;
    if (data_len > 0) {
        new_payload = paxos_payload_alloc(data, data_len);
        if (!new_payload) {
            p->fatal_error = true;
            return false;
        }
    }

    paxos_entry_t old_entry = s->accepted_entry;
    bool had_old = s->has_accepted;

    s->accepted_entry.slot = slot;
    s->accepted_entry.accepted_ballot = ballot;
    s->accepted_entry.type = type;
    s->accepted_entry.client_id = cid;
    s->accepted_entry.client_seq = cseq;
    s->accepted_entry.data_len = data_len;
    s->accepted_entry.data = new_payload;

    s->has_accepted = true;

    if (!s->unstable) {
        if (p->num_unstable_slots >= p->unstable_slots_cap) {
            size_t new_cap = p->unstable_slots_cap == 0 ? 1024 : p->unstable_slots_cap * 2;
            uint64_t* new_vec = realloc(p->unstable_slots, new_cap * sizeof(uint64_t));
            if (!new_vec) { p->fatal_error = true; return false; }
            p->unstable_slots = new_vec;
            p->unstable_slots_cap = new_cap;
        }
        p->unstable_slots[p->num_unstable_slots++] = slot;
    }
    s->unstable = true;

    if (had_old) paxos_entry_destroy(&old_entry);
    if (slot > p->highest_slot) p->highest_slot = slot;

    return true;
}

bool paxos_log_learn_chosen(paxos_t* p, uint64_t slot, const paxos_entry_t* entry) {
    if (slot <= p->snapshot_index) return true;
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

    paxos_log_slot_t* s = &p->log_chunks[c_idx]->slots[c_off];

    if (s->is_chosen) {
        if (!paxos_entry_value_equal(&s->chosen_entry, entry)) p->fatal_error = true;
        return true;
    }

    paxos_entry_t temp_clone;
    if (!paxos_entry_clone_deep(&temp_clone, entry)) {
        p->fatal_error = true;
        return false;
    }

    s->chosen_entry = temp_clone;
    s->is_chosen = true;

    if (!s->unstable) {
        if (p->num_unstable_slots >= p->unstable_slots_cap) {
            size_t new_cap = p->unstable_slots_cap == 0 ? 1024 : p->unstable_slots_cap * 2;
            uint64_t* new_vec = realloc(p->unstable_slots, new_cap * sizeof(uint64_t));
            if (!new_vec) { p->fatal_error = true; return false; }
            p->unstable_slots = new_vec;
            p->unstable_slots_cap = new_cap;
        }
        p->unstable_slots[p->num_unstable_slots++] = slot;
    }
    s->unstable = true;

    if (slot > p->highest_slot) p->highest_slot = slot;
    return true;
}

paxos_entry_t* paxos_log_get(paxos_t* p, uint64_t slot) {
    uint64_t c_idx = paxos_chunk_idx(p, slot);
    uint64_t c_off = paxos_chunk_off(slot);
    if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx]) return NULL;

    paxos_log_slot_t* s = &p->log_chunks[c_idx]->slots[c_off];
    if (s->is_chosen) return &s->chosen_entry;
    if (s->has_accepted) return &s->accepted_entry;
    return NULL;
}

paxos_entry_t* paxos_log_get_accepted(paxos_t* p, uint64_t slot) {
    uint64_t c_idx = paxos_chunk_idx(p, slot);
    uint64_t c_off = paxos_chunk_off(slot);
    if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx]) return NULL;

    paxos_log_slot_t* s = &p->log_chunks[c_idx]->slots[c_off];
    return s->has_accepted ? &s->accepted_entry : NULL;
}

paxos_entry_t* paxos_log_extract_unstable(paxos_t* p, size_t* out_count) {
    if (p->num_unstable_slots == 0) { *out_count = 0; return NULL; }
    paxos_entry_t* arr = calloc(p->num_unstable_slots, sizeof(paxos_entry_t));
    if (!arr) { p->fatal_error = true; return NULL; }

    size_t valid = 0;
    for (size_t i = 0; i < p->num_unstable_slots; i++) {
        uint64_t slot = p->unstable_slots[i];
        paxos_entry_t* e = paxos_log_get(p, slot);
        if (e && paxos_entry_clone_retain(&arr[valid], e)) {
            valid++;
        }
    }
    *out_count = valid;
    return arr;
}

paxos_entry_t* paxos_log_extract_range(paxos_t* p, uint64_t start_slot, uint64_t end_slot, size_t* out_count) {
    *out_count = 0;
    if (start_slot > end_slot || start_slot <= p->snapshot_index || end_slot > p->highest_slot) return NULL;

    size_t cap = end_slot - start_slot + 1;
    paxos_entry_t* arr = calloc(cap, sizeof(paxos_entry_t));
    if (!arr) { p->fatal_error = true; return NULL; }

    size_t count = 0;
    for (uint64_t i = start_slot; i <= end_slot; i++) {
        paxos_entry_t* e = paxos_log_get(p, i);
        if (e) {
            paxos_entry_clone_retain(&arr[count++], e);
        } else {
            for (size_t j = 0; j < count; j++) paxos_entry_destroy(&arr[j]);
            free(arr);
            return NULL;
        }
    }
    *out_count = count;
    return arr;
}

paxos_entry_t* paxos_log_extract_suffix(paxos_t* p, uint64_t start_slot, size_t* out_count) {
    if (start_slot > p->highest_slot) { *out_count = 0; return NULL; }
    return paxos_log_extract_range(p, start_slot, p->highest_slot, out_count);
}

void paxos_compact(paxos_t* p, uint64_t up_to_slot) {
    if (p->fatal_error || up_to_slot <= p->log_base_slot || up_to_slot > p->last_applied) return;

    uint64_t old_base = p->log_base_slot;
    uint64_t new_base = up_to_slot + 1;

    uint64_t old_base_c = old_base / PAXOS_INTERNAL_LOG_CHUNK_SIZE;
    uint64_t new_base_c = new_base / PAXOS_INTERNAL_LOG_CHUNK_SIZE;
    uint64_t shift_chunks = new_base_c > old_base_c ? new_base_c - old_base_c : 0;
    if (shift_chunks > p->log_chunks_cap) shift_chunks = p->log_chunks_cap;

    uint64_t start_c = paxos_chunk_idx(p, old_base);
    uint64_t end_c = paxos_chunk_idx(p, up_to_slot);

    for (size_t c = start_c; c <= end_c && c < p->log_chunks_cap; c++) {
        if (!p->log_chunks[c]) continue;
        for (size_t o = 0; o < PAXOS_INTERNAL_LOG_CHUNK_SIZE; o++) {
            paxos_log_slot_t* s_data = &p->log_chunks[c]->slots[o];
            if (!s_data->is_chosen || s_data->chosen_entry.slot < old_base || s_data->chosen_entry.slot > up_to_slot) continue;

#if PAXOS_ENABLE_RECONFIG
            paxos_entry_t* e = &s_data->chosen_entry;
            if (e->type == PAXOS_ENTRY_CONF_ADD && e->data_len == sizeof(uint64_t)) {
                uint64_t target; memcpy(&target, e->data, sizeof(uint64_t));
                p->base_config_mask |= paxos_peer_register(p, target);
            } else if (e->type == PAXOS_ENTRY_CONF_REMOVE && e->data_len == sizeof(uint64_t)) {
                uint64_t target; memcpy(&target, e->data, sizeof(uint64_t));
                p->base_config_mask &= ~paxos_peer_bit(p, target);
            } else if (e->type == PAXOS_ENTRY_CONF_FINAL) {
                p->base_config_mask = p->joint_config_mask;
            }
#endif
        }
    }

    if (shift_chunks > 0) {
        for (size_t i = 0; i < shift_chunks; i++) {
            if (p->log_chunks[i]) {
                for (size_t j = 0; j < PAXOS_INTERNAL_LOG_CHUNK_SIZE; j++) {
                    if (p->log_chunks[i]->slots[j].has_accepted) paxos_entry_destroy(&p->log_chunks[i]->slots[j].accepted_entry);
                    if (p->log_chunks[i]->slots[j].is_chosen) paxos_entry_destroy(&p->log_chunks[i]->slots[j].chosen_entry);
                }
                free(p->log_chunks[i]);
            }
        }

        size_t keep = p->log_chunks_cap - shift_chunks;
        memmove(p->log_chunks, p->log_chunks + shift_chunks, keep * sizeof(paxos_log_chunk_t*));
        memset(p->log_chunks + keep, 0, shift_chunks * sizeof(paxos_log_chunk_t*));
    } else {
        uint64_t c_idx = 0;
        uint64_t start_off = paxos_chunk_off(old_base);
        uint64_t end_off = paxos_chunk_off(up_to_slot);

        if (p->log_chunks[c_idx]) {
            for (uint64_t j = start_off; j <= end_off; j++) {
                if (p->log_chunks[c_idx]->slots[j].has_accepted) {
                    paxos_entry_destroy(&p->log_chunks[c_idx]->slots[j].accepted_entry);
                    p->log_chunks[c_idx]->slots[j].has_accepted = false;
                }
                if (p->log_chunks[c_idx]->slots[j].is_chosen) {
                    paxos_entry_destroy(&p->log_chunks[c_idx]->slots[j].chosen_entry);
                    p->log_chunks[c_idx]->slots[j].is_chosen = false;
                }
            }
        }
    }

    p->log_base_slot = new_base;
    p->snapshot_index = up_to_slot;
    p->snapshot_ballot = p->active_ballot;
    if (p->stable_accepted_through < up_to_slot) p->stable_accepted_through = up_to_slot;
    if (up_to_slot > p->highest_slot) p->highest_slot = up_to_slot;

    paxos_rebuild_config(p);
}

void paxos_advance_local_commit(paxos_t* p, uint64_t author_id, uint64_t author_ballot) {
    (void)author_id;
    (void)author_ballot;

    while (p->local_commit_index < p->leader_commit_hint) {
        uint64_t check_slot = p->local_commit_index + 1;
        if (check_slot < p->log_base_slot) break;

        uint64_t c_idx = paxos_chunk_idx(p, check_slot);
        uint64_t c_off = paxos_chunk_off(check_slot);
        if (c_idx >= p->log_chunks_cap || !p->log_chunks[c_idx]) break;

        paxos_log_slot_t* s = &p->log_chunks[c_idx]->slots[c_off];

        if (!s->is_chosen) break;

        p->local_commit_index++;

#if PAXOS_ENABLE_RECONFIG
        if (s->chosen_entry.type >= PAXOS_ENTRY_CONF_ADD && s->chosen_entry.type <= PAXOS_ENTRY_CONF_FINAL) {
            paxos_rebuild_config(p);
            if (s->chosen_entry.type == PAXOS_ENTRY_CONF_JOINT && p->id == p->leader_id) {
                p->needs_conf_final = true;
            }
        }
#endif

        if (s->chosen_entry.type == PAXOS_ENTRY_CONF_FINAL || s->chosen_entry.type == PAXOS_ENTRY_CONF_REMOVE) {
            if (!(p->active_config_mask & paxos_peer_bit(p, p->id))) {
                p->state = PAXOS_STATE_LEARNER;
                p->leader_id = 0;
            }
        }
    }
}

paxos_err_t paxos_set_snapshot_chunk(paxos_t* p, uint64_t peer_id, const uint8_t* data, size_t len, uint64_t offset, bool done) {
    if (p->fatal_error || p->state != PAXOS_STATE_ACTIVE) return PAXOS_ERR_NOT_ACTIVE;

    size_t peer_idx = 0; bool found = false;
    for (size_t i = 0; i < PAXOS_MAX_PEERS; i++) {
        if (p->node_directory[i] == peer_id) { peer_idx = i; found = true; break; }
    }
    if (!found) return PAXOS_ERR_INVALID_ARG;

    if (p->snapshot_offset[peer_idx] != offset) return PAXOS_ERR_INVALID_ARG;

    uint8_t* chunk = NULL;
    if (len > 0 && data) {
        chunk = malloc(len);
        if (!chunk) { p->fatal_error = true; return PAXOS_ERR_NOMEM; }
        memcpy(chunk, data, len);
    }

    paxos_msg_t snap = {
        .type = PAXOS_MSG_INSTALL_SNAPSHOT,
        .to = peer_id,
        .ballot = p->active_ballot,
        .slot = p->snapshot_index,
        .snapshot_offset = offset,
        .snapshot_len = len,
        .snapshot_data = chunk,
        .snapshot_done = done
    };

    paxos_send_immediate(p, snap);
    return PAXOS_OK;
}

void paxos_snapshot_acked(paxos_t* p, bool success) {
    if (!p->pending_snapshot) return;

    p->pending_snapshot_chunk_ready = false;
    uint64_t next_offset = success ? p->pending_snapshot_offset + p->pending_snapshot_len : p->pending_snapshot_offset;
    if (!success) p->expected_snapshot_offset = p->pending_snapshot_offset;

    paxos_msg_t res = {
        .type = PAXOS_MSG_INSTALL_SNAPSHOT_RES,
        .to = p->pending_snapshot_from,
        .ballot = p->pending_snapshot_msg_ballot,
        .reject = !success,
        .slot = next_offset,
        .snapshot_done = p->pending_snapshot_done
    };

    if (success && p->pending_snapshot_done) {
        for (size_t c = 0; c < p->log_chunks_cap; c++) {
            if (!p->log_chunks[c]) continue;
            for (size_t o = 0; o < PAXOS_INTERNAL_LOG_CHUNK_SIZE; o++) {
                if (p->log_chunks[c]->slots[o].has_accepted) paxos_entry_destroy(&p->log_chunks[c]->slots[o].accepted_entry);
                if (p->log_chunks[c]->slots[o].is_chosen) paxos_entry_destroy(&p->log_chunks[c]->slots[o].chosen_entry);
                p->log_chunks[c]->slots[o].has_accepted = false;
                p->log_chunks[c]->slots[o].is_chosen = false;
            }
            free(p->log_chunks[c]);
            p->log_chunks[c] = NULL;
        }
        p->log_base_slot = p->pending_snapshot_msg_slot + 1;
        p->snapshot_index = p->pending_snapshot_msg_slot;
        p->last_applied = p->snapshot_index;
        p->local_commit_index = p->snapshot_index;
        p->leader_commit_hint = p->snapshot_index;
        paxos_rebuild_config(p);
    }

    paxos_send_immediate(p, res);

    if (p->pending_snapshot_data) free(p->pending_snapshot_data);
    p->pending_snapshot_data = NULL;
    p->pending_snapshot_len = 0;

    if (p->pending_snapshot_done || !success) {
        p->pending_snapshot = false;
        p->pending_snapshot_offset = 0;
        p->pending_snapshot_done = false;
    }
}

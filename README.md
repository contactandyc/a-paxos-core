# a-paxos-core

**A hyper-optimized, zero-copy Multi-Paxos consensus engine written in standard C.**

`a-paxos-core` is a deterministic, high-throughput consensus library designed for building distributed state machines and strongly consistent data stores. It is built to compete with enterprise-grade replication layers, bypassing standard abstractions in favor of direct hardware acceleration and strict memory management.

## 🚀 Architectural Highlights

* **Zero-Copy Intrusive Ref-Counting:** Payloads are allocated once with a hidden atomic reference counter. Outbound `MSG_ACCEPT` packets broadcast across the network by passing pointers, completely eliminating `malloc` and `memcpy` from the hot path.
* **O(1) Memory Compaction:** Instead of a flat array requiring massive `memmove` operations, the distributed log uses a 2D chunked sparse array. Compacting the log safely drops entire memory blocks in `O(1)` time.
* **Hardware-Accelerated Quorums:** Evaluates cluster consensus using 64-bit hardware popcounts (`__builtin_popcountll`). Quorum verifications are resolved in exactly two CPU clock cycles.
* **Joint Consensus (Alpha-Windows):** Safe, dynamic cluster topology reconfiguration without taking the database offline. Prevents split-brain by requiring overlapping majorities from both the old and new configurations during transit.
* **Validation Firewalls & Fetch Suppression:** Strict ballot validation drops rogue packets at the network boundary, and rate-limited catch-up syncs prevent followers from accidentally DoS'ing the leader during partition heals.
* **Deterministic Chaos Tested:** The core architecture is validated via a discrete-event simulator that mercilessly drops, duplicates, and delays packets across virtual nodes for thousands of cycles to mathematically prove the absence of deadlocks and split-brain.

## 🛠️ Build Instructions

The engine is built using CMake and Ninja. It has zero external dependencies other than the standard C library.

```bash
# Generate the build files
cmake -B build -G Ninja

# Build the core library and test suite
ninja -C build

# Run the strict validation suite and the Chaos Simulator
cd build && ctest --output-on-failure

```

## 🧠 The "Ready" Architecture

`a-paxos-core` is a pure state machine. It does not touch the network or the disk itself. Instead, it follows the **Ready Pattern** popularized by systems like etcd.

You feed the engine incoming network messages and local proposals, and then ask it what it is "Ready" to do.

### 1. Feed the Engine

```c
// Step remote messages received from the network
paxos_step_remote(p, &incoming_msg);

// Propose a new value to the cluster
paxos_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
paxos_step_local(p, &prop);

// Tick the engine (call this from a timer loop)
paxos_tick(p);

```

### 2. Process the Output

```c
paxos_ready_t ready = paxos_get_ready(p);

// 1. Save hard state (promised ballot) to disk if changed
if (ready.hard_state.has_update) { save_hard_state(ready.hard_state); }

// 2. Append unstable log entries to disk
if (ready.num_entries_to_save > 0) { append_to_disk(ready.entries_to_save); }

// 3. Send immediate network messages (e.g., NACKs, Tick, Fetches)
send_network(ready.messages_immediate);

// 4. Wait for disk fsync(), then send after-persist messages (e.g., Accepts)
fsync_disk();
send_network(ready.messages_after_persist);

// 5. Apply chosen entries to your application's State Machine
for (size_t i = 0; i < ready.num_chosen_entries; i++) {
    apply_to_state_machine(ready.chosen_entries[i]);
}

```

### 3. Advance the Engine

Once you have safely persisted the data and applied the chosen entries, you must advance the engine to release the reference counters and free the memory.

```c
// Tell the engine which slots are safely on disk, and what your state machine has applied
paxos_advance(p, stable_slots_array, num_stable_slots, highest_applied_slot);
paxos_ready_destroy(&ready);

```

## 📦 Snapshot Streaming

To prevent the log from growing infinitely, `a-paxos-core` supports coordinated snapshot streaming.

When a follower falls too far behind to catch up via memory, the core will generate an empty `MSG_INSTALL_SNAPSHOT` with the requested offset. The host application intercepts this message, reads the chunk from its persistent database snapshot, provides the bytes via `paxos_set_snapshot_chunk()`, and then fires the fully formed message over the network.

## ⚖️ License

SPDX-License-Identifier: Apache-2.0
Copyright 2026 Andy Curtis

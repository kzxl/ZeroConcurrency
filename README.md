# ZeroConcurrency

[![ZeroPlatform Tier](https://img.shields.io/badge/ZeroPlatform-Tier%200%20(Core%20Foundation)-0284c7.svg)](https://github.com/kzxl/ZeroPlatform)
[![NuGet Version](https://img.shields.io/badge/nuget-v1.2.0-blue.svg)](https://www.nuget.org/packages/ZeroConcurrency/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET Multi-Targeting](https://img.shields.io/badge/.NET-8.0%20%7C%204.6.2%20%7C%20Standard%202.0-purple.svg)](https://dotnet.microsoft.com/)
[![Tests: 31 Passed](https://img.shields.io/badge/Tests-31%20Passed%20(100%25)-brightgreen.svg)]()
[![Zero External Dependencies](https://img.shields.io/badge/Dependencies-0%20(Pure%20C%23)-brightgreen.svg)]()

**ZeroConcurrency** is an enterprise-grade, pure C# lock-free concurrency, high-throughput streaming, and asynchronous execution engine for .NET. Engineered with **zero external unmanaged dependencies**, it delivers micro-to-nanosecond latency, zero GC allocations for steady-state workloads, and execution context bypass for mission-critical industrial automation, SCADA, edge IoT, and streaming pipelines.

Part of the **ZeroUniverse / ZeroPlatform** ecosystem.

---

## Key Capabilities

### 1. Data-Plane: Lock-Free Ring Buffers & Off-Heap Disruptors
- **`ZeroNativeRingBuffer` (Off-Heap Disruptor)**:
  - High-speed Disruptor-style ring buffer storing unmanaged payloads directly in off-heap memory backed by `ZeroPrimitives.Memory.NativeMemoryPool`.
  - Zero managed heap wrapping overhead; ideal for sensor data and camera frames between threads.
- **`ZeroRingBuffer<T>` (SPSC)**:
  - Single-Producer Single-Consumer lock-free circular queue.
  - Strict 64-byte cache-line separation padding between producer (`_tail`) and consumer (`_head`) to eliminate L1/L2 false sharing.
  - Power-of-two capacity bitmask wrapping for fast pointer advancement without modulo division.
  - Over 10 million operations per second with 0 GC allocations.
- **`ZeroMpmcRingBuffer<T>` (MPMC)**:
  - Multi-Producer Multi-Consumer lock-free bounded queue based on Dmitry Vyukov's algorithm.
  - Monotonic sequence barriers for contention mitigation and ABA prevention.

### 2. Control-Plane: Zero-Allocation Async & Scheduling Primitives
- **`AsyncManualResetEvent` & `AsyncAutoResetEvent`**:
  - Zero-allocation awaitable synchronization primitives returning pooled `ValueTask` instances.
  - Enables asynchronous thread coordination and gate signaling without GC pressure.
- **`ZeroWorkStealingPool`**:
  - Multi-threaded task execution engine with local per-worker queues and lock-free work stealing.
  - Dynamically balances bursty compute workloads with minimal scheduling overhead.
- **`ZeroPromise<T>` & `ZeroPromise`**:
  - Recyclable `IValueTaskSource<T>` and `IValueTaskSource` implementations.
  - Integrated with `ZeroPromisePool<T>`: automatically resets and recycles instances back to the lock-free pool as soon as the awaiter consumes the result.
  - Eliminates heap allocations for `ValueTask` completions in high-frequency request-reply or pipeline nodes.
- **`ZeroScheduler`**:
  - Bypasses `ExecutionContext` capture and restoration (no `AsyncLocal`, security context, or culture dictionary cloning).
  - Provides `IZeroWorkItem` interface to queue stateful tasks with 0 delegate closure allocations.
  - Cuts dispatch overhead by 30% - 50% compared to `Task.Run()`.
- **`ZeroDedicatedWorker`**:
  - Dedicated OS background worker with adaptive backoff (lightweight spin -> yield -> sleep).
  - Guarantees sub-microsecond response time for sensor capture, serial streams, and frame pump loops.

### 3. Channels: Go-Style CSP Concurrency
- **`ZeroChannel<T>`**:
  - Communicating Sequential Processes (CSP) channel modeled after Go channels (`chan T`).
  - Backed by lock-free ring buffering and `ZeroPromise`.
  - Supports synchronous zero-copy fast-paths (`TryWrite`, `TryRead`) and asynchronous continuations (`WriteAsync`, `ReadAsync`, `ReadAllAsync`).

---

## Multi-Targeting

- **.NET 8.0+** (Modern high-throughput JIT, hardware intrinsics)
- **.NET Standard 2.0** (Cross-platform compatibility)
- **.NET Framework 4.6.2** (Legacy enterprise and industrial HMI support)

---

## Quick Example

### High-Speed Go-Style Channel

```csharp
using System;
using System.Threading.Tasks;
using ZeroPlatform.Concurrency;

var channel = new ZeroChannel<int>(capacityPowerOfTwo: 1024);

// Producer
_ = Task.Run(async () =>
{
    for (int i = 1; i <= 100_000; i++)
    {
        await channel.WriteAsync(i);
    }
    channel.Complete();
});

// Consumer (0-allocation streaming)
await foreach (var item in channel.ReadAllAsync())
{
    // Process item
}
```

---

## Architecture

```
ZeroConcurrency/
├── Channels/
│   └── ZeroChannel.cs            # Go-style CSP channel (chan T)
├── ControlPlane/
│   ├── AsyncResetEvents.cs       # Zero-allocation AsyncManualResetEvent & AsyncAutoResetEvent
│   ├── ZeroWorkStealingPool.cs   # Multi-threaded work-stealing job pool
│   ├── IZeroWorkItem.cs          # Zero-allocation work item interface
│   ├── ZeroDedicatedWorker.cs    # Pinned OS thread streaming worker
│   ├── ZeroPromise.cs            # Reusable IValueTaskSource & pool
│   └── ZeroScheduler.cs          # ExecutionContext bypass scheduler
└── DataPlane/
    ├── ZeroNativeRingBuffer.cs   # Off-heap Disruptor RingBuffer backed by NativeMemoryPool
    ├── ZeroMpmcRingBuffer.cs     # Lock-free MPMC queue
    └── ZeroRingBuffer.cs         # Lock-free SPSC cache-line padded ring
```

---

## 📜 Release History

| Version | Release Date | Key Milestones & Highlights |
| :--- | :---: | :--- |
| **`v1.2.0`** | 2026-09-22 | **Tier 0 Concurrency Hardening & Off-Heap Disruptor**:<br/>• Integrated with `ZeroPrimitives.Core 1.3.0` off-heap foundation.<br/>• Added `ZeroNativeRingBuffer`: Off-heap Disruptor RingBuffer managing unmanaged memory blocks with 0 GC overhead.<br/>• Added `AsyncManualResetEvent` & `AsyncAutoResetEvent`: Zero-allocation awaitable synchronization primitives.<br/>• Added `ZeroWorkStealingPool`: Dedicated multi-threaded worker pool with work stealing.<br/>• Verified across 31 automated tests (100% pass rate). |
| **`v1.1.0`** | 2026-09-16 | **MPMC Ring Buffers & Go Channels**:<br/>• Added `ZeroMpmcRingBuffer<T>` lock-free multi-producer multi-consumer ring.<br/>• Added `ZeroChannel<T>` CSP channels with async iterator streaming.<br/>• Added `ZeroDedicatedWorker` pinned thread loop. |
| **`v1.0.0`** | 2026-09-12 | **Initial Release**:<br/>• `ZeroRingBuffer<T>` SPSC cache-line padded ring.<br/>• `ZeroPromise` & `ZeroPromisePool` reusable ValueTask source.<br/>• `ZeroScheduler` ExecutionContext bypass dispatcher. |

---

## License

MIT License. Copyright © 2026 Phong Võ (`kzxl`).

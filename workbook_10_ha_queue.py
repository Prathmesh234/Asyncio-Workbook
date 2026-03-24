"""
================================================================================
WORKBOOK 10: HIGH-AVAILABILITY PRODUCER-CONSUMER QUEUE SYSTEM
================================================================================
Difficulty: Expert
Topics: System design, fault tolerance, distributed patterns, advanced asyncio

Prerequisites: Workbooks 01-09

Learning Objectives:
- Design a production-grade message queue system
- Implement fault tolerance and recovery
- Handle consumer failures and message redelivery
- Build monitoring and health checks
- Apply all asyncio concepts in a real-world scenario

This is the capstone project combining everything learned in previous workbooks.
================================================================================
"""

import asyncio
import time
import uuid
import random
from typing import Any, Optional, Callable, Dict, List
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
import json


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║           PRIMER: HIGH-AVAILABILITY PRODUCER-CONSUMER QUEUE                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
CAPSTONE PROJECT OVERVIEW
=========================
This workbook combines EVERYTHING from Workbooks 01-09 into a production-grade
message queue system. You'll implement a system similar to SQS, RabbitMQ, or
Redis queues.


WHAT IS A HIGH-AVAILABILITY QUEUE?
==================================
A message queue that:
1. NEVER loses messages (even if consumers crash)
2. Handles consumer failures gracefully
3. Provides exactly-once or at-least-once delivery
4. Scales with multiple producers and consumers
5. Monitors system health


KEY CONCEPTS TO IMPLEMENT:
==========================

1. MESSAGE LIFECYCLE:
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                                                                          │
    │   ENQUEUE ──► PENDING ──► PROCESSING ──► COMPLETED                      │
    │                              │                                           │
    │                              │ (consumer fails)                          │
    │                              ▼                                           │
    │                           FAILED ──► (retry) ──► PENDING                │
    │                              │                                           │
    │                              │ (max retries exceeded)                   │
    │                              ▼                                           │
    │                        DEAD_LETTER                                       │
    └─────────────────────────────────────────────────────────────────────────┘


2. VISIBILITY TIMEOUT:
   When a consumer takes a message, it becomes "invisible" to other consumers.
   If the consumer doesn't complete within the timeout, the message becomes
   visible again for another consumer.

    Consumer 1: dequeue() ──► message becomes invisible for 30s
                    │
                    │ (consumer crashes after 10s)
                    │
                    └──► After 30s, message visible again
                              │
    Consumer 2: ──────────────► dequeue() gets the message


3. DEAD LETTER QUEUE:
   Messages that fail too many times go to a separate queue for investigation.
   This prevents bad messages from blocking the system forever.


4. CONSUMER HEARTBEAT:
   Consumers send periodic heartbeats. If heartbeats stop, the consumer
   is considered dead, and its messages are recovered.

    Consumer ──► heartbeat ──► heartbeat ──► heartbeat ──► (crash)
                                                                │
    System detects: no heartbeat for 30s ──► recover messages ◄─┘


5. GRACEFUL SHUTDOWN:
   Stop accepting new messages, let in-flight messages complete,
   then shut down cleanly.


ARCHITECTURE:
=============
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                         HAQueueSystem                                   │
    │                                                                          │
    │  ┌────────────────────────────────────────────────────────────────────┐ │
    │  │                      HAMessageQueue                                 │ │
    │  │  - enqueue(message)      - Stores messages                         │ │
    │  │  - dequeue(consumer_id)  - Message lifecycle management            │ │
    │  │  - complete(msg_id)      - Dead letter queue                       │ │
    │  │  - fail(msg_id)          - Statistics                              │ │
    │  └────────────────────────────────────────────────────────────────────┘ │
    │                                                                          │
    │  ┌───────────────────────────┐ ┌────────────────────────────────────┐  │
    │  │    ConsumerManager        │ │    MessageRecoveryService          │  │
    │  │  - register()             │ │  - recover_stuck_messages()        │  │
    │  │  - heartbeat()            │ │  - recover_from_dead_consumer()    │  │
    │  │  - cleanup_dead()         │ │  - run_recovery_loop()             │  │
    │  └───────────────────────────┘ └────────────────────────────────────┘  │
    │                                                                          │
    │  ┌────────────────────────────────────────────────────────────────────┐ │
    │  │                     Background Tasks                                │ │
    │  │  - Consumer heartbeat loops     - Consumer cleanup                  │ │
    │  │  - Message recovery loop        - Health monitoring                 │ │
    │  └────────────────────────────────────────────────────────────────────┘ │
    └─────────────────────────────────────────────────────────────────────────┘


ASYNCIO CONCEPTS USED:
======================
From previous workbooks, you'll use:

| Concept                    | Used For                                    |
|----------------------------|---------------------------------------------|
| asyncio.Queue              | Internal message storage                    |
| asyncio.Lock               | Thread-safe state access                    |
| asyncio.Event              | Shutdown signaling                          |
| asyncio.create_task        | Background workers                          |
| asyncio.gather             | Concurrent operations                       |
| asyncio.wait_for           | Timeouts                                    |
| asyncio.sleep              | Heartbeat intervals, delays                 |
| Async context managers     | Resource cleanup                            |


EXAMPLE USAGE (what you're building):
=====================================
    # Start the system
    system = HAQueueSystem()
    await system.start()

    # Submit messages
    msg_id = await system.submit_message({"task": "process_image", "id": 123})

    # Create consumers
    async def handler(message):
        print(f"Processing: {message.payload}")
        # ... do work ...

    consumer_id = await system.create_consumer(handler)

    # Check health
    health = await system.health_check()
    print(health)  # {"status": "healthy", "queue_size": 5, "consumers": 3}

    # Graceful shutdown
    await system.stop()


IMPLEMENTATION ORDER:
=====================
1. HAMessageQueue - Core queue with lifecycle management
2. ConsumerManager - Track consumer health
3. MessageRecoveryService - Handle failures
4. HAQueueSystem - Integrate everything
5. Demo - Show it working!

Good luck! This is a challenging but rewarding exercise.
================================================================================
"""


# ==============================================================================
# DATA MODELS
# ==============================================================================

class MessageStatus(Enum):
    PENDING = auto()
    PROCESSING = auto()
    COMPLETED = auto()
    FAILED = auto()
    DEAD_LETTER = auto()


@dataclass
class Message:
    """Represents a message in the queue"""
    id: str
    payload: Any
    created_at: float
    status: MessageStatus = MessageStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    last_attempt_at: Optional[float] = None
    consumer_id: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "payload": self.payload,
            "status": self.status.name,
            "attempts": self.attempts,
            "created_at": self.created_at,
        }


@dataclass
class ConsumerInfo:
    """Tracks consumer state"""
    id: str
    registered_at: float
    last_heartbeat: float
    messages_processed: int = 0
    messages_failed: int = 0
    is_healthy: bool = True


# ==============================================================================
# QUESTION 1: Implement the Core Queue
# ==============================================================================
"""
REQUIREMENTS:
Implement `HAMessageQueue` - the core message queue with the following features:

1. Thread-safe message operations using asyncio.Lock
2. Message lifecycle: enqueue -> assign to consumer -> complete/fail
3. Dead letter queue for messages that exceed max_attempts
4. Visibility timeout: reassign messages if consumer doesn't complete in time

Methods to implement:
- async def enqueue(self, payload: Any, max_attempts: int = 3) -> str
- async def dequeue(self, consumer_id: str, visibility_timeout: float = 30.0) -> Optional[Message]
- async def complete(self, message_id: str, consumer_id: str) -> bool
- async def fail(self, message_id: str, consumer_id: str, error: str) -> bool
- async def get_stats(self) -> dict

EXPECTED BEHAVIOR:
>>> queue = HAMessageQueue()
>>> msg_id = await queue.enqueue({"task": "process_data"})
>>> msg = await queue.dequeue("consumer_1")
>>> msg.payload["task"]
'process_data'
>>> await queue.complete(msg.id, "consumer_1")
True
"""

class HAMessageQueue:
    def __init__(self, dead_letter_enabled: bool = True):
        self._lock = asyncio.Lock()
        self._messages: Dict[str, Message] = {}
        self._pending_queue: asyncio.Queue = asyncio.Queue()
        self._dead_letter: List[Message] = []
        self._dead_letter_enabled = dead_letter_enabled
        self._stats = {
            "total_enqueued": 0,
            "total_completed": 0,
            "total_failed": 0,
            "total_dead_lettered": 0,
        }
        # YOUR CODE HERE: Add any additional initialization

    async def enqueue(self, payload: Any, max_attempts: int = 3) -> str:
        """
        Add a new message to the queue.
        Returns the message ID.
        """
        # YOUR CODE HERE
        pass

    async def dequeue(self, consumer_id: str, visibility_timeout: float = 30.0) -> Optional[Message]:
        """
        Get the next available message for processing.
        Message becomes invisible to other consumers for visibility_timeout seconds.
        Returns None if queue is empty.
        """
        # YOUR CODE HERE
        pass

    async def complete(self, message_id: str, consumer_id: str) -> bool:
        """
        Mark a message as successfully processed.
        Returns True if successful, False if message not found or wrong consumer.
        """
        # YOUR CODE HERE
        pass

    async def fail(self, message_id: str, consumer_id: str, error: str) -> bool:
        """
        Mark a message as failed.
        If attempts < max_attempts, requeue for retry.
        Otherwise, move to dead letter queue.
        Returns True if successful.
        """
        # YOUR CODE HERE
        pass

    async def get_stats(self) -> dict:
        """Return queue statistics"""
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 2: Implement Consumer Manager
# ==============================================================================
"""
REQUIREMENTS:
Implement `ConsumerManager` to handle:

1. Consumer registration and deregistration
2. Heartbeat tracking for health monitoring
3. Automatic detection of dead consumers
4. Cleanup of messages assigned to dead consumers

Methods to implement:
- async def register(self, consumer_id: str) -> bool
- async def heartbeat(self, consumer_id: str) -> bool
- async def deregister(self, consumer_id: str) -> bool
- async def get_healthy_consumers(self) -> List[str]
- async def cleanup_dead_consumers(self, timeout: float) -> List[str]

EXPECTED BEHAVIOR:
>>> manager = ConsumerManager()
>>> await manager.register("consumer_1")
True
>>> await manager.heartbeat("consumer_1")
True
>>> consumers = await manager.get_healthy_consumers()
>>> "consumer_1" in consumers
True
"""

class ConsumerManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._consumers: Dict[str, ConsumerInfo] = {}
        # YOUR CODE HERE: Add any additional initialization

    async def register(self, consumer_id: str) -> bool:
        """Register a new consumer"""
        # YOUR CODE HERE
        pass

    async def heartbeat(self, consumer_id: str) -> bool:
        """Update consumer heartbeat"""
        # YOUR CODE HERE
        pass

    async def deregister(self, consumer_id: str) -> bool:
        """Remove a consumer"""
        # YOUR CODE HERE
        pass

    async def get_healthy_consumers(self) -> List[str]:
        """Get list of healthy consumer IDs"""
        # YOUR CODE HERE
        pass

    async def cleanup_dead_consumers(self, timeout: float) -> List[str]:
        """
        Find and mark consumers as unhealthy if no heartbeat within timeout.
        Returns list of dead consumer IDs.
        """
        # YOUR CODE HERE
        pass

    async def get_consumer_info(self, consumer_id: str) -> Optional[ConsumerInfo]:
        """Get info for a specific consumer"""
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 3: Implement Message Recovery
# ==============================================================================
"""
REQUIREMENTS:
Implement `MessageRecoveryService` to handle:

1. Periodic scanning for stuck messages (visibility timeout expired)
2. Requeuing messages from dead consumers
3. Moving expired messages to dead letter queue

Methods to implement:
- async def recover_stuck_messages(self, queue: HAMessageQueue) -> int
- async def recover_from_dead_consumer(self, queue: HAMessageQueue, consumer_id: str) -> int
- async def run_recovery_loop(self, queue: HAMessageQueue, interval: float) -> None

EXPECTED BEHAVIOR:
>>> recovery = MessageRecoveryService()
>>> # After consumer dies without completing
>>> recovered = await recovery.recover_from_dead_consumer(queue, "dead_consumer")
>>> recovered >= 0
True
"""

class MessageRecoveryService:
    def __init__(self, visibility_timeout: float = 30.0):
        self.visibility_timeout = visibility_timeout
        self._running = False
        # YOUR CODE HERE

    async def recover_stuck_messages(self, queue: HAMessageQueue) -> int:
        """
        Find messages that have exceeded visibility timeout and requeue them.
        Returns number of messages recovered.
        """
        # YOUR CODE HERE
        pass

    async def recover_from_dead_consumer(
        self,
        queue: HAMessageQueue,
        consumer_id: str
    ) -> int:
        """
        Recover all messages assigned to a dead consumer.
        Returns number of messages recovered.
        """
        # YOUR CODE HERE
        pass

    async def run_recovery_loop(
        self,
        queue: HAMessageQueue,
        interval: float = 5.0
    ) -> None:
        """
        Background task that periodically recovers stuck messages.
        Run this as a task that can be cancelled.
        """
        # YOUR CODE HERE
        pass

    def stop(self):
        """Signal the recovery loop to stop"""
        self._running = False


# ==============================================================================
# QUESTION 4: Implement the High-Availability Queue System
# ==============================================================================
"""
REQUIREMENTS:
Implement `HAQueueSystem` - the complete high-availability queue system that
integrates all components:

1. HAMessageQueue for core queue operations
2. ConsumerManager for consumer lifecycle
3. MessageRecoveryService for fault tolerance
4. Health check endpoint
5. Graceful shutdown

This is the main class that orchestrates everything.

Methods to implement:
- async def start(self) -> None
- async def stop(self) -> None
- async def submit_message(self, payload: Any) -> str
- async def create_consumer(self, handler: Callable) -> str
- async def stop_consumer(self, consumer_id: str) -> bool
- async def health_check(self) -> dict

EXPECTED BEHAVIOR:
>>> system = HAQueueSystem()
>>> await system.start()
>>> msg_id = await system.submit_message({"data": "test"})
>>> health = await system.health_check()
>>> health["status"]
'healthy'
>>> await system.stop()
"""

class HAQueueSystem:
    def __init__(
        self,
        max_consumers: int = 10,
        visibility_timeout: float = 30.0,
        recovery_interval: float = 5.0,
        heartbeat_interval: float = 10.0,
        consumer_timeout: float = 30.0,
    ):
        self.max_consumers = max_consumers
        self.visibility_timeout = visibility_timeout
        self.recovery_interval = recovery_interval
        self.heartbeat_interval = heartbeat_interval
        self.consumer_timeout = consumer_timeout

        self.queue = HAMessageQueue()
        self.consumer_manager = ConsumerManager()
        self.recovery_service = MessageRecoveryService(visibility_timeout)

        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        self._background_tasks: List[asyncio.Task] = []
        self._running = False
        self._started_at: Optional[float] = None
        # YOUR CODE HERE: Add any additional initialization

    async def start(self) -> None:
        """
        Start the queue system and all background services.
        """
        # YOUR CODE HERE
        pass

    async def stop(self) -> None:
        """
        Gracefully stop the queue system.
        - Stop accepting new messages
        - Wait for in-flight messages to complete (with timeout)
        - Cancel background tasks
        - Clean up resources
        """
        # YOUR CODE HERE
        pass

    async def submit_message(self, payload: Any, max_attempts: int = 3) -> str:
        """Submit a message to the queue"""
        # YOUR CODE HERE
        pass

    async def create_consumer(
        self,
        handler: Callable[[Message], Any],
        auto_heartbeat: bool = True
    ) -> str:
        """
        Create and start a new consumer.
        Handler is called for each message.
        Returns consumer_id.
        """
        # YOUR CODE HERE
        pass

    async def stop_consumer(self, consumer_id: str) -> bool:
        """Stop a specific consumer"""
        # YOUR CODE HERE
        pass

    async def health_check(self) -> dict:
        """
        Return system health status including:
        - Overall status (healthy/degraded/unhealthy)
        - Queue stats
        - Consumer stats
        - Uptime
        """
        # YOUR CODE HERE
        pass

    async def _consumer_loop(
        self,
        consumer_id: str,
        handler: Callable[[Message], Any],
        auto_heartbeat: bool
    ) -> None:
        """Internal consumer loop that processes messages"""
        # YOUR CODE HERE
        pass

    async def _heartbeat_loop(self, consumer_id: str) -> None:
        """Background task to send heartbeats for a consumer"""
        # YOUR CODE HERE
        pass

    async def _consumer_cleanup_loop(self) -> None:
        """Background task to clean up dead consumers"""
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 5: Implement a Test Scenario
# ==============================================================================
"""
REQUIREMENTS:
Implement `run_ha_queue_demo()` that demonstrates the full system:

1. Start the HA queue system
2. Create multiple producers sending messages
3. Create multiple consumers (some will "fail" randomly)
4. Simulate consumer crashes
5. Verify message recovery
6. Show stats and health checks
7. Graceful shutdown

This function should demonstrate:
- Normal message flow
- Consumer failure handling
- Message retry and dead letter
- Recovery of stuck messages
- System health monitoring
"""

async def run_ha_queue_demo() -> dict:
    """
    Run a complete demonstration of the HA queue system.

    Returns a summary dict with:
    - total_messages_sent
    - total_messages_processed
    - total_messages_failed
    - total_messages_recovered
    - consumers_crashed
    - final_health_status
    """
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 6: Implement Priority Queue Extension
# ==============================================================================
"""
BONUS CHALLENGE:
Extend the system to support message priorities.

Requirements:
- Messages can have priority levels (1=highest, 10=lowest)
- Higher priority messages are processed first
- Priority should not cause starvation (aging mechanism)
"""

class PriorityHAMessageQueue(HAMessageQueue):
    """Extension of HAMessageQueue with priority support"""

    async def enqueue_with_priority(
        self,
        payload: Any,
        priority: int = 5,
        max_attempts: int = 3
    ) -> str:
        """Enqueue a message with priority (1=highest, 10=lowest)"""
        # YOUR CODE HERE
        pass

    async def dequeue(
        self,
        consumer_id: str,
        visibility_timeout: float = 30.0
    ) -> Optional[Message]:
        """Dequeue considering priority"""
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 7: Implement Metrics Collection
# ==============================================================================
"""
BONUS CHALLENGE:
Add comprehensive metrics collection for monitoring.
"""

class MetricsCollector:
    """Collects and exposes metrics for the queue system"""

    def __init__(self):
        self._metrics: Dict[str, Any] = defaultdict(lambda: {"count": 0, "sum": 0})
        self._lock = asyncio.Lock()
        # YOUR CODE HERE

    async def record_latency(self, operation: str, latency_ms: float) -> None:
        """Record operation latency"""
        # YOUR CODE HERE
        pass

    async def increment_counter(self, metric: str, value: int = 1) -> None:
        """Increment a counter metric"""
        # YOUR CODE HERE
        pass

    async def get_metrics(self) -> dict:
        """Get all metrics as a dictionary"""
        # YOUR CODE HERE
        pass

    async def get_summary(self) -> dict:
        """Get a summary with averages and totals"""
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 8: Integration Test
# ==============================================================================
"""
Final integration test that verifies the entire system works correctly.
"""

async def integration_test() -> dict:
    """
    Comprehensive integration test for the HA queue system.

    Tests:
    1. Basic enqueue/dequeue
    2. Message completion
    3. Message failure and retry
    4. Dead letter queue
    5. Consumer registration/heartbeat
    6. Dead consumer detection
    7. Message recovery
    8. Graceful shutdown

    Returns test results dict.
    """
    results = {
        "tests_passed": 0,
        "tests_failed": 0,
        "details": []
    }

    def record_result(name: str, passed: bool, details: str = ""):
        if passed:
            results["tests_passed"] += 1
            results["details"].append(f"PASS: {name}")
        else:
            results["tests_failed"] += 1
            results["details"].append(f"FAIL: {name} - {details}")

    # Test 1: Basic enqueue/dequeue
    try:
        queue = HAMessageQueue()
        msg_id = await queue.enqueue({"test": "data"})
        msg = await queue.dequeue("test_consumer")
        assert msg is not None
        assert msg.id == msg_id
        assert msg.payload["test"] == "data"
        record_result("Basic enqueue/dequeue", True)
    except Exception as e:
        record_result("Basic enqueue/dequeue", False, str(e))

    # Test 2: Message completion
    try:
        queue = HAMessageQueue()
        msg_id = await queue.enqueue({"test": "complete"})
        msg = await queue.dequeue("consumer_1")
        success = await queue.complete(msg.id, "consumer_1")
        assert success
        stats = await queue.get_stats()
        assert stats["total_completed"] == 1
        record_result("Message completion", True)
    except Exception as e:
        record_result("Message completion", False, str(e))

    # Test 3: Message failure and retry
    try:
        queue = HAMessageQueue()
        msg_id = await queue.enqueue({"test": "retry"}, max_attempts=3)
        for i in range(2):
            msg = await queue.dequeue("consumer_1")
            await queue.fail(msg.id, "consumer_1", f"Error {i}")
        msg = await queue.dequeue("consumer_1")
        assert msg.attempts == 2
        record_result("Message failure and retry", True)
    except Exception as e:
        record_result("Message failure and retry", False, str(e))

    # Test 4: Dead letter queue
    try:
        queue = HAMessageQueue()
        msg_id = await queue.enqueue({"test": "dead_letter"}, max_attempts=2)
        for i in range(2):
            msg = await queue.dequeue("consumer_1")
            if msg:
                await queue.fail(msg.id, "consumer_1", f"Error {i}")
        stats = await queue.get_stats()
        assert stats["total_dead_lettered"] == 1
        record_result("Dead letter queue", True)
    except Exception as e:
        record_result("Dead letter queue", False, str(e))

    # Test 5: Consumer registration
    try:
        manager = ConsumerManager()
        await manager.register("consumer_1")
        await manager.heartbeat("consumer_1")
        consumers = await manager.get_healthy_consumers()
        assert "consumer_1" in consumers
        record_result("Consumer registration", True)
    except Exception as e:
        record_result("Consumer registration", False, str(e))

    # Test 6: Dead consumer detection
    try:
        manager = ConsumerManager()
        await manager.register("old_consumer")
        # Manually set old heartbeat
        async with manager._lock:
            manager._consumers["old_consumer"].last_heartbeat = time.time() - 100
        dead = await manager.cleanup_dead_consumers(timeout=30.0)
        assert "old_consumer" in dead
        record_result("Dead consumer detection", True)
    except Exception as e:
        record_result("Dead consumer detection", False, str(e))

    # Add more tests as needed...

    print("\n" + "=" * 60)
    print("INTEGRATION TEST RESULTS")
    print("=" * 60)
    for detail in results["details"]:
        print(f"  {detail}")
    print(f"\nTotal: {results['tests_passed']} passed, {results['tests_failed']} failed")

    return results


# ==============================================================================
# MAIN - Run Tests and Demo
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 10: HIGH-AVAILABILITY PRODUCER-CONSUMER QUEUE")
    print("=" * 60)

    print("\nRunning Integration Tests...")
    print("-" * 40)
    test_results = await integration_test()

    if test_results["tests_failed"] == 0:
        print("\nAll integration tests passed!")
        print("\nRunning HA Queue Demo...")
        print("-" * 40)
        try:
            demo_results = await run_ha_queue_demo()
            print("\nDemo Results:")
            for key, value in demo_results.items():
                print(f"  {key}: {value}")
        except NotImplementedError:
            print("Demo not yet implemented - complete the exercises first!")
        except Exception as e:
            print(f"Demo failed: {e}")
    else:
        print(f"\n{test_results['tests_failed']} tests failed.")
        print("Fix the implementation before running the demo.")

    print("\n" + "=" * 60)
    print("Workbook 10 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

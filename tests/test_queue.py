"""Tests for the SQLite queue."""

import tempfile
import time
from pathlib import Path

import pytest

from queue_processor.queue import QueueItem, SQLiteQueue


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_queue.db"
        yield str(db_path)


@pytest.fixture
def queue(temp_db):
    """Create a test queue."""
    return SQLiteQueue(db_path=temp_db, queue_name="test_queue")


def test_push_item(queue):
    """Test pushing an item to the queue."""
    item = QueueItem(
        payload={"page_url": "https://example.com"},
        metadata={"source": "test"},
    )

    item_id = queue.push(item)
    assert item_id > 0


def test_pop_item(queue):
    """Test popping an item from the queue."""
    # Push an item
    item = QueueItem(
        payload={"page_url": "https://example.com"},
        metadata={"source": "test"},
    )
    queue.push(item)

    # Pop it
    popped = queue.pop()
    assert popped is not None
    assert popped.payload["page_url"] == "https://example.com"
    assert popped.metadata["source"] == "test"


def test_pop_empty_queue(queue):
    """Test popping from an empty queue."""
    popped = queue.pop()
    assert popped is None


def test_ack_item(queue):
    """Test acknowledging an item."""
    item = QueueItem(payload={"page_url": "https://example.com"})
    item_id = queue.push(item)

    popped = queue.pop()
    assert popped is not None

    queue.ack(popped.id)

    # Item should be marked as completed
    items = queue.get_items(status="completed")
    assert len(items) == 1
    assert items[0]["id"] == item_id


def test_nack_item(queue):
    """Test negative acknowledging an item."""
    item = QueueItem(payload={"page_url": "https://example.com"})
    queue.push(item)

    popped = queue.pop()
    assert popped is not None

    queue.nack(popped.id)

    # Item should be available again
    popped_again = queue.pop()
    assert popped_again is not None
    assert popped_again.id == popped.id


def test_fail_item(queue):
    """Test failing an item."""
    item = QueueItem(payload={"page_url": "https://example.com"})
    item_id = queue.push(item)

    popped = queue.pop()
    assert popped is not None

    queue.fail(popped.id, "Test error")

    # Item should be marked as failed
    items = queue.get_items(status="failed")
    assert len(items) == 1
    assert items[0]["id"] == item_id
    assert "error" in items[0]["metadata"]


def test_visibility_timeout(queue):
    """Test visibility timeout."""
    item = QueueItem(payload={"page_url": "https://example.com"})
    queue.push(item)

    # Pop with short visibility timeout
    popped = queue.pop(visibility_timeout=1)
    assert popped is not None

    # Should not be available immediately
    popped_again = queue.pop()
    assert popped_again is None

    # Wait for visibility timeout
    time.sleep(2)

    # Should be available again
    popped_after_timeout = queue.pop()
    assert popped_after_timeout is not None
    assert popped_after_timeout.id == popped.id


def test_get_stats(queue):
    """Test getting queue statistics."""
    # Add some items
    for i in range(5):
        item = QueueItem(payload={"page_url": f"https://example.com/{i}"})
        queue.push(item)

    stats = queue.get_stats()
    assert stats["total"] == 5
    assert stats["pending"] == 5
    assert stats["processing"] == 0
    assert stats["completed"] == 0
    assert stats["failed"] == 0

    # Pop and ack one
    popped = queue.pop()
    queue.ack(popped.id)

    stats = queue.get_stats()
    assert stats["completed"] == 1
    assert stats["pending"] == 4


def test_get_items(queue):
    """Test getting items from the queue."""
    # Add items
    for i in range(10):
        item = QueueItem(payload={"page_url": f"https://example.com/{i}"})
        queue.push(item)

    # Get all items
    items = queue.get_items()
    assert len(items) == 10

    # Get with limit
    items = queue.get_items(limit=5)
    assert len(items) == 5

    # Get with offset
    items = queue.get_items(limit=5, offset=5)
    assert len(items) == 5


def test_clear_completed(queue):
    """Test clearing completed items."""
    # Add and complete items
    for i in range(5):
        item = QueueItem(payload={"page_url": f"https://example.com/{i}"})
        queue.push(item)

    # Complete all items
    while True:
        popped = queue.pop()
        if popped is None:
            break
        queue.ack(popped.id)

    stats = queue.get_stats()
    assert stats["completed"] == 5

    # Clear completed items (with 0 days to clear all)
    queue.clear_completed(older_than_days=0)

    # All should be gone
    items = queue.get_items(status="completed")
    assert len(items) == 0


def test_multiple_workers(queue):
    """Test that multiple workers don't process the same item."""
    # Add an item
    item = QueueItem(payload={"page_url": "https://example.com"})
    queue.push(item)

    # Pop from first "worker"
    popped1 = queue.pop()
    assert popped1 is not None

    # Try to pop from second "worker"
    popped2 = queue.pop()
    assert popped2 is None  # Should not get the same item

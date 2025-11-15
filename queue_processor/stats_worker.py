"""Worker for computing statistics on processed pages."""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from .queue import QueueItem, SQLiteQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StatsProcessor:
    """Processes completed pages and computes statistics."""

    def __init__(self, input_dir: str = "data", output_dir: str = "data/stats"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def compute_stats(self, processed_file: str) -> Optional[dict]:
        """Compute statistics from a processed page file.

        Args:
            processed_file: Path to processed JSON file

        Returns:
            Dictionary with statistics or None if failed
        """
        try:
            with open(processed_file, 'r') as f:
                data = json.load(f)

            chunks = data.get('chunks', [])
            if not chunks:
                logger.warning(f"No chunks found in {processed_file}")
                return None

            # Separate text and image chunks
            text_chunks = [c for c in chunks if c.get('type') == 'text']
            image_chunks = [c for c in chunks if c.get('type') == 'image']

            # Compute word counts for text chunks
            word_counts = []
            for chunk in text_chunks:
                text = chunk.get('value', '')
                words = len(text.split())
                word_counts.append(words)

            # Compute image URL lengths
            image_url_lengths = []
            for chunk in image_chunks:
                url = chunk.get('value', '')
                image_url_lengths.append(len(url))

            stats = {
                'page_url': data.get('page_url'),
                'total_chunks': len(chunks),
                'text_chunks': len(text_chunks),
                'image_chunks': len(image_chunks),
                'avg_words_per_text_chunk': sum(word_counts) / len(word_counts) if word_counts else 0,
                'total_words': sum(word_counts),
                'avg_image_url_length': sum(image_url_lengths) / len(image_url_lengths) if image_url_lengths else 0,
                'min_words_per_chunk': min(word_counts) if word_counts else 0,
                'max_words_per_chunk': max(word_counts) if word_counts else 0,
                'min_image_url_length': min(image_url_lengths) if image_url_lengths else 0,
                'max_image_url_length': max(image_url_lengths) if image_url_lengths else 0,
            }

            return stats

        except Exception as e:
            logger.error(f"Error computing stats for {processed_file}: {e}", exc_info=True)
            return None

    def save_stats(self, stats: dict, item_id: int) -> str:
        """Save statistics to a file.

        Args:
            stats: Statistics dictionary
            item_id: Queue item ID

        Returns:
            Path to saved stats file
        """
        output_file = self.output_dir / f"stats_{item_id}.json"
        with open(output_file, 'w') as f:
            json.dump(stats, f, indent=2)
        return str(output_file)


def process_completed_items(
    source_queue_name: str,
    target_queue_name: str,
    db_path: str = "queue.db",
    input_dir: str = "data",
    output_dir: str = "data/stats",
    batch_size: int = 100,
):
    """Process completed items from source queue and push to target queue.

    Args:
        source_queue_name: Queue to read completed items from
        target_queue_name: Queue to push stats to
        db_path: Path to SQLite database
        input_dir: Directory with processed files
        output_dir: Directory to save stats
        batch_size: Number of items to process before stopping
    """
    source_queue = SQLiteQueue(db_path, source_queue_name)
    target_queue = SQLiteQueue(db_path, target_queue_name)
    processor = StatsProcessor(input_dir, output_dir)

    logger.info(f"Starting stats processor: {source_queue_name} → {target_queue_name}")
    logger.info(f"Input dir: {input_dir}, Output dir: {output_dir}")

    # Get completed items from source queue
    completed_items = source_queue.get_items(status='completed', limit=batch_size)
    logger.info(f"Found {len(completed_items)} completed items to process")

    processed_count = 0
    failed_count = 0

    for item in completed_items:
        try:
            # Get the output file path from metadata
            output_file = item['metadata'].get('output_file')
            if not output_file:
                logger.warning(f"Item {item['id']} has no output_file in metadata")
                failed_count += 1
                continue

            # Check if file exists
            if not os.path.exists(output_file):
                logger.warning(f"File not found: {output_file}")
                failed_count += 1
                continue

            # Compute stats
            stats = processor.compute_stats(output_file)
            if stats is None:
                failed_count += 1
                continue

            # Save stats
            stats_file = processor.save_stats(stats, item['id'])

            # Create new queue item for target queue
            queue_item = QueueItem(
                payload={
                    'source_item_id': item['id'],
                    'page_url': item['payload'].get('page_url'),
                    'processed_file': output_file,
                    'stats_file': stats_file,
                },
                metadata={
                    'source': 'stats_processor',
                    'stats': stats,
                    'completed_at': time.time(),
                },
            )

            # Push to target queue and immediately mark as completed
            target_item_id = target_queue.push(queue_item)
            target_queue.ack(target_item_id, metadata=queue_item.metadata)
            processed_count += 1

            logger.info(
                f"Processed item {item['id']}: {stats['page_url']} - "
                f"{stats['avg_words_per_text_chunk']:.1f} avg words/chunk, "
                f"{stats['avg_image_url_length']:.0f} avg img URL length"
            )

        except Exception as e:
            logger.error(f"Error processing item {item['id']}: {e}", exc_info=True)
            failed_count += 1

    logger.info(
        f"Stats processing complete: {processed_count} succeeded, {failed_count} failed"
    )


def run_stats_worker(
    source_queue_name: str = "page_queue",
    target_queue_name: str = "stats_queue",
    db_path: str = "queue.db",
    input_dir: str = "data",
    output_dir: str = "data/stats",
    interval: int = 10,
    batch_size: int = 100,
):
    """Run stats worker that continuously processes completed items.

    Args:
        source_queue_name: Queue to read completed items from
        target_queue_name: Queue to push stats to
        db_path: Path to SQLite database
        input_dir: Directory with processed files
        output_dir: Directory to save stats
        interval: Seconds to wait between checks
        batch_size: Number of items to process per batch
    """
    logger.info(f"Starting continuous stats worker (checking every {interval}s)")

    while True:
        try:
            process_completed_items(
                source_queue_name=source_queue_name,
                target_queue_name=target_queue_name,
                db_path=db_path,
                input_dir=input_dir,
                output_dir=output_dir,
                batch_size=batch_size,
            )
        except Exception as e:
            logger.error(f"Error in stats worker loop: {e}", exc_info=True)

        time.sleep(interval)


def main():
    """Main entry point for stats worker."""
    parser = argparse.ArgumentParser(
        description="Stats worker for processing completed pages"
    )
    parser.add_argument(
        "--source-queue",
        default="page_queue",
        help="Queue to read completed items from"
    )
    parser.add_argument(
        "--target-queue",
        default="stats_queue",
        help="Queue to push stats to"
    )
    parser.add_argument(
        "--db",
        default="queue.db",
        help="Path to SQLite database"
    )
    parser.add_argument(
        "--input-dir",
        default="data",
        help="Directory with processed files"
    )
    parser.add_argument(
        "--output-dir",
        default="data/stats",
        help="Directory to save stats files"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process once and exit (don't run continuously)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Seconds to wait between checks (for continuous mode)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of items to process per batch"
    )

    args = parser.parse_args()

    if args.once:
        process_completed_items(
            source_queue_name=args.source_queue,
            target_queue_name=args.target_queue,
            db_path=args.db,
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            batch_size=args.batch_size,
        )
    else:
        run_stats_worker(
            source_queue_name=args.source_queue,
            target_queue_name=args.target_queue,
            db_path=args.db,
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            interval=args.interval,
            batch_size=args.batch_size,
        )


if __name__ == "__main__":
    main()

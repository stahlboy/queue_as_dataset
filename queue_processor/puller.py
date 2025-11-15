"""Example puller that reads from the queue and displays chunk counts."""

import argparse
import json
import logging
from pathlib import Path

from .queue import SQLiteQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueuePuller:
    """Reads completed items from the queue and displays statistics."""

    def __init__(self, queue: SQLiteQueue, output_dir: str = "data"):
        self.queue = queue
        self.output_dir = Path(output_dir)

    def pull_and_display(self):
        """Pull completed items and display their chunk counts."""
        items = self.queue.get_items(status="completed", limit=1000)

        if not items:
            logger.info("No completed items in queue")
            return

        logger.info(f"Found {len(items)} completed items")
        print("\n" + "=" * 80)
        print(f"{'ID':<8} {'URL':<50} {'Chunks':<10}")
        print("=" * 80)

        total_chunks = 0
        total_images = 0
        total_text = 0

        for item in items:
            item_id = item["id"]
            url = item["payload"]["page_url"]
            num_chunks = item["metadata"].get("num_chunks", 0)

            # Truncate URL if too long
            display_url = url[:47] + "..." if len(url) > 50 else url

            print(f"{item_id:<8} {display_url:<50} {num_chunks:<10}")

            # Try to load the processed file for more details
            output_file = item["metadata"].get("output_file")
            if output_file and Path(output_file).exists():
                try:
                    with open(output_file, "r") as f:
                        data = json.load(f)
                        total_chunks += len(data["chunks"])
                        total_images += sum(
                            1 for chunk in data["chunks"]
                            if chunk["type"] == "image"
                        )
                        total_text += sum(
                            1 for chunk in data["chunks"]
                            if chunk["type"] == "text"
                        )
                except Exception as e:
                    logger.error(f"Error reading {output_file}: {e}")

        print("=" * 80)
        print(f"\nSummary:")
        print(f"  Total items:  {len(items)}")
        print(f"  Total chunks: {total_chunks}")
        print(f"  Text chunks:  {total_text}")
        print(f"  Image chunks: {total_images}")
        print()

    def display_item_details(self, item_id: int):
        """Display detailed information about a specific item.

        Args:
            item_id: ID of the item to display
        """
        items = self.queue.get_items(limit=1000)
        item = next((i for i in items if i["id"] == item_id), None)

        if not item:
            logger.error(f"Item {item_id} not found")
            return

        print("\n" + "=" * 80)
        print(f"Item #{item_id}")
        print("=" * 80)
        print(f"URL:      {item['payload']['page_url']}")
        print(f"Status:   {item['status']}")
        print(f"Attempts: {item['attempts']}")
        print(f"Chunks:   {item['metadata'].get('num_chunks', 'N/A')}")

        output_file = item["metadata"].get("output_file")
        if output_file and Path(output_file).exists():
            print(f"Output:   {output_file}")

            try:
                with open(output_file, "r") as f:
                    data = json.load(f)

                print(f"\nContent breakdown:")
                print(f"  Total chunks: {len(data['chunks'])}")

                # Count by type
                text_chunks = [c for c in data["chunks"] if c["type"] == "text"]
                image_chunks = [c for c in data["chunks"] if c["type"] == "image"]

                print(f"  Text chunks:  {len(text_chunks)}")
                print(f"  Image chunks: {len(image_chunks)}")

                # Show first few chunks
                print(f"\nFirst 5 chunks:")
                for i, chunk in enumerate(data["chunks"][:5], 1):
                    if chunk["type"] == "text":
                        preview = chunk["value"][:100]
                        if len(chunk["value"]) > 100:
                            preview += "..."
                        print(f"  {i}. [TEXT] {preview}")
                    else:
                        print(f"  {i}. [IMAGE] {chunk['value']}")

            except Exception as e:
                logger.error(f"Error reading {output_file}: {e}")
        else:
            print("Output file not found")

        print("=" * 80 + "\n")


def main():
    """Main entry point for the puller."""
    parser = argparse.ArgumentParser(
        description="Pull completed items from queue and display statistics"
    )
    parser.add_argument(
        "--db",
        default="queue.db",
        help="Path to SQLite database"
    )
    parser.add_argument(
        "--queue",
        default="page_queue",
        help="Queue name"
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Output directory where processed files are stored"
    )
    parser.add_argument(
        "--item-id",
        type=int,
        help="Display details for a specific item ID"
    )

    args = parser.parse_args()

    queue = SQLiteQueue(db_path=args.db, queue_name=args.queue)
    puller = QueuePuller(queue, output_dir=args.output_dir)

    if args.item_id:
        puller.display_item_details(args.item_id)
    else:
        puller.pull_and_display()


if __name__ == "__main__":
    main()

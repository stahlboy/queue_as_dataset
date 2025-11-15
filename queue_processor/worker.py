"""Worker for processing web pages into interleaved content."""

import json
import logging
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, NavigableString, Tag

from .models import InterleavedChunk, ProcessedPage
from .queue import QueueItem, SQLiteQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WebPageProcessor:
    """Processes web pages into interleaved text/image content."""

    def __init__(self, output_dir: str = "data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def download_page(self, url: str, timeout: int = 30) -> Optional[str]:
        """Download HTML content from a URL.

        Args:
            url: URL to download
            timeout: Request timeout in seconds

        Returns:
            HTML content as string, or None if failed
        """
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            with httpx.Client(follow_redirects=True, timeout=timeout) as client:
                response = client.get(url, headers=headers)
                response.raise_for_status()
                return response.text
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            return None

    def extract_interleaved_content(
        self, html: str, base_url: str
    ) -> list[InterleavedChunk]:
        """Extract interleaved text and image content from HTML.

        Args:
            html: HTML content
            base_url: Base URL for resolving relative URLs

        Returns:
            List of InterleavedChunk objects
        """
        soup = BeautifulSoup(html, "lxml")

        # Remove script and style elements
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()

        chunks = []

        # Find the main content area (try common patterns)
        main_content = (
            soup.find("main")
            or soup.find("article")
            or soup.find("div", class_=lambda x: x and "content" in x.lower() if x else False)
            or soup.find("body")
        )

        if not main_content:
            main_content = soup.find("body")

        if not main_content:
            return chunks

        # Walk through elements and extract text and images in order
        for element in main_content.descendants:
            # Handle text nodes
            if isinstance(element, NavigableString):
                text = str(element).strip()
                # Skip empty text and very short text
                if text and len(text) > 10:
                    # Check if we should add a new text chunk or append to existing
                    if chunks and chunks[-1].type == "text":
                        # Append to last text chunk
                        chunks[-1].value += " " + text
                    else:
                        chunks.append(InterleavedChunk(type="text", value=text))

            # Handle image elements
            elif isinstance(element, Tag) and element.name == "img":
                src = element.get("src")
                if src:
                    # Resolve relative URLs
                    absolute_url = urljoin(base_url, src)

                    # Filter out likely tracking pixels, icons, etc.
                    if self._is_valid_image(element, src):
                        chunks.append(
                            InterleavedChunk(type="image", value=absolute_url)
                        )

            # Handle paragraph and heading elements for better text extraction
            elif isinstance(element, Tag) and element.name in ["p", "h1", "h2", "h3", "h4", "h5", "h6"]:
                text = element.get_text(" ", strip=True)
                if text and len(text) > 10:
                    # Add as new chunk to maintain structure
                    chunks.append(InterleavedChunk(type="text", value=text))

        # Post-process: merge consecutive text chunks
        merged_chunks = []
        for chunk in chunks:
            if chunk.type == "text":
                if merged_chunks and merged_chunks[-1].type == "text":
                    merged_chunks[-1].value += "\n\n" + chunk.value
                else:
                    merged_chunks.append(chunk)
            else:
                merged_chunks.append(chunk)

        # Filter out very short text chunks
        final_chunks = [
            chunk for chunk in merged_chunks
            if chunk.type == "image" or (chunk.type == "text" and len(chunk.value) > 50)
        ]

        return final_chunks

    def _is_valid_image(self, img_element: Tag, src: str) -> bool:
        """Check if an image is worth including.

        Args:
            img_element: BeautifulSoup img tag
            src: Image source URL

        Returns:
            True if image should be included
        """
        # Skip tracking pixels
        width = img_element.get("width")
        height = img_element.get("height")

        if width and height:
            try:
                w = int(width)
                h = int(height)
                if w < 50 or h < 50:
                    return False
            except ValueError:
                pass

        # Skip common icon/logo patterns
        src_lower = src.lower()
        skip_patterns = [
            "icon", "logo", "avatar", "emoji", "pixel",
            "tracking", "analytics", "ad", "banner"
        ]

        if any(pattern in src_lower for pattern in skip_patterns):
            return False

        # Skip data URLs (usually small icons)
        if src.startswith("data:"):
            return False

        return True

    def process_page(self, url: str) -> Optional[ProcessedPage]:
        """Process a web page into interleaved content.

        Args:
            url: URL to process

        Returns:
            ProcessedPage object or None if failed
        """
        logger.info(f"Processing: {url}")

        html = self.download_page(url)
        if not html:
            return None

        chunks = self.extract_interleaved_content(html, url)

        if not chunks:
            logger.warning(f"No content extracted from {url}")
            return None

        return ProcessedPage(
            page_url=url,
            chunks=chunks,
            metadata={
                "processed_at": time.time(),
                "num_chunks": len(chunks),
                "num_images": sum(1 for c in chunks if c.type == "image"),
                "num_text": sum(1 for c in chunks if c.type == "text"),
            }
        )

    def save_processed_page(self, processed: ProcessedPage, item_id: int) -> str:
        """Save processed page to disk.

        Args:
            processed: ProcessedPage object
            item_id: Queue item ID

        Returns:
            Path to saved file
        """
        filename = f"processed_{item_id}.json"
        filepath = self.output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(processed.model_dump(), f, indent=2, ensure_ascii=False)

        logger.info(f"Saved processed page to {filepath}")
        return str(filepath)


class QueueWorker:
    """Worker that processes items from the queue."""

    def __init__(
        self,
        queue: SQLiteQueue,
        processor: WebPageProcessor,
        poll_interval: int = 5,
        visibility_timeout: int = 300,
    ):
        self.queue = queue
        self.processor = processor
        self.poll_interval = poll_interval
        self.visibility_timeout = visibility_timeout
        self.running = False

    def run(self):
        """Run the worker loop."""
        self.running = True
        logger.info("Worker started")

        try:
            while self.running:
                item = self.queue.pop(visibility_timeout=self.visibility_timeout)

                if item is None:
                    # No items available, sleep and retry
                    time.sleep(self.poll_interval)
                    continue

                self.process_item(item)

        except KeyboardInterrupt:
            logger.info("Worker stopped by user")
        finally:
            self.running = False

    def process_item(self, item: QueueItem):
        """Process a single queue item.

        Args:
            item: QueueItem to process
        """
        url = item.payload.get("page_url")
        if not url:
            logger.error(f"Item {item.id} has no page_url")
            self.queue.fail(item.id, "Missing page_url in payload")
            return

        try:
            # Process the page
            processed = self.processor.process_page(url)

            if processed is None:
                self.queue.fail(item.id, "Failed to process page")
                return

            # Save to disk
            filepath = self.processor.save_processed_page(processed, item.id)

            # Update metadata with result
            item.metadata["output_file"] = filepath
            item.metadata["num_chunks"] = len(processed.chunks)
            item.metadata["completed_at"] = time.time()

            # Acknowledge success
            self.queue.ack(item.id)
            logger.info(
                f"Successfully processed item {item.id}: {url} "
                f"({len(processed.chunks)} chunks)"
            )

        except Exception as e:
            logger.error(f"Error processing item {item.id}: {e}", exc_info=True)
            self.queue.fail(item.id, str(e))

    def stop(self):
        """Stop the worker."""
        self.running = False


def main():
    """Main entry point for the worker."""
    import argparse

    parser = argparse.ArgumentParser(description="Queue worker for processing web pages")
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
        help="Output directory for processed files"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Polling interval in seconds"
    )

    args = parser.parse_args()

    queue = SQLiteQueue(db_path=args.db, queue_name=args.queue)
    processor = WebPageProcessor(output_dir=args.output_dir)
    worker = QueueWorker(queue, processor, poll_interval=args.poll_interval)

    logger.info(f"Starting worker with queue '{args.queue}' from {args.db}")
    worker.run()


if __name__ == "__main__":
    main()

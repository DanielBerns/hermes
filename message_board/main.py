import pdb

import argparse
import yaml
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import asyncio

from message_board_client.core import MessageBoardClient

logger = logging.getLogger(__name__)

def perform_action(tags: List[str] | None, content: str) -> None:
    """
    Decides what action to take based on the parsed content.
    """
    print(f"> {content}")
    for tt in tags:
        print(f"  {tt}")
    logger.info(f"ACTION: Received generic content: {content}")

async def async_main():
    # --- Configuration ---
    # Path to the directory containing your secrets file (e.g., ./secrets/my_agent.json)
    # The JSON file must contain: {"base_url": "...", "username": "...", "password": "..."}

    parser = argparse.ArgumentParser(description="Execute some actions following orders.")
    parser.add_argument(
        "-s", "--secrets",
        type=str,
        required=True,
        help="Path to user secrets"
    )
    parser.add_argument(
        "-t", "--tags-file",
        type=str,
        required=False,
        help="Path to a text file containing tags to watch (one per line)"
    )

    args = parser.parse_args()

    secrets_file = Path(args.secrets)

    # Tags to filter messages by (optional)
    tags_to_watch = None
    if args.tags_file:
        try:
            with open(args.tags_file, "r") as f:
                tags_to_watch = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Failed to read tags file {args.tags_file}: {e}")
            sys.exit(1)

    try:
        # 1. Initialize the Client
        async with MessageBoardClient(str(secrets_file)) as client:
            # 2. Retrieve messages
            if tags_to_watch:
                messages = await client.get_public_messages(tags=tags_to_watch)
            else:
                messages = await client.get_public_messages()

            if not messages:
                logger.info("No new messages retrieved.")
                return

        logger.info(f"Processing {len(messages)} messages...")

        # 6. Iterate, Parse, and Act
        for msg in messages:
            # Assuming the server returns a dictionary with 'tags' and 'content' field
            tags = msg.get("tags", None)
            content = msg.get("content", "")

            if not content:
                continue

            try:
                # Execute action based on the parsed data
                perform_action(tags, content)

            except json.JSONDecodeError:
                logger.error(f"Failed to parse message ID {msg.get('id', '?')}")
            except Exception as e:
                logger.error(f"Error processing message ID {msg.get('id', '?')}: {e}")

    except Exception as e:
        logger.critical(f"Agent failed: {e}")

def main() -> None:
    asyncio.run(async_main())

if __name__ == "__main__":
    main()

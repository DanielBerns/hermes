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

def perform_action(data: Dict[str, Any]) -> None:
    """
    Decides what action to take based on the parsed JSON content.
    """
    action_type = data.get("type", "unknown")

    if action_type == "database_update":
        # Example Action 1: Update a record
        record_id = data.get("id")
        value = data.get("value")
        logger.info(f"ACTION: Updating database record {record_id} with value '{value}'")

    elif action_type == "alert":
        # Example Action 2: Trigger an alert
        level = data.get("level", "info")
        msg = data.get("message", "No message")
        logger.warning(f"ACTION: Triggering {level.upper()} alert: {msg}")

    else:
        logger.info(f"ACTION: Received generic data: {data}")

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

    args = parser.parse_args()

    secrets_file = Path(args.secrets)

    # Tags to filter messages by (optional)
    tags_to_watch = ["system_events", "json_data"]

    try:
        # 1. Initialize the Client
        async with MessageBoardClient(str(secrets_file)) as client:
            # 2. Retrieve messages
            messages = await client.get_public_messages(tags=tags_to_watch)

            if not messages:
                logger.info("No new messages retrieved.")
                return

        logger.info(f"Processing {len(messages)} messages...")

        # 6. Iterate, Parse, and Act
        for msg in messages:
            # Assuming the server returns a dictionary with a 'content' field
            raw_content = msg.get("content", "")

            if not raw_content:
                continue

            try:
                # Parse the content as JSON
                json_data = json.loads(raw_content)

                # Execute action based on the parsed data
                perform_action(json_data)

            except json.JSONDecodeError:
                logger.error(f"Failed to parse message ID {msg.get('id', '?')} as JSON. Content: {raw_content[:50]}...")
            except Exception as e:
                logger.error(f"Error processing message ID {msg.get('id', '?')}: {e}")

    except Exception as e:
        logger.critical(f"Agent failed: {e}")

def main() -> None:
    asyncio.run(async_main())

if __name__ == "__main__":
    main()

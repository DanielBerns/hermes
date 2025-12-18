import json
import logging
from pathlib import Path
from typing import Any, Dict

# Ensure the hermes package is in your python path
from hermes.message_board.agent import MessageBoardAgent, ReceivePublicMessages

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

def main():
    # --- Configuration ---
    # Path to the directory containing your secrets file (e.g., ./secrets/my_agent.json)
    # The JSON file must contain: {"base_url": "...", "username": "...", "password": "..."}
    secrets_dir = Path("./secrets")
    agent_identifier = "my_agent"

    # Tags to filter messages by (optional)
    tags_to_watch = ["system_events", "json_data"]

    try:
        # 1. Initialize the Agent
        # This loads credentials from the secrets file associated with the identifier
        agent = MessageBoardAgent(secrets_dir, agent_identifier)

        # 2. Create the operation to retrieve messages
        # We use ReceivePublicMessages to fetch messages visible to everyone or specific tags
        receive_op = ReceivePublicMessages(tags=tags_to_watch)

        # 3. Add the operation to the agent
        agent.add(receive_op)

        # 4. Run the agent
        # This handles authentication and executes the added operations
        logger.info(f"Agent '{agent_identifier}' connecting to server...")
        agent.run()

        # 5. Process the response
        response = receive_op.response

        # The structure of 'response' depends on your specific MessageBoard server implementation.
        # Here we assume a standard list of message dictionaries or a dict with a 'messages' key.
        messages = []
        if isinstance(response, list):
            messages = response
        elif isinstance(response, dict):
            messages = response.get("messages", [])
            if not messages and "content" in response: # Handle single message response edge case
                 messages = [response]

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

if __name__ == "__main__":
    main()




from pathlib import Path

from hermes.core.action import execute
from hermes.message_board.actor import MessageBoardActor


def main() -> None:
    filename = Path(__file__)
    script, project_identifier = filename.stem, filename.parents[1].stem
    action = MessageBoardActor()
    execute(script, project_identifier, action)

if __name__ == "__main__":
    main()

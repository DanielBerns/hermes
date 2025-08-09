import logging
import json
from typing import Protocol, Any, List, Tuple, Generator
from contextlib import contextmanager
from pathlib import Path

from hermes.core.helpers import get_resource, read_json, write_json
from hermes.message_board.client import MessageBoardClient

logger = logging.getLogger(__name__)

class AgentException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class AgentOperation(Protocol):
    def execute(self, client: MessageBoardClient) -> None:
        ...

    @property
    def response(self) -> dict[str, Any] | None:
        ...


def response_to_string(response: dict[str, Any] | None) -> str:
    if response:
        try:
            result = json.dumps(response) if response else "-"
        except Exception as error:
            a_string = str(response) if response else "-"
            result = f"Error {error}: check this {a_string}"
    else:
        result = "-"
    return result


class SendPrivateMessage:
    def __init__(self, recipient: str, content: str) -> None:
        self._recipient = recipient
        self._content = content
        self._response: dict[str, Any] | None = None

    @property
    def recipient(self) -> str:
        return self._recipient

    @property
    def content(self) -> str:
        return self._content

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.send_private_message(self.recipient, self.content)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}: recipient {self.recipient} - content {self.content}"


class ReceivePrivateMessage:
    def __init__(self) -> None:
        self._response: dict[str, Any] | None = None

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.get_private_message()

    def __repr__(self) -> str:
        response_str = response_to_string(self.response)
        return f"{self.__class__.__name__}: {response_str}"


class SendGroupMessage:
    def __init__(self, recipients: List[str], content: str) -> None:
        self._recipients = recipients
        self._content = content
        self._response: dict[str, Any] | None = None

    @property
    def recipients(self) -> List[str]:
        return self._recipients

    @property
    def content(self) -> str:
        return self._content

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.send_group_message(self.recipients, self.content)

    def __repr__(self) -> str:
        recipients = ",".join(self.recipients)
        response_str = response_to_string(self.response)
        return f"{self.__class__.__name__}: {recipients} {response_str}"


class ReceiveGroupMessages:
    def __init__(self) -> None:
        self._response: dict[str, Any] | None = None

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.get_group_messages()

    def __repr__(self) -> str:
        response_str = response_to_string(self.response)
        return f"{self.__class__.__name__}: {response_str}"


class SendPublicMessage:
    def __init__(self, tags: List[str], content: str) -> None:
        self._tags = tags
        self._content = content
        self._response: dict[str, Any] | None = None

    @property
    def tags(self) -> List[str]:
        return self._tags

    @property
    def content(self) -> str:
        return self._content

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.send_public_message(self.content, self.tags)

    def __repr__(self) -> str:
        response_str = response_to_string(self.response)
        return f"{self.__class__.__name__}: {response_str}"


class ReceivePublicMessages:
    def __init__(self, tags: List[str]) -> None:
        self._tags = tags
        self._response: dict[str, Any] | None = None

    @property
    def tags(self) -> List[str]:
        return self._tags

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.get_public_messages(filter_tags=self.tags)

    def __repr__(self) -> str:
        response_str = response_to_string(self.response)
        return f"{self.__class__.__name__}: {response_str}"


class SubscribeTags:
    def __init__(self, tags: List[str]) -> None:
        self._tags = tags
        self._response: dict[str, Any] | None = None

    @property
    def tags(self) -> List[str]:
        return self._tags

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.subscribe_to_tags(self.tags)

    def __repr__(self) -> str:
        response_str = response_to_string(self.response)
        return f"{self.__class__.__name__}: {response_str}"


class UnsubscribeTags:
    def __init__(self, tags: List[str]) -> None:
        self._tags = tags
        self._response: dict[str, Any] | None = None

    @property
    def tags(self) -> List[str]:
        return self._tags

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.unsubscribe_from_tags(self.tags)

    def __repr__(self) -> str:
        response_str = response_to_string(self.response)
        return f"{self.__class__.__name__}: {response_str}"


class DeleteMessage:
    def __init__(self, message_id: int) -> None:
        self._message_id = message_id
        self._response: dict[str, Any] | None = None

    @property
    def message_id(self) -> int:
        return self._message_id

    @property
    def response(self) -> dict[str, Any] | None:
        return self._response

    @response.setter
    def response(self, value: dict[str, Any]) -> None:
        self._response = value

    def execute(self, client: MessageBoardClient) -> None:
        self.response = client.delete_message(self.message_id)

    def __repr__(self) -> str:
        response_str = response_to_string(self.response)
        return f"{self.__class__.__name__}: {response_str}"


class MessageBoardAgent:

    @staticmethod
    def get_secrets_resource(secrets_container: Path, identifier: str) -> Path:
        secrets_resource = get_resource(secrets_container, identifier, ".json")
        if secrets_resource.exists():
            pass
        else:
            secrets_json = {
                "base_url": "http://127.0.0.1:5000",
                "username": "unknown",
                "password": "unknown"
            }
            write_json(secrets_resource, secrets_json)
        return secrets_resource

    def __init__(
        self,
        secrets_container: str,
        identifier: str
    ) -> None:
        try:
            secrets_resource = MessageBoardAgent.get_secrets_resource(
                secrets_container,
                identifier
            )
            secrets_json = read_json(secrets_resource)
        except Exception as an_exception:
            message = f"no {identifier} secrets_json at {secrets_resource}"
            raise AgentException(message) from an_exception

        base_url = secrets_json.get("base_url", False)
        username = secrets_json.get("username", False) if base_url else False
        password = secrets_json.get("password", False) if username else False
        if not password:
            message = f"check secrets_json: {secrets_resource}"
            raise AgentException(message)

        self._identifier = identifier
        self._credentials = (base_url, username, password)
        self._operations: List[AgentOperation] = []

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def credentials(self) -> Tuple[str, str, str]:
        return self._credentials

    def add(self, op: AgentOperation) -> None:
        self._operations.append(op)

    def operations(self) -> Generator[AgentOperation, None, None]:
        yield from self._operations

    def run(self) -> None:
        base_url, username, password = self.credentials
        client = MessageBoardClient(base_url)
        try:
            success, _ = client.login(username, password)
            if not success:
                return
        except Exception as an_exception:
            message = f"{self.__class__.__item__}.run|: no credentials"
            raise AgentException(message) from an_exception

        for this_operation in self.operations():
            current_username = username
            try:
                this_operation.execute(client)
            except Exception as an_exception:
                message = f"Running {self.identifier} operation {this_operation}"
                raise AgentException(message) from an_exception

    def log_responses(self) -> None:
        for op in self.operations():
            logger.info(response_to_string(op.response))

    def clear(self) -> None:
        self._operations.clear()


@contextmanager
def initialize_agent(secrets_container: Path, identifier: str) -> Generator[MessageBoardAgent, None, None]:
    try:
        agent = MessageBoardAgent(secrets_container, identifier)
        yield agent
    except Exception as an_exception:
        message = f"initialize_agent({secrets_container, identifier}) (1)"
        raise AgentException(message) from an_exception

    try:
        agent.run()
        agent.log_responses()
        agent.clear()
    except Exception as an_exception:
        message = f"initialize_agent({secrets_container, identifier}) (2)"
        raise AgentException(message) from an_exception

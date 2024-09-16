#!/usr/bin/env python3
"""
Retry ActiveMQ Dead Letter Queue (DLQ) Messages.

This script allows users to retry messages from ActiveMQ DLQs based on specified criteria.
Users can configure various parameters via command-line arguments to tailor the retry process.

Usage:
    python retry_dlq_messages.py --host <ActiveMQ_HOST_URL> [options]

Options:
    --host                ActiveMQ host URL (e.g., https://activemq-host:port) [required]
    --age-limit           Message age limit in hours (default: 24)
    --jolokia-api-path    Jolokia API path (default: /api/jolokia/)
    --dlq-prefixes        Prefixes for DLQ queues (default: DLQ ActiveMQ.DLQ)
    --special-queues      Names of special queues to always retry (default: Aberdunk Arsenal)
    --verify-ssl          Enable SSL certificate verification
"""

import argparse
import os
import sys
import getpass
import re
import logging
from datetime import datetime
from typing import Tuple, List, Dict, Any, Optional, Set

import requests
from tabulate import tabulate

# Constants
DEFAULT_AGE_LIMIT_HOURS: int = 24
DEFAULT_JOLKIA_API_PATH: str = "/api/jolokia/"
DEFAULT_DLQ_PREFIXES: Tuple[str, ...] = ("DLQ", "ActiveMQ.DLQ")
DEFAULT_SPECIAL_QUEUES: Tuple[str, ...] = ("foo", "bar")
REQUEST_TIMEOUT: int = 10  # Timeout for HTTP requests in seconds

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
LOGGER: logging.Logger = logging.getLogger(__name__)


def confirm_action() -> bool:
    """
    Prompt the user for confirmation to proceed.

    Returns:
        bool: True if the user confirms, exits otherwise.
    """
    while True:
        user_input = input(
            "Do you want to retry these messages? (yes/no): "
        ).strip().lower()
        if user_input in {"yes", "y"}:
            return True
        if user_input in {"no", "n"}:
            LOGGER.info("Operation cancelled by the user.")
            sys.exit(0)
        else:
            LOGGER.warning("Please enter 'yes' or 'no'.")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Retry ActiveMQ DLQ Messages")
    parser.add_argument(
        "--host",
        required=True,
        help="ActiveMQ host URL (e.g., https://activemq-host:port)",
    )
    parser.add_argument(
        "--age-limit",
        type=int,
        default=DEFAULT_AGE_LIMIT_HOURS,
        help=f"Message age limit in hours (default: {DEFAULT_AGE_LIMIT_HOURS})",
    )
    parser.add_argument(
        "--jolokia-api-path",
        type=str,
        default=DEFAULT_JOLKIA_API_PATH,
        help=f"Jolokia API path (default: {DEFAULT_JOLKIA_API_PATH})",
    )
    parser.add_argument(
        "--dlq-prefixes",
        nargs='+',
        default=list(DEFAULT_DLQ_PREFIXES),
        help=f"Prefixes for DLQ queues (default: {' '.join(DEFAULT_DLQ_PREFIXES)})",
    )
    parser.add_argument(
        "--special-queues",
        nargs='+',
        default=list(DEFAULT_SPECIAL_QUEUES),
        help=f"Names of queues to always retry (default: {' '.join(DEFAULT_SPECIAL_QUEUES)})",
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        help="Enable SSL certificate verification",
    )
    return parser.parse_args()


def get_credentials() -> Tuple[str, str]:
    """
    Retrieve ActiveMQ credentials from environment variables or prompt the user.

    Returns:
        Tuple[str, str]: Username and password.
    """
    username = os.environ.get("ACTIVEMQ_USER") or input("Enter ActiveMQ username: ")
    password = os.environ.get("ACTIVEMQ_PASSWORD") or getpass.getpass(
        "Enter ActiveMQ password: "
    )
    return username, password


def build_headers(host: str) -> Dict[str, str]:
    """
    Build HTTP headers for the requests.

    Args:
        host (str): ActiveMQ host URL.

    Returns:
        Dict[str, str]: Headers dictionary.
    """
    headers = {"Content-Type": "application/json"}
    if host.startswith("https://"):
        headers["Origin"] = host
    return headers


def get_jolokia_url(host: str, jolokia_api_path: str) -> str:
    """
    Construct the Jolokia API URL.

    Args:
        host (str): ActiveMQ host URL.
        jolokia_api_path (str): Jolokia API path.

    Returns:
        str: Jolokia API URL.
    """
    return host.rstrip("/") + jolokia_api_path


def fetch_mbeans(
    jolokia_url: str, headers: Dict[str, str], auth: Tuple[str, str], verify_ssl: bool
) -> List[str]:
    """
    Fetch MBeans from ActiveMQ using the Jolokia API.

    Args:
        jolokia_url (str): Jolokia API URL.
        headers (Dict[str, str]): HTTP headers.
        auth (Tuple[str, str]): Authentication credentials.
        verify_ssl (bool): SSL verification flag.

    Returns:
        List[str]: List of MBeans.
    """
    search_payload = {
        "type": "search",
        "mbean": "org.apache.activemq:type=Broker,brokerName=*,destinationType=Queue,destinationName=*",
    }
    try:
        response = requests.post(
            jolokia_url,
            json=search_payload,
            auth=auth,
            headers=headers,
            verify=verify_ssl,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json().get("value", [])
    except requests.exceptions.RequestException as error:
        LOGGER.error(f"Error fetching queues: {error}")
        sys.exit(1)


def extract_queue_mbeans(mbeans: List[str]) -> Dict[str, str]:
    """
    Extract a mapping of queue names to their MBeans.

    Args:
        mbeans (List[str]): List of MBeans.

    Returns:
        Dict[str, str]: Mapping of queue names to MBeans.
    """
    queue_mbeans: Dict[str, str] = {}
    for mbean in mbeans:
        match = re.search(r"destinationName=([^,]+)", mbean)
        if match:
            queue_name = match.group(1)
            queue_mbeans[queue_name] = mbean
    return queue_mbeans


def browse_queue(
    jolokia_url: str,
    mbean: str,
    headers: Dict[str, str],
    auth: Tuple[str, str],
    verify_ssl: bool,
) -> List[Dict[str, Any]]:
    """
    Browse messages in a specific queue.

    Args:
        jolokia_url (str): Jolokia API URL.
        mbean (str): MBean identifier.
        headers (Dict[str, str]): HTTP headers.
        auth (Tuple[str, str]): Authentication credentials.
        verify_ssl (bool): SSL verification flag.

    Returns:
        List[Dict[str, Any]]: List of messages.
    """
    operation_payload = {"type": "exec", "mbean": mbean, "operation": "browse()"}
    try:
        response = requests.post(
            jolokia_url,
            json=operation_payload,
            auth=auth,
            headers=headers,
            verify=verify_ssl,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json().get("value", [])
    except requests.exceptions.RequestException as error:
        LOGGER.error(f"Error browsing queue {mbean}: {error}")
        return []


def parse_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single message to extract relevant fields.

    Args:
        msg (Dict[str, Any]): Message dictionary.

    Returns:
        Dict[str, Any]: Parsed message details.
    """
    message_id: Optional[str] = msg.get("JMSMessageID")
    timestamp_str: Optional[str] = msg.get("JMSTimestamp")
    priority: Optional[int] = msg.get("JMSPriority")
    properties: Dict[str, Any] = msg.get("Properties", {})
    original_destination: Optional[str] = (
        msg.get("OriginalDestination") or properties.get("JMSXOriginalDestination")
    )

    try:
        msg_time = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
        age_hours = round((datetime.now().timestamp() - msg_time.timestamp()) / 3600)
    except (ValueError, TypeError) as error:
        LOGGER.warning(f"Error parsing timestamp for message {message_id}: {error}")
        age_hours = None

    return {
        "message_id": message_id,
        "age_hours": age_hours,
        "priority": priority,
        "original_destination": original_destination,
    }


def display_messages(messages: List[Dict[str, Any]], age_limit: int) -> None:
    """
    Display messages in a tabulated format.

    Args:
        messages (List[Dict[str, Any]]): List of message dictionaries.
        age_limit (int): Age limit in hours.
    """
    table: List[List[Any]] = [
        [
            msg["message_id"],
            msg["queue_name"],
            msg["age_hours"],
            msg["priority"],
        ]
        for msg in messages
        if msg["age_hours"] is not None and msg["age_hours"] <= age_limit
    ]
    table_headers: List[str] = ["Message ID", "Queue Name", "Age (hours)", "Priority"]
    if table:
        print(tabulate(table, headers=table_headers))
    else:
        LOGGER.info("No messages found within the specified age limit.")


def retry_messages(
    messages_to_retry: List[Dict[str, Any]],
    queue_mbeans: Dict[str, str],
    jolokia_url: str,
    headers: Dict[str, str],
    auth: Tuple[str, str],
    verify_ssl: bool,
) -> None:
    """
    Retry the specified messages by moving them back to their original destinations.

    Args:
        messages_to_retry (List[Dict[str, Any]]): Messages to retry.
        queue_mbeans (Dict[str, str]): Mapping of queue names to MBeans.
        jolokia_url (str): Jolokia API URL.
        headers (Dict[str, str]): HTTP headers.
        auth (Tuple[str, str]): Authentication credentials.
        verify_ssl (bool): SSL verification flag.
    """
    for msg in messages_to_retry:
        queue_name: str = msg["queue_name"]
        message_id: Optional[str] = msg["message_id"]
        original_destination: Optional[str] = msg["original_destination"]

        if not original_destination:
            LOGGER.warning(
                "Cannot determine original destination for message %s, skipping.",
                message_id,
            )
            continue

        mbean: Optional[str] = queue_mbeans.get(queue_name)
        if not mbean:
            LOGGER.warning(
                "Cannot find MBean for queue %s, skipping message %s.",
                queue_name,
                message_id,
            )
            continue

        operation_payload = {
            "type": "exec",
            "mbean": mbean,
            "operation": "moveMessageTo",
            "arguments": [message_id, original_destination],
        }

        try:
            response = requests.post(
                jolokia_url,
                json=operation_payload,
                auth=auth,
                headers=headers,
                verify=verify_ssl,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            LOGGER.info(
                "Successfully retried message %s to %s",
                message_id,
                original_destination,
            )
        except requests.exceptions.RequestException as error:
            LOGGER.error("Error moving message %s: %s", message_id, error)


def main() -> None:
    """
    Main function to process DLQs and messages.
    """
    args = parse_args()
    host: str = args.host
    age_limit_hours: int = args.age_limit
    jolokia_api_path: str = args.jolokia_api_path
    dlq_prefixes: Tuple[str, ...] = tuple(args.dlq_prefixes)
    special_queues: Set[str] = set(args.special_queues)
    verify_ssl: bool = args.verify_ssl

    username, password = get_credentials()
    auth: Tuple[str, str] = (username, password)

    headers: Dict[str, str] = build_headers(host)
    jolokia_url: str = get_jolokia_url(host, jolokia_api_path)

    mbeans: List[str] = fetch_mbeans(jolokia_url, headers, auth, verify_ssl)
    queue_mbeans: Dict[str, str] = extract_queue_mbeans(mbeans)

    messages: List[Dict[str, Any]] = []
    for queue_name, mbean in queue_mbeans.items():
        if not queue_name.startswith(dlq_prefixes):
            continue

        message_data: List[Dict[str, Any]] = browse_queue(
            jolokia_url, mbean, headers, auth, verify_ssl
        )
        for msg in message_data:
            parsed_msg: Dict[str, Any] = parse_message(msg)
            if parsed_msg["age_hours"] is not None:
                parsed_msg["queue_name"] = queue_name
                messages.append(parsed_msg)

    display_messages(messages, age_limit_hours)

    if not messages:
        LOGGER.info("No messages to retry.")
        sys.exit(0)

    confirm_action()

    # Determine messages to retry based on queue and age
    messages_to_retry: List[Dict[str, Any]] = [
        msg
        for msg in messages
        if msg["queue_name"] in special_queues or msg["age_hours"] <= age_limit_hours
    ]

    if not messages_to_retry:
        LOGGER.info("No messages meet the criteria for retrying.")
        sys.exit(0)

    retry_messages(
        messages_to_retry, queue_mbeans, jolokia_url, headers, auth, verify_ssl
    )


if __name__ == "__main__":
    main()

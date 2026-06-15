import json
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.handle_http_request import (ParseError, deliver_notification,
                                     lambda_handler, parse_data)


def test_parse_data():
    """Asserts data is parse correctly or raises correct exception."""
    with pytest.raises(ParseError) as err:
        parse_data({})
    assert str(err.value) == 'Data received did not have expected structure. {}'

    output = parse_data({"package_id": "12345", "archivematica_uuid": "54321"})
    assert output == ("12345", "54321")


@mock_aws
def test_deliver_notification():
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = sns.create_topic(
        Name='my-topic.fifo',
        Attributes={
            "FifoTopic": 'true',
            "ContentBasedDeduplication": 'true',
        }
    )['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    queue_name = "test-queue.fifo"
    sqs_conn.create_queue(
        QueueName=queue_name,
        Attributes={
            "FifoQueue": 'true',
            "ContentBasedDeduplication": 'true',
        })
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:{queue_name}",
    )

    config = {"AWS_SNS_TOPIC": topic_arn}
    package_id = "12345"
    archivematica_uuid = "54321"
    deliver_notification(config, package_id, archivematica_uuid)

    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['Message'] == json.dumps(
        {"identifiers": {"archivematica_uuid": archivematica_uuid}})
    assert message_body['MessageAttributes']['outcome']['Value'] == 'SUCCESS'
    assert message_body['MessageAttributes']['package_id']['Value'] == package_id
    assert message_body['MessageAttributes']['service']['Value'] == 'digital_ingest_webhook'


@patch('src.handle_http_request.get_config')
@patch('src.handle_http_request.parse_data')
@patch('src.handle_http_request.deliver_notification')
def test_lambda_handler(mock_notification, mock_parse, mock_config):
    event_data = {
        "body": "{\"package_id\": \"12345\", \"archivematica_uuid\": \"54321\"}"}
    mock_parse.return_value = ("12345", "54321")
    config = {"AWS_SNS_TOPIC": "987654321"}
    mock_config.return_value = config

    output = lambda_handler(event_data, None)

    mock_config.assert_called_once()
    mock_parse.assert_called_once_with(
        {"package_id": "12345", "archivematica_uuid": "54321"})
    mock_notification.assert_called_once_with(config, "12345", "54321")
    assert output == {
        'statusCode': 200,
        'body': 'Notification for package 12345 sent successfully.'
    }

    mock_notification.reset_mock()

    mock_parse.side_effect = ParseError(
        "Data received did not have expected structure")
    output = lambda_handler(event_data, None)
    mock_notification.assert_not_called()
    assert output == {
        "statusCode": 500,
        "body": "Failed to handle request: Data received did not have expected structure"
    }

    mock_notification.reset_mock()

    mock_config.side_effect = Exception("Error loading SSM config")
    output = lambda_handler(event_data, None)
    mock_notification.assert_not_called()
    assert output == {
        "statusCode": 500,
        "body": "Failed to handle request: Error loading SSM config"}

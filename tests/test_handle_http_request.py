import json
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.handle_http_request import (AuthenticationError, ParseError,
                                     authorize, deliver_notification,
                                     lambda_handler, parse_data)


def test_authorize(monkeypatch):
    """Asserts authorization raises expected exceptions."""
    with pytest.raises(AuthenticationError) as err:
        authorize({})
    assert str(err.value) == "Missing API key"

    monkeypatch.setenv('ARCHIVEMATICA_API_KEY', '12345')
    with pytest.raises(AuthenticationError) as err:
        authorize({'headers': {'x-api-key': '54321'}})
    assert str(err.value) == "Invalid API key"

    authorize({'headers': {'x-api-key': '12345'}})


def test_parse_data():
    """Asserts data is parse correctly or raises correct exception."""
    with pytest.raises(ParseError) as err:
        parse_data({})
    assert str(err.value) == 'Data received did not have expected structure. {}'

    output = parse_data({"package_id": "12345"})
    assert output == "12345"


@ mock_aws
def test_deliver_notification():
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = sns.create_topic(Name='my-topic')['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    config = {"AWS_SNS_TOPIC": topic_arn}
    package_id = "12345"
    deliver_notification(sns, config, package_id)

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['outcome']['Value'] == 'SUCCESS'
    assert message_body['MessageAttributes']['package_id']['Value'] == package_id
    assert message_body['MessageAttributes']['service']['Value'] == 'webhook'


@patch('src.handle_http_request.authorize')
@patch('src.handle_http_request.get_config')
@patch('src.handle_http_request.get_client_with_role')
@patch('src.handle_http_request.parse_data')
@patch('src.handle_http_request.deliver_notification')
def test_lambda_handler(mock_notification, mock_parse,
                        mock_client, mock_config, mock_authorize):
    event_data = {"body": "{\"package_id\": \"12345\"}"}
    mock_parse.return_value = "12345"
    mock_client.return_value = 'mock_client'
    config = {"AWS_SNS_TOPIC": "987654321"}
    mock_config.return_value = config

    output = lambda_handler(event_data, None)

    mock_authorize.assert_called_once_with(event_data)
    mock_config.assert_called_once()
    mock_client.assert_called_once_with('sns', config)
    mock_parse.assert_called_once_with({"package_id": "12345"})
    mock_notification.assert_called_once_with('mock_client', config, "12345")
    assert output == 'Notification for package 12345 sent successfully.'

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

    mock_authorize.side_effect = AuthenticationError("Invalid API key")
    output = lambda_handler(event_data, None)
    mock_notification.assert_not_called()
    assert output == {"statusCode": 403, "body": "Invalid API key"}

import json
import logging
import traceback
from os import getenv

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

full_config_path = f"/{getenv('ENV')}/{getenv('APP_CONFIG_PATH')}"


class AuthenticationError(Exception):
    pass


class ParseError(Exception):
    pass


def get_config(ssm_parameter_path):
    """Fetch config values from Parameter Store.

    Args:
        ssm_parameter_path (str): Path to parameters

    Returns:
        configuration (dict): all parameters found at the supplied path.
    """
    configuration = {}
    try:
        ssm_client = boto3.client(
            'ssm',
            region_name=getenv('AWS_REGION'))

        param_details = ssm_client.get_parameters_by_path(
            Path=ssm_parameter_path,
            Recursive=False,
            WithDecryption=True)

        for param in param_details.get('Parameters', []):
            param_path_array = param.get('Name').split("/")
            section_position = len(param_path_array) - 1
            section_name = param_path_array[section_position]
            configuration[section_name] = param.get('Value')

    except BaseException:
        logging.error("Encountered an error loading config from SSM.")
        traceback.print_exc()
    finally:
        return configuration


def authorize(event, config):
    """Checks API Key header to make sure request is authorized."""
    logging.debug('Attempting authorization')
    try:
        api_key = event['headers']['x-api-key']
        assert api_key == config.get('ARCHIVEMATICA_API_KEY')
    except KeyError:
        raise AuthenticationError("Missing API key")
    except AssertionError:
        raise AuthenticationError("Invalid API key")


def parse_data(body):
    """Returns data from request body.

    Args:
        body (dict): Request body

    Returns:
        packge_id (tuple of strings): attribute parsed from body.
    """
    logging.debug(f'Parsing data from body: {body}')
    try:
        package_id = body['package_id']
        archivematica_uuid = body['archivematica_uuid']
        return package_id, archivematica_uuid
    except KeyError:
        raise ParseError(
            f'Data received did not have expected structure. {body}')


def deliver_notification(config, package_id, archivematica_uuid):
    """Send SNS message about successful job.

    Args:
        config (dict): Configuration values
        package_id (str): Package identifier
        archivematicat_uuid (str): Archivematica UUID
    """
    client = boto3.client(
        'sns',
        region_name=getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    client.publish(
        TopicArn=config.get('AWS_SNS_TOPIC'),
        MessageGroupId=f'digital_ingest_webhook-{package_id}',
        Message=json.dumps(
            {'identifiers': {'archivematica_uuid': archivematica_uuid}}),
        MessageAttributes={
            'package_id': {
                'DataType': 'String',
                'StringValue': package_id,
            },
            'service': {
                'DataType': 'String',
                'StringValue': 'digital_ingest_webhook',
            },
            'outcome': {
                'DataType': 'String',
                'StringValue': 'SUCCESS',
            },
            'message': {
                'DataType': 'String',
                'StringValue': f'Post store webhook for {package_id} received.',
            },
        })
    logging.debug('Notification delivered.')


def lambda_handler(event, context):
    try:
        config = get_config(full_config_path)
        authorize(event, config)
        package_id, archivematica_uuid = parse_data(json.loads(event['body']))
        deliver_notification(
            config,
            package_id,
            archivematica_uuid)
        logging.info(
            f'Notification for package {package_id} sent successfully.')
        return f'Notification for package {package_id} sent successfully.'
    except AuthenticationError as e:
        logging.error(f"Authentication error: {str(e)}")
        return {
            "statusCode": 403,
            "body": str(e)
        }
    except Exception as e:
        logging.error(f"Failed to handle request: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Failed to handle request: {str(e)}"
        }

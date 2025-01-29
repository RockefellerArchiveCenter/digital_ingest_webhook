import json
import logging
import traceback
from os import environ

import boto3
from aws_assume_role_lib import assume_role

logger = logging.getLogger()
logger.setLevel(logging.INFO)

full_config_path = f"/{environ.get('ENV')}/{environ.get('APP_CONFIG_PATH')}"


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
            region_name=environ.get('AWS_REGION'))

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
        print("Encountered an error loading config from SSM.")
        traceback.print_exc()
    finally:
        return configuration


def get_client_with_role(resource, config):
    """Gets Boto3 client which authenticates with a specific IAM role."""
    session = boto3.Session()
    assumed_role_session = assume_role(
        session,
        config.get('AWS_ROLE_ARN'),
        region_name=config.get('AWS_REGION'))
    return assumed_role_session.client(resource)


def parse_data(event):
    # parse data, throw error if missing identifier or package id
    # Return archivematica uuid and package id (both of which are in data)
    pass


def deliver_success_notification(
        client, config, archivematica_uuid, package_id):
    """Send SNS message about successful job.

    Args:
        package_path (pathlib.Path): location of the package binary
        data (dict): data about the package
    """
    # TODO does the package ID need to be in the data?
    package_data = {'archivematica_uuid': archivematica_uuid}
    client.publish(
        TopicArn=config.get('AWS_SNS_TOPIC'),
        Message=f'Package {package_id} successfully discovered.',
        MessageAttributes={
            'package_id': {
                'DataType': 'String',
                'StringValue': package_id,
            },
            'service': {
                'DataType': 'String',
                'StringValue': 'webhook',
            },
            'outcome': {
                'DataType': 'String',
                'StringValue': 'SUCCESS',
            },
            'package_data': {
                'DataType': 'String',
                'StringValue': json.dumps(package_data),
            },
        })
    logging.debug('Success notification delivered.')


def deliver_failure_notification(client, config, package_id, exception):
    """Send SNS message about failed job.

    Args:
        package_path (pathlib.Path): location of the package binary
        data (dict): data about the package
        exception (Exception): the exception that was thrown.
    """
    tb = ''.join(traceback.format_exception(exception)[:-1])
    client.publish(
        TopicArn=config.get('AWS_SNS_TOPIC'),
        Message=f'Error handlin post store callback for {package_id}.',
        MessageAttributes={
            'package_id': {
                'DataType': 'String',
                'StringValue': package_id,
            },
            'service': {
                'DataType': 'String',
                'StringValue': 'webhook',
            },
            'outcome': {
                'DataType': 'String',
                'StringValue': 'FAILURE',
            },
            'message': {
                'DataType': 'String',
                'StringValue': str(exception),
            },
            'traceback': {
                'DataType': 'String',
                'StringValue': tb,
            }
        })
    logging.debug('Failure notification delivered.')


def lambda_handler(event, context):
    # TODO what is getting passed in here as the event?
    try:
        config = get_config(full_config_path)
        sns_client = get_client_with_role('sns', config)
        package_id, archivematica_uuid = parse_data(event)
        deliver_success_notification(
            sns_client, config, archivematica_uuid, package_id)
    except Exception as e:
        deliver_failure_notification(sns_client, package_id, e)

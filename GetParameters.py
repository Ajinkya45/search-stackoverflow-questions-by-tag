import boto3
import json

def get_parameters(parameter_keys):
    ssm_client = boto3.client('ssm')
    parameters = ssm_client.get_parameters(
        Names=parameter_keys
    )

    if len(parameters['Parameters']) > 0:
        parameter_dict = {parameter['Name']: parameter['Value'] for parameter in parameters['Parameters']}

    return parameter_dict
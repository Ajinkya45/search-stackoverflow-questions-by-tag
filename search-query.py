from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json
import requests
import GetParameters
import os

parameters = {}
def lambda_handler(event, conext):
    global parameters;
    # read parameters from SSM
    parameters_name = [
        '/project-stackoverflow/es/host', 
        '/project-stackoverflow/chime/webhook'
    ]
    parameters = GetParameters.get_parameters(parameters_name)
    
    print(event)
    Credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        Credentials.access_key, 
        Credentials.secret_key,
        'us-east-1',
        'es',
        session_token=Credentials.token
    )

    host = parameters['/project-stackoverflow/es/host'].split("//")[1]
    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    search_todays_result(es)

def search_todays_result(es_client):
    with open('search-query.json', 'r') as query_file:
        query = json.load(query_file)

    try:
        es_response = es_client.search(
            body=query,
            index=os.environ["index"],
            _source = os.environ["source"]
        )
    except Exception as e:
            print("Exception raised - ", e)
    else:
        send_chime_notification(es_response)

def send_chime_notification(payload):
    global parameters;
    hits = [hit['_source'] for hit in payload['hits']['hits']]

    if len(hits) > 0:
        msg = {
            "Content": json.dumps(hits, indent=4, sort_keys=True)
        }
    else:
        msg = {
            "Content": "no new questions"
        }

    chime_webhook = parameters['/project-stackoverflow/chime/webhook']
    try:
        resp = requests.post(chime_webhook, json=msg, headers={'Content-Type': 'application/json'})
    except Exception as e:
        print("ES exception -", e)
    else:
        print(resp.json())

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json
import requests

sts_client = client = boto3.client('sts')
host = 'search-new-domain-hv5z4dbp4nkgacx32iwvixrudu.us-east-1.es.amazonaws.com'
chime_webhook = 'https://hooks.chime.aws/incomingwebhooks/4fbfcf11-eed0-4b2c-8a93-3ed6fb391c88?token=TTJYdzY0YnB8MXxkR0hYTU1HWHVKRGNOa3ZMRGZsclRCTngzSDVNT0d3RWdmSTFLQ0RON0hF'
query = {
    "query": {
        "range": {
            "creation_date": {
                "gte": "now-1d",
                "lt": "now"
            }
        }
    }
}

def lambda_handler(event, conext):

    print(event)
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials['AccessKeyId'], 
        credentials['SecretAccessKey'],
        'us-east-1',
        'es',
        session_token=credentials['SessionToken']
    )

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    search_todays_result(es, query)

def search_todays_result(es_client, query):
    es_response = es_client.search(
        body=query,
        index='stackoverflow-index',
        _source = "tag,title,link,is_answered,creation_date"
    )

    send_chime_notification(es_response)


def send_chime_notification(payload):
    hits = [hit['_source'] for hit in payload['hits']['hits']]

    if len(hits) > 0:
        msg = {
            "Content": json.dumps(hits, indent=4, sort_keys=True)
        }
    else:
        msg = {
            "Content": "no new questions"
        }

    try:
        resp = requests.post(chime_webhook, json=msg, headers={'Content-Type': 'application/json'})
    except Exception as e:
        print("ES exception -", e)
    else:
        print(resp.json())

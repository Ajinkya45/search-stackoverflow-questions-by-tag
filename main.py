import os
import time
import math
import requests
import json
import boto3
from requests_aws4auth import AWS4Auth
from datetime import date
import GetParameters

def lambda_handler(event, conext):

    # all query parameters for stackoverflow request
    ToDate = math.floor(time.time())
    FromDate = ToDate - 86400
    Order = "desc"
    Sort = "creation"
    Accepted = False
    Tagged = os.environ['tags'].split(",")
    Site = "stackoverflow"

    # Base API url
    url = "https://api.stackexchange.com/2.2/search/advanced"

    # sending a request
    for tag in Tagged:
        try:
            Response = requests.get(
                url,
                params=[('fromdate', FromDate),
                        ('todate', ToDate),
                        ('order', Order),
                        ('sort', Sort),
                        ('accepted', Accepted),
                        ('tagged', tag),
                        ('site', Site)],
                headers={'Accept-Encoding': 'gzip'})
        except Exception as e:
            print("Exception raised - ", e)
        else:
            FormRequestBody(tag, Response.json())

# a function to form bulk request body
def FormRequestBody(Tag, Questions):
    # read parameters from SSM
    parameters_name = [
        '/project-stackoverflow/es/host', 
        '/project-stackoverflow/s3/bucket/name'
    ]
    parameters = GetParameters.get_parameters(parameters_name)

    if Questions["items"]:

        BulkRequestBody = ""
        S3body = ""
        for q in Questions["items"]:
            action = {"index" : {"_id": q["question_id"]}}
            
            q["creation_date"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(q["creation_date"]))
            q["last_activity_date"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(q["last_activity_date"]))
            q["ingestion_date"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(math.floor(time.time())))
            # adding seperate "tag" field to create viz in quicksight which do not support array
            q['tag'] = Tag
            source = json.dumps(q)

            BulkRequestBody += "\n".join([json.dumps(action), source]) + "\n"
            S3body += source + "\n"

        pushToES(BulkRequestBody, parameters)
        pushToS3(S3body, Tag, parameters)

    else:
        print("no questions to push to elasticsearch for tag", Tag)
        print("-----------------------------------------------------------------------")

def pushToES(body, params):

    # AWS elasticsearch request parameters
    Host = params['/project-stackoverflow/es/host']
    Region = 'us-east-1'
    Service = 'es'
    Credentials = boto3.Session().get_credentials()
    Awsauth = AWS4Auth(Credentials.access_key, Credentials.secret_key, Region, Service, session_token=Credentials.token)

    Path = "/" + os.environ["index"] + "/_bulk"
    EsUrl = Host + Path

    # Elasticsearch bulk request
    try:
        EsResponse = requests.put(EsUrl, data=body,
            headers={'Content-Type': 'application/x-ndjson'}, auth=Awsauth)
    except Exception as e:
        print("ES exception -", e)
        print("-----------------------------------------------------------------------")
    else:
        print(EsResponse.json())
        print("-----------------------------------------------------------------------")

def pushToS3(body, Tag, params):

    # S3 client
    client = boto3.client('s3')
    dt = date.today()
    key = "year=" + str(dt.year) + "/month=" + str(dt.month) + "/day=" + str(dt.day) + "/" + Tag

    s3Response = client.put_object(
        Body = body,
        Bucket = params['/project-stackoverflow/s3/bucket/name'],
        Key = key
    )

    print(s3Response)
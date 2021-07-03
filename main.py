import time
import math
import requests
import json
import boto3
from requests_aws4auth import AWS4Auth
from datetime import date

def lambda_handler(event, conext):

    # all query parameters for stackoverflow request
    ToDate = math.floor(time.time())
    FromDate = ToDate - 86400
    Order = "desc"
    Sort = "creation"
    Accepted = False
    Tagged = ["aws-elasticsearch", "amazon-kinesis", "amazon-kinesis-firehose", "amazon-kinesis-analytics", "amazon-elasticsearch", "aws-msk"]
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

    if Questions["items"]:

        BulkRequestBody = ""
        S3body = ""
        for q in Questions["items"]:
            action = {"index" : {"_id": q["question_id"]}}
            
            q["creation_date"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(q["creation_date"]))
            q["last_activity_date"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(q["last_activity_date"]))
            q["ingestion_date"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(math.floor(time.time())))
            source = json.dumps(q)

            BulkRequestBody += "\n".join([json.dumps(action), source]) + "\n"
            S3body += source + "\n"

        pushToES(BulkRequestBody)
        pushToS3(S3body, Tag)

    else:
        print("no questions to push to elasticsearch for tag", Tag)
        print("-----------------------------------------------------------------------")

def pushToES(body):

    # AWS elasticsearch request parameters
    Host = 'https://search-new-domain-hv5z4dbp4nkgacx32iwvixrudu.us-east-1.es.amazonaws.com/'
    Region = 'us-east-1'
    Service = 'es'
    Credentials = boto3.Session().get_credentials()
    Awsauth = AWS4Auth(Credentials.access_key, Credentials.secret_key, Region, Service, session_token=Credentials.token)

    Path = "stackoverflow-index/_bulk"
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

def pushToS3(body, Tag):

    # S3 client
    client = boto3.client('s3')
    dt = date.today()
    key = "year=" + str(dt.year) + "/month=" + str(dt.month) + "/day=" + str(dt.day) + "/" + Tag

    s3Response = client.put_object(
        Body = body,
        Bucket = 'stackoverflow-questions-bucket',
        Key = key
    )

    print(s3Response)
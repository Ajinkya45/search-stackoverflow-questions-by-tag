import boto3
import redshift_connector
from datetime import datetime, timedelta, date
import os

def lambda_handler(event, conext):

    client_rs = boto3.client(service_name='redshift')
    client_s3 = boto3.client(service_name='s3')
    s3_bucket = os.environ["bucket"]

    # get temporary credentials to run copy command
    try:
        redshift_credentials = client_rs.get_cluster_credentials(
            DbUser=os.environ["user"],
            DbName=os.environ["db"],
            ClusterIdentifier=os.environ["clustername"],
            AutoCreate=True,
            DbGroups=[os.environ["usergroup"]]
        )
    except Exception as e:
            print("Exception raised while getting cluster credentials- ", e)
    else:
        print("got credentials successfully")

    try:
        connection = redshift_connector.connect(
            host=os.environ["rshost"],
            port=5439,
            database=os.environ["db"],
            user=redshift_credentials['DbUser'],
            password=redshift_credentials['DbPassword']
        )
    except Exception as e:
            print("Exception raised while establishing connection - ", e)
    else:
        print("connection established")

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT ingestion_date FROM stackquestions order by ingestion_date desc limit 1")
        result = cursor.fetchall()
    except Exception as e:
            print("Exception raised in select statement - ", e)

    # get difference between todays date and latest date in RS table 
    todays_date = date.today()
    latest_date_in_table = result[0][0].date()
    date_difference = (todays_date - latest_date_in_table).days
    print(f"redshift table is {date_difference} behind current date")
    
    for day in range(date_difference):

        # form S3 path for copy command
        next_date = latest_date_in_table + timedelta(days=day+1)
        year = next_date.year
        month = next_date.month
        day = next_date.day

        s3_path = "year=" + str(year) + "/month=" + str(month) + "/day=" + str(day)


        if is_s3_path_exist(client_s3, s3_bucket, s3_path):
            s3_uri = "s3://" + s3_bucket + "/" + s3_path
            copy_command = "copy stackquestions from '" + s3_uri + "' iam_role 'arn:aws:iam::649687724644:role/redshift-test-role' format as json 's3://stackoverflow-questions-bucket-copy/stackquestion_redshift_jsonpath.json' dateformat 'auto' timeformat 'auto';"

            try:
                print(f"copying from prefix - {s3_uri}")
                cursor.execute(copy_command)
            except Exception as e:
                print(f"Exception raised while running copy command - {e}")
    
    try:
        connection.commit()
        connection.close()
    except Exception as e:
            print(f"Exception raised - {e}")
    else:
        print(f"successfully copied to redshift")

# check if s3 path exist or not
def is_s3_path_exist(client, bucket, path):
    try:
        s3_response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=path
        )
    except Exception as e:
        print(f"Exception in s3 list objects - {e}")
    else:
        # checking on "KeyCount" response field. Request will return zero if path provided in prefix do not exist
        if s3_response["KeyCount"] > 0:
            return True

    return False
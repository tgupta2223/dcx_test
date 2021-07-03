import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
import psycopg2


def process_data(file_location):
    df = pd.read_csv(file_location)
    df = (
        df.drop('volume', 1)
            .join(df.volume.map(eval).apply(lambda x: list(x.items())).explode().apply(
            lambda x: pd.Series(x, ['symbol', 'volume'])))
    )
    df.to_csv("processed_data.csv", index=False)


def pandas_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def s3_to_db():
    conn = psycopg2.connect(
        host='redshift-cluster-2.ccdnvfr0tsh2.us-east-2.redshift.amazonaws.com',
        user='awsuser',
        port=5439,
        password='Awspassword2223',
        dbname='dev')

    cur = conn.cursor()

    sql = """copy users_coindcx from 's3://iakshaybucket/data_processing.csv'
        access_key_id 'AKIA5BU5MSRFIDZ2Z6MM'
        secret_access_key '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'
        region 'us-east-2'
        ignoreheader 1
        null as 'NA'
        removequotes
        delimiter ',';"""
    cur.execute(sql)
    conn.commit()


if __name__ == '__main__':
    # Always use .env file to store your credentials
    ACCESS_KEY = 'AKIA5BU5MSRFIDZ2Z6MM'
    SECRET_KEY = 'SF+thvQ50WxMDgS0dAjykTDyJT6n1qspllc+0klp'

    file_path = "C:\\Users\\tgupta\\Desktop\\data_processing.csv"
    process_data(file_path)

    uploaded = pandas_to_s3('processed_data.csv', 'iakshaybucket', 'data_processing.csv')

    s3_to_db()
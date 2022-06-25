import os
from sys import prefix
from numpy import array
from collections import Counter
import datetime as dt
import boto3
from requests_html import HTMLSession
from io import StringIO
from datetime import timedelta
import praw
import pandas as pd
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import glob

#These are the variables used to access the reddit API
reddit_authorized = praw.Reddit(client_id=Variable.get('client_id'),      
                                client_secret=Variable.get('client_secret'),      
                                user_agent=Variable.get('user_agent'),        
                                username=Variable.get('username'),        
                                password=Variable.get('password'))        # 
subreddit = reddit_authorized.subreddit("cryptocurrency")
posts = subreddit.top("day")

# Our bucket
BUCKET_NAME = "crypto_gerard_de1"
# Folder names for if raw data is needed
CRYPTO = "combined_data/"
COINS = "coins_data/"
REDDIT = "reddit_data/"
#dictionary used to create Reddit DF
posts_dict = {"Title": [], "Post Text": [],
              "ID": [], "Score": [],
              "Total Comments": [], "Post URL": []
              }
#Upload string used for uploading the combined data
def upload_crypto_string_to_gcs(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    gcs_resource.Object(BUCKET_NAME, CRYPTO + uploaded_filename).put(Body=csv_body.getvalue())
#Upload string used if raw coin data is needed in bucket
def upload_coins_string_to_gcs(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    gcs_resource.Object(BUCKET_NAME, COINS + uploaded_filename).put(Body=csv_body.getvalue())
#Upload string used if raw crypto data is needed in bucket
def upload_reddit_string_to_gcs(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    gcs_resource.Object(BUCKET_NAME, COINS + uploaded_filename).put(Body=csv_body.getvalue())

@task(task_id="coins")              
def coingecko_top100():
    prefix = COINS
    session = HTMLSession()
    home_dir = os.getcwd()
    output_dir = f'{home_dir}/logs/unprocessedcoins'
    Path(output_dir).mkdir(parents=True, exist_ok=True) 
    s = session.get('https://www.coingecko.com/en')
    df = pd.read_html(s.text)[0]
    df = df[["Coin",'Price','24h','Mkt Cap']]
    df['Symbol']=df['Coin']
    df['Coin'] = df['Coin'].apply(lambda x: x.split('  ')[0])
    df['Symbol'] = df['Symbol'].apply(lambda x: x.split('  ')[1])
    df['time_scraped'] = ([dt.datetime.now()] * len(df))
    df["Price"] = df["Price"].replace(",", "", regex=True)
    df["Mkt Cap"] = df["Mkt Cap"].replace(",", "", regex=True)
    df['Price'] = df['Price'].str.strip('$')
    df['Mkt Cap'] = df['Mkt Cap'].str.strip('$')
    # csv_buffer = StringIO()
    # df.to_csv(csv_buffer)
    # time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    # filename = 'coins' + '_' + time_now + '.csv'
    # upload_coins_string_to_gcs(csv_body=csv_buffer, uploaded_filename=filename)
    df.to_csv(f"{output_dir}/coindata.csv", index = False)
    print(f"file at: {output_dir}/coindata.csv")


@task(task_id="reddit")
def scrape_reddit():
    prefix = REDDIT
    subreddit = reddit_authorized.subreddit('cryptocurrency')
    home_dir = os.getcwd()
    output_dir = f'{home_dir}/logs/unprocessedreddit'
    Path(output_dir).mkdir(parents=True, exist_ok=True) 
    posts = subreddit.top("day")
    for post in posts:
        posts_dict["Title"].append(post.title)
    
        posts_dict["Post Text"].append(post.selftext)
     
        posts_dict["ID"].append(post.id)
     
        posts_dict["Score"].append(post.score)
     
        posts_dict["Total Comments"].append(post.num_comments)
     
    # URL of each post
        posts_dict["Post URL"].append(post.url)
    redditdata = pd.DataFrame(posts_dict)
    redditdata['time_scraped'] = ([dt.datetime.now()] * len(redditdata))
    # csv_buffer = StringIO()
    # redditdata.to_csv(csv_buffer)
    # time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    # filename = 'reddit' + '_' + time_now + '.csv'
    # upload_crypto_string_to_gcs(csv_body=csv_buffer, uploaded_filename=filename)
    redditdata['Post Text'] = redditdata['Post Text'].fillna('notext')
    redditdata.to_csv(f"{output_dir}/redditdata.csv", index = False)
    print(f"file at: {output_dir}/redditdata.csv")
@task(task_id='validation_reddit')
def data_val_reddit():
    home_dir = os.getcwd()
    redditdir = f'{home_dir}/logs/unprocessedreddit'
    df = pd.read_csv(redditdir + '/redditdata.csv')
    for col in df.columns:
        miss = df[col].isnull().sum()
        if miss>0:
            print("{} has {} missing value(s)".format(col,miss))
        else:
            print("{} has NO missing value!".format(col)) 
    
@task(task_id='validation_coins')
def data_val_coins():
    home_dir = os.getcwd()
    coindir = f'{home_dir}/logs/unprocessedcoins'
    df = pd.read_csv(coindir + '/coindata.csv')
    for col in df.columns:
        miss = df[col].isnull().sum()
        if miss>0:
            zprint("{} has {} missing value(s)".format(col,miss))
        else:
            print("{} has NO missing value!".format(col)) 

@task(task_id='wordcount')
def word_count():
    home_dir = os.getcwd()
    redditdir = f'{home_dir}/logs/unprocessedreddit'
    coindir = f'{home_dir}/logs/unprocessedcoins'
    output_dir = f'{home_dir}/logs/processedreddit'
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    d1 = pd.read_csv(redditdir+ '/redditdata.csv')
    d2= pd.read_csv(coindir + '/coindata.csv')
    d2['Coin'] = d2['Coin'].str.lower()
    d2['Symbol'] = d2['Symbol'].str.lower()
    d1['Title'] = d1['Title'].str.lower()
    d1['Post Text'] = d1['Post Text'].str.lower()
    coinlist=d2['Coin'].unique()
    symbollist=d2['Symbol'].unique()
    d1['mention_title'] = d1['Title'].str.count(r'\b|\b'.join(coinlist)) + d1['Title'].str.count(r'\b|\b'.join(symbollist))
    d1['mention_pt'] = d1['Post Text'].str.count(r'\b|\b'.join(coinlist)) + d1['Post Text'].str.count(r'\b|\b'.join(symbollist))
   # d1.to_csv(f"{output_dir}/processedredditdata.csv", index = False)
    #print(f"file at: {output_dir}/processedredditdata.csv")
    csv_buffer = StringIO()
    d1.to_csv(csv_buffer)
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = 'redditprocessed' + '_' + time_now + '.csv'
    upload_crypto_string_to_gcs(csv_body=csv_buffer, uploaded_filename=filename)

with DAG(
    'Crypto_Scraper_Wordcount',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        # 'depends_on_past': False,
        # 'email': ['caleb@eskwelabs.com'],
        # 'email_on_failure': False,
        # 'email_on_retry': False,
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='Scraping crypto data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 15),
    catchup=False,
    tags=['scrapers'],
) as dag:

    t1 = EmptyOperator(task_id="start_message")

    t2=coingecko_top100()
    
    t2a=data_val_coins()

    t3=scrape_reddit()
    
    t3a=data_val_reddit()

    t4=word_count()

    t_end = EmptyOperator(task_id="end_message")
    


    t1 >> t2
    t1 >> t3
    t2 >> t2a
    t3 >> t3a
    t2a >> t4
    t3a >> t4
    t4 >> t_end

    #t1 >> [t2 >> t2a, t3] >> t4 >> t_end
    #t1 >> [t2 >> t2a, t3 >> t3a] >> t4 >> t_end
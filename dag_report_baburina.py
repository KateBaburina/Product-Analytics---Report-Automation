from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import numpy as np
import seaborn as sns
import pandas as pd
import pandahouse as ph
import io
import matplotlib.pyplot as plt
   
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220720',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

# Default parameteres that will be used in tasks
default_args = {
    'owner': 'e-baburina-9',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 5),
}

# DAG schedule interval - cron - 11 am every day
schedule_interval = '0 11 * * *'

my_token = os.environ.get("REPORT_BOT_TOKEN")
bot = telegram.Bot(token = my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_baburina():
    
    @task()
    def message(chat=None):
        chat_id = chat or -770113521


        query = '''
        select 
           uniq(user_id) as DAU,
           countIf(action = 'view') as Views, 
           countIf(action = 'like') as Likes, 
           round(countIf(action = 'like')/countIf(action = 'view'),2) as CTR
           from {db}.feed_actions 
           where toDate(time) = yesterday()
           order by Views desc'''
        
        df = ph.read_clickhouse(query, connection=connection)
        msg = f'Newsfeed metrics for yesterday:\nDAU: {df.DAU.sum()}\nViews: {df.Views.sum()} \nLikes: {df.Likes.sum()}\nCTR: {df.CTR.sum()}'
        bot.sendMessage(chat_id=chat_id, text=msg)
  
    @task()
    def report(chat=None):
        chat_id = chat or -770113521
        
        query1 = '''
        select 
           toDate(time) as Day,
           uniq(user_id) as DAU,
           countIf(action = 'view') as Views, 
           countIf(action = 'like') as Likes, 
           round(countIf(action = 'like')/countIf(action = 'view'),2) as CTR,
           countIf(source = 'ads') as Ads,
           countIf(source = 'organic') as Organic
           from {db}.feed_actions 
           WHERE toDate(time) >= yesterday()-6 and toDate(time) <= yesterday()
           GROUP BY Day'''

        df1 = ph.read_clickhouse(query1, connection=connection)
        font = {'family': 'serif',
        'color':  'darkviolet',
        'weight': 'normal',
        'size': 10,
        }

        x1 = df1.Day
        x2 = df1.Day

        y1 = df1.DAU
        y2 = df1.CTR

        plt.figure(figsize=(15, 8))


        plt.subplot(3, 2, 1)
        plt.title('Key metrics for the last week', horizontalalignment='center', fontdict=font)
        plt.plot(x1, y1)
        plt.ylabel('DAU', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        plt.subplot(3, 2, 2)
        plt.plot(x2, y2)
        plt.ylabel('CTR', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        x3 = df1.Day
        x4 = df1.Day

        y3 = df1.Likes
        y4 = df1.Views

        plt.subplot(3, 2, 3)
        plt.plot(x3, y3)
        plt.ylabel('Likes', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        plt.subplot(3, 2, 5)
        plt.plot(x4, y4)
        plt.ylabel('Views', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        x5 = df1.Day
        x6 = df1.Day

        y5 = df1.Ads
        y6 = df1.Organic

        plt.subplot(3, 2, 4)
        plt.plot(x5, y5)
        plt.ylabel('Ads', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        plt.subplot(3, 2, 6)
        plt.plot(x6, y6)
        plt.ylabel('Organic', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        plt.show()
        # Creating the file object, so we do not need to store the plot locally to save space
        plot_object =io.BytesIO()
        plt.savefig(plot_object)
        # Moving the cursor at the beginning of the file object
        plot_object.seek(0)
        # Называем объект
        plot_object.name = 'Feed_metrics_plot.png'
        # Closing matplotlib.pyplot
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    
    message()
    report()
    
    
dag_report_baburina = dag_report_baburina()   

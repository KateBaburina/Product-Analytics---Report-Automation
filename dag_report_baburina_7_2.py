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
def dag_report_baburina_app():
    
    @task()
    def message(chat=None):
        chat_id = chat or -770113521
        group_id = -770113521


        query = '''
        select 
                   uniq(user_id) as DAU,
                   countIf(action = 'view') as Views, 
                   countIf(action = 'like') as Likes, 
                   round(countIf(action = 'like')/countIf(action = 'view'),2) as CTR,
                   countIf(source = 'ads') as Ads,
                   countIf(source = 'organic') as Organic,
                   countIf(os = 'Android') as Android,
                   countIf(os = 'iOS') as iOS
                   from {db}.feed_actions 
                   where toDate(time) = yesterday()
                   order by Views desc
        '''

        df = ph.read_clickhouse(query, connection=connection)
        
        query2 = '''
        select 
                   toDate(time) as Day,
                   uniq(user_id) as DAU_messenger
                   from {db}.message_actions 
                   WHERE toDate(time) >= yesterday()-6 and toDate(time) <= yesterday()
                   GROUP BY Day
        '''

        df2 = ph.read_clickhouse(query2, connection=connection)
        
        query3 = '''
        select 
           toDate(time) as Day,
           uniq(user_id) as DAU_messenger
           from {db}.message_actions 
           WHERE toDate(time) = yesterday()
           GROUP BY Day
        '''

        df3 = ph.read_clickhouse(query3, connection=connection)
        
        msg = f'Newsfeed metrics for yesterday:\nDAU: {df.DAU.sum()}\nViews: {df.Views.sum()} \nLikes: {df.Likes.sum()}\nCTR: {df.CTR.sum()}\nOrganic traffic users:{df.Organic.sum()}\nUsers coming from ads:{df.Ads.sum()}\nUsers with Android:{df.Android.sum()}\nUsers with iOS:{df.iOS.sum()}\nMessenger DAU:{df3.DAU_messenger.sum()}'
        bot.sendMessage(chat_id=chat_id, text=msg)
    
  
    @task()
    def report(chat=None):
        chat_id = chat or -770113521
        
        query2 = '''
        select 
                   toDate(time) as Day,
                   uniq(user_id) as DAU_messenger
                   from {db}.message_actions 
                   WHERE toDate(time) >= yesterday()-6 and toDate(time) <= yesterday()
                   GROUP BY Day
        '''

        df2 = ph.read_clickhouse(query2, connection=connection)
        
        query1 = '''
        select 
           toDate(time) as Day,
           uniq(user_id) as DAU,
           countIf(action = 'view') as Views, 
           countIf(action = 'like') as Likes, 
           round(countIf(action = 'like')/countIf(action = 'view'),2) as CTR,
           countIf(source = 'ads') as Ads,
           countIf(source = 'organic') as Organic,
           countIf(os = 'Android') as Android,
           countIf(os = 'iOS') as iOS
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

        x9 = df2.Day
        y9 = df2.DAU_messenger

        plt.figure(figsize=(15, 8))

        plt.subplot(4, 2, 3)
        plt.plot(x1, y1, color='b')
        plt.ylabel('DAU feed', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        plt.subplot(4, 2, 4)
        plt.plot(x9, y9, color='r')
        plt.ylabel('DAU messenger', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        plt.subplot(4, 2, 1)
        plt.title('Key metrics for the last week', horizontalalignment='center', fontdict=font)
        plt.plot(x2, y2)
        plt.ylabel('CTR', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        x3 = df1.Day
        x4 = df1.Day

        y3 = df1.Likes
        y4 = df1.Views

        plt.subplot(4, 2, 7)
        plt.plot(x3, y3, color='b')
        plt.plot(x3, y4, color='r')
        plt.title('Likes - black, views - red', horizontalalignment='center', fontdict=font)
        plt.ylabel('Likes/views', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        x5 = df1.Day
        x6 = df1.Day

        y5 = df1.Ads
        y6 = df1.Organic

        ax4=plt.subplot(4, 2, 8)
        plt.plot(x5, y5, color='b')
        plt.plot(x5, y6, color='g')
        ax4.set_title('Organic - green, ads - blue', horizontalalignment='center', fontdict=font)
        plt.ylabel('Traffic', fontdict=font)
        plt.grid(True)
        plt.xticks(rotation=15)

        x7 = df1.Day
        x8 = df1.Day

        y7 = df1.Android
        y8 = df1.iOS

        ax5=plt.subplot(4, 2, 2)
        plt.grid(True)
        plt.xticks(rotation=15)
        ax5.set_title('Android - yellow, iOS - blue', horizontalalignment='center', fontdict=font)
        plt.ylabel('OS', fontdict=font)
        plt.plot(x7, y7, color='y')
        plt.plot(x7, y8, color='g')

        plt.show()
        # Creating the file object, so we do not need to store the plot locally to save space
        plot_object =io.BytesIO()
        plt.savefig(plot_object)
        # Moving the cursor at the beginning of the file object
        plot_object.seek(0)
        # Naming the object
        plot_object.name = 'Feed_metrics_plot.png'
        # Closing matplotlib.pyplot
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    
    message()
    report()
    
    
dag_report_baburina_app = dag_report_baburina_app()   

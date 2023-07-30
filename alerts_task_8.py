Система должна с периодичность каждые 15 минут проверять ключевые метрики, такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 

Изучить поведение метрик и подберите наиболее подходящий метод для детектирования аномалий.
В случае обнаружения аномального значения, в чат должен отправиться алерт - сообщение со следующей информацией: метрика, ее значение, величина отклонения.
В сообщение можно добавить дополнительную информацию, которая поможет при исследовании причин возникновения аномалии, это может быть, например,  график, ссылки на дашборд/чарт в BI системе. 

Пример шаблона алерта: 

Метрика {metric_name} в срезе {group}. 
Текущее значение {current_x}. Отклонение более {x}%.
[опционально: ссылка на риалтайм чарт этой метрики в BI для более гибкого просмотра]
[опционально: ссылка на риалтайм дашборд в BI для исследования ситуации в целом]
@[опционально: тегаем ответственного/наиболее заинтересованного человека в случае отклонения конкретно 
  этой метрики в этом срезе (если такой человек есть)]
   
[график]

Автоматизировать систему алертов с помощью Airflow.

import telegram
import pandas as pd
import pandahouse as ph
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import sys
from datetime import date, datetime, timedelta


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 't-pozharskaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 12),
}

# Интервал запуска DAG (каждые 15 минут)
schedule_interval = '*/15 * * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230520',
                      'user':'student',
                    'password':'dpo_python_2020'
              }


chat_id = -938659451  # id канала  
#chat_id = 797978378
bot = telegram.Bot(token='5981322866:AAE-nxcLJKljsaeR5WC00bYHTdmMGeg8duA') # получаем доступ


def check_anomaly(df, metric, a=4, n=5):  # a - коэфф, влияющий на ширину интервала; n - коэфф, влияющий на кол-во временных промежутков
    # функция предлагает алгоритм поиска аномалий (межкваритильный размах)
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df
  
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alerts_bot_8():
    @task
    def create_message():
        query = ''' 
            select *
            from 
              (SELECT
                     toStartOfFifteenMinutes(time) as ts
                   , toDate(ts) as date
                   , formatDateTime(ts, '%R') as hm
                   , uniqExact(user_id) as users_lenta
                   , countIf(user_id, action='view') as views
                   , countIf(user_id, action='like') as likes
                   , round(100*(countIf(user_id, action='like') / countIf(user_id, action='view')), 2) as ctr
               FROM simulator.feed_actions
               WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
               GROUP BY ts, date, hm
               ORDER BY ts) t1 
            full join
              (select
                     toStartOfFifteenMinutes(time) as ts
                   , toDate(ts) as date
                   , formatDateTime(ts, '%R') as hm
                   , uniqExact(user_id) as users_messenger
                   , count(user_id) as sent_messages
               from simulator_20230520.message_actions
               WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
               GROUP BY ts, date, hm
               ORDER BY ts) t2
               using ts, date, hm
               order by ts       
        '''
        data = ph.read_clickhouse(query=query, connection=connection)
        return data
    
    @task
    def report_alert(data, chat_id=None):
        chat_id = -938659451
        bot = telegram.Bot(token='5981322866:AAE-nxcLJKljsaeR5WC00bYHTdmMGeg8duA')
               
        metrics_list = ['users_lenta', 'views', 'likes', 'ctr', 'users_messenger', 'sent_messages']
        for metric in metrics_list:
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric) 
            if is_alert == 1:
                msg = '''Метрика {metric}:\n текущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%}\nСсылка на дашборд - {url}'''.format(metric=metric,                                                                                                           current_val=df[metric].iloc[-1],
                                                                                                        last_val_diff= abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2])),                                                                                                                             url='https://superset.lab.karpov.courses/superset/dashboard/3754/') 
                    
                sns.set(rc={'figure.figsize':(16, 10)})
                plt.tight_layout()
                
                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric') 
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')
            
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 15 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                        
                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                
                ax.set_title(metric)
                ax.set(ylim=(0, None))
                
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()
                
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                return plot_object
        
       
        
                    
    data = create_message()
    report_alert(data, chat_id)
    
    
alerts_bot_8 = alerts_bot_8()    

    
import logging
import os
from datetime import datetime

import pandas as pd
import shutil
import pymysql
import requests

logger = logging.getLogger("sync")


def convert_date(date_str):
    return datetime.strptime(date_str, '%Y%m%dT').strftime('%Y-%m-%d')


class Sync:
    def __init__(self, config):
        self.mysql_config = config["mysql_config"]
        self.uuid = config["uuid"]
        self.backend_url = config["backend_url"]

    def scan_img(self):
        file_dir = os.path.split(os.path.realpath(__file__))[0] + os.sep + "weibo/"
        df = pd.read_csv(file_dir+"users.csv")
        users = df['昵称'].tolist()
        user_ids = df['用户id'].tolist()
        f_list = os.listdir(file_dir)
        tweets = {}
        for idx, user in enumerate(users):
            base_dir = file_dir + user
            if not os.path.exists(base_dir):
                continue
            for img in os.listdir(base_dir + '/img/原创微博图片'):
                img_path = base_dir + '/img/原创微博图片/' + img
                file_size = os.path.getsize(img_path)
                if file_size < 1000:
                    continue
                params = img.split('_')
                date = params[0]
                weibo_id = params[1].split('.')[0]
                order = 0
                if len(params) > 2:
                    order = params[2].split('.')[0]
                if weibo_id not in tweets:
                    df_user = pd.read_csv(base_dir + '/{}.csv'.format(user_ids[idx]))
                    tweets[weibo_id] = {
                        'date': date,
                        'author': user,
                        'images': {
                            order: '/static/weibo/' + img
                        },
                        'content': df_user.loc[df_user['id'] == int(weibo_id), '正文'].values[0]
                    }
                else:
                    tweets[weibo_id]['images'][order] = '/static/weibo/' + img
                shutil.move(img_path, '/data/static/weibo/' + img)
        return tweets

    def upload_to_backend(self):
        tweets = self.scan_img()

        url = self.backend_url+"/crawler/token"
        params = {'uuid': self.uuid}
        response = requests.post(url, params=params)
        token = response.json()['data']


        img_count = 0
        post_data_list = []

        for weibo_id in tweets:
            post_data = {}
            tweet = tweets[weibo_id]
            new_tweet = {
                'date': convert_date(tweet['date']),
                'author': tweet['author'],
                'containsImage': len(tweet['images']),
                'note': weibo_id,
                "source": "weibo",
                'content': tweet['content']
            }
            post_data['tweets'] = new_tweet

            image_list = []
            for order in tweets[weibo_id]['images']:
                image_list.append(tweets[weibo_id]['images'][order])
                img_count += 1

            post_data['images'] = image_list
            post_data_list.append(post_data)

        url = self.backend_url + "/crawler/weibo"
        headers = {'X-Auth-Token': token}
        response = requests.post(url, json={'list':post_data_list}, headers=headers)

        return "weibo_crawler: 已获取{}条微博，共{}张图片".format(len(tweets), img_count)

    def mysql_insert_sql(self, data, table):
        values = ",".join(["%s"] * len(data))
        keys = ",".join(data.keys())
        sql = """INSERT INTO {table}({keys}) VALUES ({values})""".format(
            table=table, keys=keys, values=values
        )
        return sql

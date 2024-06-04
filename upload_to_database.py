import logging
import os
from datetime import datetime

import pandas as pd
import shutil
import pymysql

logger = logging.getLogger("sync")


def convert_date(date_str):
    return datetime.strptime(date_str, '%Y%m%dT').strftime('%Y-%m-%d')


class Sync:
    def __init__(self, config):
        self.mysql_config = config["mysql_config"]

    def scan_img(self):
        file_dir = os.path.split(os.path.realpath(__file__))[0] + os.sep + "weibo/"
        df = pd.read_csv(file_dir+"users.csv")
        users = df['昵称'].tolist()
        user_ids = df['用户id'].tolist()
        f_list = os.listdir("weibo")
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

    def upload_to_database(self):
        tweets = self.scan_img()
        connection = pymysql.connect(**self.mysql_config)
        cursor = connection.cursor()
        img_count = 0
        for weibo_id in tweets:
            tweet = tweets[weibo_id]
            new_tweet = {
                'date': convert_date(tweet['date']),
                'author': tweet['author'],
                'contains_image': len(tweet['images']),
                'note': weibo_id,
                "source": "weibo",
                'content': tweet['content']
            }
            sql = self.mysql_insert_sql(new_tweet, "tweets")
            tweet_id = -1
            try:
                cursor.execute(sql, list(new_tweet.values()))
                tweet_id = cursor.lastrowid
            except Exception as e:
                connection.rollback()
                raise e

            if tweet_id == -1:
                continue

            for order in tweets[weibo_id]['images']:
                new_img = {
                    "path": tweets[weibo_id]['images'][order],
                }
                sql = self.mysql_insert_sql(new_img, "images")
                try:
                    cursor.execute(sql, list(new_img.values()))
                    img_id = cursor.lastrowid
                    new_rel = {
                        "image_id": img_id,
                        "tweet_id": tweet_id,
                        "sequence": int(order),
                    }
                    sql = self.mysql_insert_sql(new_rel, "rel_tweets_images")
                    cursor.execute(sql, list(new_rel.values()))
                except Exception as e:
                    connection.rollback()
                    raise e
                img_count += 1
        connection.commit()
        connection.close()
        return "weibo_crawler: 已获取{}条微博，共{}张图片".format(len(tweets), img_count)

    def mysql_insert_sql(self, data, table):
        values = ",".join(["%s"] * len(data))
        keys = ",".join(data.keys())
        sql = """INSERT INTO {table}({keys}) VALUES ({values})""".format(
            table=table, keys=keys, values=values
        )
        return sql

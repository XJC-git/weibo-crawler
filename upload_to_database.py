import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import shutil
import pymysql
import requests

logger = logging.getLogger("sync")


def convert_date(date_str):
    return datetime.strptime(date_str, '%Y%m%dT').strftime('%Y-%m-%d')


class Sync:
    token = None

    def __init__(self, config):
        self.config = config
        self.mysql_config = config["mysql_config"]
        self.uuid = config["uuid"]
        self.backend_url = config["backend_url"]
        if self.token is None:
            self.get_token()
        self.r2 = boto3.client(
            's3',
            endpoint_url=config['ENDPOINT_URL'],
            aws_access_key_id=config['ACCESS_KEY'],
            aws_secret_access_key=config['SECRET_KEY']
        )

    def get_token(self):
        url = self.backend_url + "/crawler/token"
        params = {'uuid': self.uuid}
        response = requests.post(url, params=params)
        self.token = response.json()['data']


    def scan_img(self):
        file_dir = os.path.split(os.path.realpath(__file__))[0] + os.sep + "weibo/"
        df = pd.read_csv(file_dir + "users.csv")
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

                self.upload_image(image_path=img_path)
                os.remove(img_path)
                # shutil.move(img_path, '/data/static/weibo/' + img)
        return tweets

    def upload_image_from_url(self, url):
        # Extract the image file name from the URL
        image_name = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
        try:
            self.r2.head_object(Bucket=self.config['BUCKET_NAME'], Key=image_name)
            print(f"The image already exists in {self.config['BUCKET_NAME']}/{image_name}, skipping")
            return image_name
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                pass

        # Define a temporary path to save the downloaded image
        temp_path = os.path.join("/tmp", image_name)

        try:
            # Download the image
            response = requests.get(url)
            if response.status_code == 200:
                with open(temp_path, 'wb') as f:
                    f.write(response.content)
            else:
                raise Exception(f"Failed to download image from {url}")

            # Upload the image using the upload_image function
            uploaded_file_path = self.upload_image(temp_path)

            # Clean up the temporary file
            os.remove(temp_path)

            return uploaded_file_path
        except Exception as e:
            print(f"Error downloading image: {url}\n{e}")
            return None

    def upload_image(self, image_path):
        try:
            with open(image_path, 'rb') as file:
                file_name = file.name.split('/')[-1]
                self.r2.upload_fileobj(file, self.config['BUCKET_NAME'], file_name)
                print(f"File {image_path} uploaded")
                return file_name
        except Exception as e:
            print(f"Error uploading file: {e}")

    def upload_to_backend(self):
        tweets = self.scan_img()

        token = self.token

        url = self.backend_url + "/crawler/token"
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
        response = requests.post(url, json={'list': post_data_list}, headers=headers)

        return "已获取{}条微博，共{}张图片".format(len(tweets), img_count)

    def log_info(self, msg):
        url = self.backend_url + "/log/crawler/info"
        headers = {'X-Auth-Token': self.token}
        params = {'msg': msg}
        requests.post(url, params=params, headers=headers)

    def log_error(self, msg):
        url = self.backend_url + "/log/crawler/error"
        headers = {'X-Auth-Token': self.token}
        params = {'msg': msg}
        requests.post(url, params=params, headers=headers)

    def mysql_insert_sql(self, data, table):
        values = ",".join(["%s"] * len(data))
        keys = ",".join(data.keys())
        sql = """INSERT INTO {table}({keys}) VALUES ({values})""".format(
            table=table, keys=keys, values=values
        )
        return sql

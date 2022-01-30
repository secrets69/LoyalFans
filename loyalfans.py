import os
import sys
import json
import concurrent.futures
import time
import datetime
import sqlite3
from contextlib import closing
import platform

import requests
from tqdm import tqdm
from blessed import Terminal
from halo import Halo
from dateutil.parser import parse
from win32_setctime import setctime

from logs.logger import Logger


class User:
    def __init__(self):
        with open(os.path.join(sys.path[0], 'config.json')) as f:
            config = json.load(f)['config']
        self.headers = config['headers']
        if settings := config['settings']:
            self.destination_path = settings['destination_path']
            if not self.destination_path:
                self.destination_path = os.getcwd()
            self.separate_file_types = settings['separate_file_types']
            self.download_preview_videos = settings['download_preview_videos']
            self.avoid_duplicates = settings['avoid_duplicates']
            self.use_original_dates = settings['use_original_dates']
            self.timezone = settings['timezone']
            self.debug = settings['debug']
        if urls := config['urls']:
            self.user_url = urls['user_url']
            self.follow_url = urls['follow_url']
            self.profile_url = urls['profile_url']
            self.timeline_url = urls['timeline_url']
            self.messages_url = urls['messages_url']
            self.video_store_url = urls['video_store_url']
        if self.avoid_duplicates:
            self.db_dir = os.path.join(sys.path[0], 'db')
            if not os.path.isdir(self.db_dir):
                os.mkdir(self.db_dir)
            self.conn = sqlite3.connect(os.path.join(self.db_dir, 'models.db'))
        self.log = Logger(self.debug)
        self.term = Terminal()

    def scrape_user(self):
        with requests.Session() as s:
            r = s.get(self.user_url, headers=self.headers)
        if r.ok:
            self.log.debug(self.term.lime(f"{r.status_code} STATUS CODE"))
        else:
            self.log.info(self.term.red(f"{r.status_code} STATUS CODE"))
            self.log.info(self.term.red(self.log.STATUS_ERROR))
            sys.exit(0)
        try:
            following_count = r.json()['following']
        except KeyError:
            self.log.error(self.term.red(self.log.KEY_ERROR))
        return following_count

    def scrape_follow(self, count):
        payload = {
            'limit': count,
        }
        with requests.Session() as s:
            r = s.post(self.follow_url, headers=self.headers, params=payload)
        if r.ok:
            self.log.debug(self.term.lime(f"{r.status_code} STATUS CODE"))
        else:
            self.log.info(self.term.red(f"{r.status_code} STATUS CODE"))
            self.log.info(self.term.red(self.log.STATUS_ERROR))
            sys.exit(0)
        creators = r.json()['followed']
        creators_info_list = [(creator['name'].strip(), creator['slug'])
                              for creator in creators]
        creators_info_list.sort(key=lambda x: x[0].casefold())
        creators_list = list(enumerate(creators_info_list, 1))
        return creators_list


class Model(User):
    def __init__(self, array):
        super().__init__()
        self.creator_list = array
        self.name = None
        self.slug = None
        self.limit = None
        if self.avoid_duplicates:
            self.ids = None

    def menu(self):
        header = ['NUMBER', 'NAME', 'HANDLE']
        FORMAT = '{:<22}' * len(header)
        self.log.info(self.term.underline(FORMAT.format(*header)))
        for c, v in self.creator_list:
            self.log.info(FORMAT.format(c, *v))
        self.log.info(self.term.bold(
            "\nSelect a creator by entering their corresponding number\nor enter a negative number to quit the program"))
        while True:
            try:
                num = int(input('>>> '))
                if num < 0:
                    self.log.info(self.term.lime("Program successfully quit"))
                    sys.exit(0)
                for c, v in self.creator_list:
                    if c == num:
                        self.name, self.slug = v[0], v[1]
                        self.log.info(
                            f"Retrieving {self.term.bold(self.name)}'s page details...")
                        return self.slug
            except ValueError:
                self.log.info(self.term.gold("Please enter a number"))

    def scrape_profile(self):
        with requests.Session() as s:
            r = s.get(self.profile_url.format(self.slug), headers=self.headers)
        if r.ok:
            self.log.debug(self.term.lime(f"{r.status_code} STATUS CODE"))
        else:
            self.log.info(self.term.red(f"{r.status_code} STATUS CODE"))
            self.log.info(self.term.red(self.log.STATUS_ERROR))
            sys.exit(0)
        profile = r.json()
        try:
            num_posts = profile['data']['counters']['posts_total']
            num_photos = profile['data']['counters']['photos']
            num_videos = profile['data']['counters']['videos']
            num_audios = profile['data']['counters']['audios']
            num_store_videos = profile['data']['counters']['store_videos']
            num_videos -= num_store_videos
        except KeyError:
            self.log.error(self.log.KEY_ERROR)
        self.log.info(
            f"According to LoyalFans, {self.term.bold(self.name)} has a total of:")
        self.log.info(self.term.bold(
            f"\t· {num_posts} posts\n\t  — {num_photos} photos\n\t  — {num_videos} videos\n\t  — {num_audios} audios\n\t  — {num_store_videos} store videos"))
        while num_posts % 4 != 0:
            num_posts += 1
        self.limit = num_posts
        if self.avoid_duplicates:
            with closing(self.conn.cursor()) as c:
                c.execute(f'''
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='{self.slug}'
                ''')
                if not c.fetchone():
                    c.execute(
                        f'''CREATE TABLE {self.slug}(
                            ID INTEGER PRIMARY KEY,
                            URL TEXT,
                            TIMESTAMP INTEGER,
                            TYPE TEXT,
                            MEDIA_TYPE TEXT,
                            DATE TEXT,
                            FILE_ID TEXT
                    )''')
                    self.conn.commit()
                c.execute(f'''SELECT file_id FROM {self.slug}''')
                file_ids = c.fetchall()
                self.ids = [i[0] for i in file_ids]
        return num_store_videos

    def scrape_timeline(self):
        with Halo(text=f"Scraping {self.term.bold(self.name)}'s photos and videos...", color='red') as spinner:
            with requests.Session() as s:
                r = s.get(self.timeline_url.format(
                    self.slug, self.limit), headers=self.headers)
            if r.ok:
                spinner.succeed()
            else:
                spinner.fail()
                self.log.info(self.term.red(f"{r.status_code} STATUS CODE"))
                self.log.info(self.term.red(self.log.STATUS_ERROR))
                sys.exit(0)
        timeline = r.json()
        posts = timeline['timeline']
        self.log.info(f"\t· Found {self.term.bold(str(len(posts)))} posts")
        images, videos, audios = [], [], []
        for post in posts:
            if self.avoid_duplicates and post['uid'] in self.ids:
                pass
            else:
                if post['photo']:
                    if 'photos' in (has_photos := post['photos']):
                        type_, media_type = 'Timeline', 'Image'
                        uid = post['uid']
                        date = post['created_at']['date']
                        ts = self.get_timestamp(date)
                        photos = has_photos['photos']
                        for photo in photos:
                            image_url = photo['images']['original']
                            image_url = image_url.replace('\\', '')
                            images.append((image_url, ts, type_,
                                           media_type, date, uid))
                    else:
                        pass
                else:
                    pass
                if post['video']:
                    if 'video_url' in (video_object := post['video_object']):
                        type_, media_type = 'Timeline', 'Video'
                        uid = post['uid']
                        date = post['created_at']['date']
                        ts = self.get_timestamp(date)
                        video_url = video_object['video_url']
                        video_url = video_url.replace('\\', '')
                        videos.append((video_url, ts, type_,
                                       media_type, date, uid))
                    elif 'video_trailer' in video_object:
                        if self.download_preview_videos:
                            type_, media_type = 'Timeline', 'Video'
                            uid = post['uid']
                            date = post['created_at']['date']
                            ts = self.get_timestamp(date)
                            video_url = video_object['video_trailer']
                            video_url.replace('\\', '')
                            videos.append((video_url, ts, type_,
                                           media_type, date, uid))
                        else:
                            pass
                    else:
                        pass
                if post['audio']:
                    if 'audio_url' in (audio_object := post['audio_object']):
                        type_, media_type = 'Timeline', 'Audio'
                        uid = post['uid']
                        date = post['created_at']['date']
                        ts = self.get_timestamp(date)
                        audio_url = audio_object['audio_url']
                        audio_url = audio_url.replace('\\', '')
                        audios.append((audio_url, ts, type_,
                                       media_type, date, uid))
        self.log.info(
            f"\t  — Found {self.term.bold(str(len(images)))} new photos")
        self.log.info(
            f"\t  — Found {self.term.bold(str(len(videos)))} new videos")
        self.log.info(
            f"\t  — Found {self.term.bold(str(len(audios)))} new audios")
        return images, videos, audios

    def scrape_messages(self, url, tz, array=[]):
        if not array:
            spinner = Halo(
                text=f"Scraping your messages with {self.term.bold(self.name)}...", color='red')
            spinner.start()
        with requests.Session() as s:
            r = s.get(url, headers=self.headers)
        if r.status_code == 200:
            self.log.debug(self.term.lime(f"{r.status_code} STATUS CODE"))
        else:
            spinner.fail()
            self.log.info(self.term.red(f"{r.status_code} STATUS CODE"))
            self.log.info(self.term.red(self.log.STATUS_ERROR))
            sys.exit(0)
        messages_api = r.json()
        try:
            list_messages = messages_api['messages']
            list_messages += array
            mid = "&mid=" + messages_api['mid_token']
            new_messages_url = self.messages_url.format(self.slug, tz, mid)
            messages = self.scrape_messages(
                new_messages_url, tz, list_messages)
        except KeyError:
            if array:
                return list_messages
            else:
                messages = list_messages
        if array:
            return messages
        spinner.succeed()
        self.log.info(
            f"\t· Found {self.term.bold(str(len(messages)))} messages")
        image_urls, video_urls, audio_urls = [], [], []
        for message in list(messages):
            if self.avoid_duplicates and message['mid'] in self.ids:
                pass
            else:
                if message['has_images']:
                    if not message['is_locked']:
                        type_, media_type = 'Message', 'Image'
                        mid = message['mid']
                        date = message['created_at']['date']
                        ts = self.get_timestamp(date)
                        images = message['images']
                        for image in images:
                            image_urls.append(
                                (image['image'], ts, type_, media_type, date, mid))
                    else:
                        pass
                if message['has_video']:
                    if not message['is_locked']:
                        type_, media_type = 'Message', 'Video'
                        mid = message['mid']
                        date = message['created_at']['date']
                        ts = self.get_timestamp(date)
                        video_urls.append(
                            (message['video'], ts, type_, media_type, date, mid))
                    else:
                        pass
                if message['has_audio']:
                    if not message['is_locked']:
                        type_, media_type = 'Message', 'Image'
                        mid = message['mid']
                        date = message['created_at']['date']
                        ts = self.get_timestamp(date)
                        audio_urls.append(
                            (message['audio'], ts, type_, media_type, date, mid))
        self.log.info(
            f"\t  — Found {self.term.bold(str(len(image_urls)))} new photos")
        self.log.info(
            f"\t  — Found {self.term.bold(str(len(video_urls)))} new videos")
        self.log.info(
            f"\t  — Found {self.term.bold(str(len(audio_urls)))} new audios")
        return image_urls, video_urls, audio_urls

    def scrape_video_store(self, num):
        spinner = Halo(
            text=f"Scraping {self.term.bold(self.name)}'s store videos...", color='red')
        spinner.start()
        payload = {
            'limit': num,
            'slug': self.slug,
            'privacy': [],
            'type': 'video',
        }
        with requests.Session() as s:
            r = s.post(self.video_store_url,
                       headers=self.headers, params=payload)
        if r.ok:
            self.log.debug(self.term.lime(f"{r.status_code} STATUS CODE"))
        else:
            self.log.info(self.term.red(f"{r.status_code} STATUS CODE"))
            self.log.info(self.term.red(self.log.STATUS_ERROR))
            sys.exit(0)
        page_meta = r.json()['page_meta']
        total = page_meta['total']
        payload['limit'] = total
        with requests.Session() as s:
            r = s.post(self.video_store_url,
                       headers=self.headers, params=payload)
        if r.ok:
            spinner.succeed()
        else:
            spinner.fail()
            self.log.info(self.term.red(f"{r.status_code} STATUS CODE"))
            self.log.info(self.term.red(self.log.STATUS_ERROR))
            sys.exit(0)
        videos = []
        store_videos = r.json()['list']
        if store_videos:
            for video in store_videos:
                if self.avoid_duplicates and video['uid'] in self.ids:
                    pass
                else:
                    if video['can_see']:
                        try:
                            if 'video_url' in (video_object := video['video_object']):
                                type_, media_type = 'Store Video', 'Video'
                                uid = video['uid']
                                video_url = video_object['video_url']
                                video_url = video_url.replace('\\', '')
                                date = video['created_at']['date']
                                ts = self.get_timestamp(date)
                                videos.append(
                                    (video_url, ts, type_, media_type, date, uid))
                        except KeyError:
                            self.log.info(
                                f"Unable to download '{video['title']}'")
                    else:
                        if self.download_preview_videos:
                            if 'video_trailer' in (video_object := video['video_object']):
                                type_, media_type = 'Store Video', 'Video'
                                uid = video['uid']
                                video_trailer = video_object['video_trailer']
                                video_trailer.replace('\\', '')
                                date = video['created_at']['date']
                                ts = self.get_timestamp(date)
                                videos.append(
                                    (video_trailer, ts, type_, media_type, date, uid))
        self.log.info(
            f"\t· Found {self.term.bold(str(len(videos)))} new store videos")
        return videos

    def get_timestamp(self, date):
        iso_datetime = parse(date)
        timestamp = datetime.datetime.timestamp(iso_datetime)
        return timestamp


class Folder(User):
    def __init__(self, slug):
        super().__init__()
        self.timeline_dir = os.path.join(
            self.destination_path, slug, 'Timeline')
        os.makedirs(self.timeline_dir, exist_ok=True)
        self.msg_dir = os.path.join(self.destination_path, slug, 'Messages')
        self.store_videos_dir = os.path.join(
            self.destination_path, slug, 'Store Videos')


class Download(Folder):
    def __init__(self, slug):
        self.dir = None
        self.desc = None
        self.slug = None
        super().__init__(slug)

    def handle_download(self, array):
        if self.avoid_duplicates:
            self.conn = sqlite3.connect(os.path.join(self.db_dir, 'models.db'))
            with closing(self.conn.cursor()) as c:
                c.executemany(f'''
                INSERT INTO {self.slug}(url, timestamp, type, media_type, date, file_id)
                VALUES(?,?,?,?,?,?)''', array)
                self.conn.commit()
        os.makedirs(self.dir, exist_ok=True)
        with tqdm(desc=self.desc, total=len(array), colour='red') as bar:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(
                    self.download, group): group for group in array}
                for future in concurrent.futures.as_completed(futures):
                    future.result
                    bar.update(1)

    def download(self, group):
        url, time = group[0], group[1]
        filename = url.rsplit('/')[-1].split('?')[0]
        file_location = os.path.join(self.dir, filename)
        with requests.Session() as s:
            r = s.get(url, headers=self.headers)
        with open(file_location, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                f.write(chunk)
        if self.use_original_dates:
            os.utime(file_location, (time, time))
            if platform.system() == 'Windows':
                setctime(file_location, time)


class Timeline(Download):
    def __init__(self, slug, type):
        super().__init__(slug)
        self.slug = slug
        self.desc = f'Downloading {type.lower()}'
        self.dir = os.path.join(
            self.timeline_dir, type) if self.separate_file_types else self.timeline_dir


class Messages(Download):
    def __init__(self, slug, type):
        super().__init__(slug)
        self.slug = slug
        self.desc = f'Downloading {type.lower()}'
        self.dir = os.path.join(
            self.msg_dir, type) if self.separate_file_types else self.msg_dir


class StoreVideos(Download):
    def __init__(self, slug):
        super().__init__(slug)
        self.slug = slug
        self.desc = 'Downloading store videos'
        self.dir = self.store_videos_dir


def main():
    with Halo(color='red'):
        user = User()
        following_count = user.scrape_user()
        creators_list = user.scrape_follow(following_count)
        model = Model(creators_list)
    model.menu()
    num_store_videos = model.scrape_profile()
    images, videos, audios = model.scrape_timeline()
    if images:
        download_images = Timeline(model.slug, 'Images')
        download_images.handle_download(images)
    if videos:
        download_videos = Timeline(model.slug, 'Videos')
        download_videos.handle_download(videos)
    if audios:
        download_audios = Timeline(model.slug, 'Audios')
        download_audios.handle_download(audios)
    messages_url = user.messages_url.format(model.slug, user.timezone, '')
    msg_images, msg_videos, msg_audios = model.scrape_messages(
        messages_url, user.timezone)
    if msg_images:
        download_msg_images = Messages(model.slug, 'Images')
        download_msg_images.handle_download(msg_images)
    if msg_videos:
        download_msg_videos = Messages(model.slug, 'Videos')
        download_msg_videos.handle_download(msg_videos)
    if msg_audios:
        download_msg_audios = Messages(model.slug, 'Audios')
        download_msg_audios.handle_download(msg_audios)
    store_videos = model.scrape_video_store(num_store_videos)
    if store_videos:
        download_store_videos = StoreVideos(model.slug)
        download_store_videos.handle_download(store_videos)
    main()


if __name__ == '__main__':
    main()

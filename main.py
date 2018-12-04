import time
import os
import random

import requests
from bs4 import BeautifulSoup
import pandas as pd
import pymysql


class SocialerusCralwer(object):
    
    def __init__(self, host, user, passwd, db, port, charset='utf8', pageno=1):
        
        self.conn = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=port, charset=charset)
        self.curs = self.conn.cursor(pymysql.cursors.DictCursor)
        self.pageno = pageno
        
        
    def query_input(self, query):
        
        self.query = query
    

    def run(self):
                
        while True:


            URL = f'https://kr.socialerus.com/ranking/?category=&order=&page={self.pageno}'


            response = requests.get(URL)

            self.pageno +=1

            soup = BeautifulSoup(response.text, 'html.parser')


            len_test = soup.select('div.ranking--list__two')

            # channel and its category check
            if len(len_test) == 0:
                print (f"Crawling stop at {URL}")
                self.conn.close()
                break

            for i in range(len(len_test)):

                channel_and_category = len_test[i].text.strip().split('\n\n\r\n')
                channel, category = [word.strip() for word in channel_and_category]
                youtube_id = soup.select('div.ranking--list input.channel')[i].get('value')
                json_data = {'name':channel, 'id':youtube_id, 'category':category, 'source_url':URL}    
                self.curs.execute(self.query, args=json_data)
                self.conn.commit()
                print (json_data)
            
            time.sleep(random.randint(1, 2))


if __name__ == '__main__':
    
    host, user, passwd = os.environ['host'], os.environ['user'], os.environ['passwd']
    sc = SocialerusCralwer(host=host, user=user, passwd=passwd, 
                       db='YOUTUBE', port=3306, charset='utf8', pageno=1)
    
    upsert = 'INSERT INTO t_channel_list (name, id, category, source_url) \
           values (%(name)s, %(id)s, %(category)s, %(source_url)s) \
           ON DUPLICATE KEY UPDATE category=%(category)s, source_url=%(source_url)s'    
    
    sc.query_input(upsert)
    sc.run()

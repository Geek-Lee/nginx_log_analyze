#!/usr/bin/env python3
# -*- coding: utf8 -*-

import fileinput
import re
import pandas as pd
import json
from sqlalchemy import create_engine
import io_tosql
from apscheduler.schedulers.background import BackgroundScheduler
import time
import datetime
import os



engine_user_info = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('sm_log', 'vR49MNW7', '182.254.128.241', 4171, 'user_info'),
    connect_args={"charset": "utf8"})

dic = {r'{': r'{"', r'}': r'"}', r',': r'","', r':': r'":"'}
def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text

dirname = r'C:\Users\K\Desktop\nginx_log_analyze'

def ngx(dirname = dirname):
    # dirname = r'C:\Users\K\Desktop\nginx_log_analyze'
    files = os.listdir(dirname)
    fullfilepath_list = [name for name in files if name.endswith('.log')]

    print("____________________________________!________________________________________________")
    for fullfilepath in fullfilepath_list:
        df = pd.DataFrame()
        print(fullfilepath)
        try:
            with fileinput.input(fullfilepath) as f:
                for line in f:
                    remote_addr, _, _, local_time, request_method, request_url, _, status, request_body, \
                    body_bytes_sent, http_referer, *http_user_agent, hf1, hf2, _, _ = re.split('\s', line)
                    local_time = datetime.datetime.strptime(local_time.lstrip('[').rstrip(']').replace('T', ' ')[:-6], '%Y-%m-%d %H:%M:%S')
                    request_method = request_method if request_method == '-' else request_method.lstrip('"')
                    status = status if status == '-' else int(status)
                    #request_body = request_body if request_body == '-' else request_body.lstrip('[').rstrip(']').replace(r'\x22', '')
                    request_body = request_body if request_body == '-' else request_body.lstrip('[').rstrip(']').replace(r'\x22', '')
                    request_body_dict = request_body if request_body == '-' else json.loads(replace_all(request_body, dic))
                    fund_id, user_id = request_body if request_body == '-' else request_body_dict.get("fund_id", '-'), request_body if request_body == '-' else request_body_dict.get("user_id", '-')
                    body_bytes_sent = body_bytes_sent if body_bytes_sent == '-' else int(body_bytes_sent)
                    http_referer = http_referer if http_referer == '-' else http_referer.strip('"')
                    http_user_agent = ' '.join(http_user_agent)
                    http_x_forwarded_for = (hf1+' '+hf2).rstrip('"')
                    one_line_df = pd.DataFrame({"remote_addr": [remote_addr], "request_method": [request_method], "local_time": [local_time],
                                                "request_url": [request_url], "status": [status], "request_body": [request_body],
                                                "body_bytes_sent": [body_bytes_sent], "http_referer": [http_referer],
                                                "http_user_agent": [http_user_agent], "http_x_forwarded_for": [http_x_forwarded_for],
                                                "fund_id": [fund_id], "user_id": [user_id]
                                                })
                    df = pd.concat([df, one_line_df])
                    print("长度为：" + str(len(df)))
                    if len(df) >= 1000:
                        io_tosql.to_sql("easy_log", engine_user_info, df)
                        df = pd.DataFrame()
        except:
            print("except")
        io_tosql.to_sql("easy_log", engine_user_info, df)  # 不到一千的也放入数据库，dataframe在循环下一个的文件时清空
        #放入excel的用法
        # writer = pd.ExcelWriter(r'C:\Users\K\Desktop\output.xlsx')
        # df.to_excel(writer, sheet_name='Sheet1', index=False)
        # writer.save()
        # print(df)

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(ngx, 'cron', month='1-12', day_of_week='0-6', hour='0-23', minute='0-59', second='0-59')
    scheduler.start()
    print('开始！')

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The Job!')

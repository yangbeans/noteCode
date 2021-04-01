# -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 09:36:34 2021

@author: yangsenbin
"""
import logging
import os
from logging import handlers


#python日志采集
def _logging(**kwargs):
    level = kwargs.pop('level', None)
    filename = kwargs.pop('filename', None)
    datefmt = kwargs.pop('datefmt', None)
    format = kwargs.pop('format', None)
    if level is None:
        level = logging.DEBUG
    if filename is None:
        filename = 'default.log'
    if datefmt is None:
        datefmt = '%Y-%m-%d %H:%M:%S'
    if format is None:
        format = '%(asctime)s [%(module)s] %(levelname)s [%(lineno)d] %(message)s'

    log = logging.getLogger(filename)
    format_str = logging.Formatter(format, datefmt)
    # backupCount 保存日志的数量，过期自动删除
    # when 按什么日期格式切分(这里方便测试使用的秒)
    th = handlers.TimedRotatingFileHandler(filename=filename, when='D', backupCount=7, encoding='utf-8')
    th.setFormatter(format_str)
    th.setLevel(logging.INFO)
    log.addHandler(th)
    log.setLevel(level)
    return log

def get_model():
    return "this is model!....."
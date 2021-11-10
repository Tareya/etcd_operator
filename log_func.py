#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging, os

basic_path = os.getcwd()
if not os.path.exists(os.path.join(basic_path, 'logs')):

  os.mkdir(os.path.join(basic_path, 'logs'))

log_path = os.path.join(basic_path, 'logs', 'debug.log')

def debug_log(logger_name='DEBUG-LOG', log_file=log_path, leve=logging.DEBUG):
    
    # 创建 logger对象
    logger = logging.getLogger(logger_name)
    # 添加等级
    logger.setLevel(leve)
    
    # 创建控制台 console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(leve)
    
    # 创建文件 handler
    file_handler = logging.FileHandler(filename=log_file, encoding='utf-8')
    
    # 创建格式化对象                   # 时间      # 日志文件           # 行数      # 日志名   # 日志等级     # 日志信息 
    formatter = logging.Formatter('%(asctime)s %(filename)s [line: %(lineno)d] %(name)s %(levelname)s %(message)s')
    
    # 添加 formatter
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # 将 handler 添加到 logger对象
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


def main():
  # 测试
  logger = debug_log()
  logger.debug('Floder zips does not exist, start to create the floder.')
  logger.info('info message')
  logger.warning('warn message')
  logger.error('error message')
  logger.critical('critical message')

if __name__ == '__main__':
    main()
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-11-09 16:43:33
# @Author  : Tareya

import os, socket, time, datetime, re, arrow    # pip3 install --user arrow
import paramiko
from scp import SCPClient         # pip3 install --user scp
import etcd3, oss2                # pip3 install --user etcd3 oss2 
from itertools import islice
import log_func



'''
基础信息 工具类
'''
class BasicOperator(object):

  def __init__(self):
    self.host   = self.get_host_ip()
    self.log    = log_func.debug_log(logger_name='basic_operator', log_file=os.path.join(os.getcwd(), 'logs', 'basic.log'))
  

  def get_hostname(self):
    # 获取主机名
    return socket.gethostname()


  def get_host_ip(self):
    # 获取本地IP地址
    try:
      s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      s.connect(('8.8.8.8', 80)) 
      ip = s.getsockname()[0]
    except socket.error as e:
      self.log.error(f'ERROR: {e}.')
    finally:
      s.close()

    return ip


  def get_running_env(self):
    # 根据 host_ip 自行判断环境，并定义 oss path
    
    global running_env

    try:
      if self.host.find('192.168.3') != -1:
        running_env = 'testing'

      elif self.host.find('172.16.0') != -1:
        running_env = 'release'

      elif self.host.find('172.16.') != -1 and self.ip.find('172.16.0') == -1:
        running_env = 'production'

      return running_env

    except Exception as e:
      self.log.error(f'Error: {e}.')


  def get_remote_ips(self):
    # 获取需要远程操作的主机列表，根据规范定义，同类型主机名后缀数字依次加1,然后由于添加了host，故可以直接用主机名解析对应ip
    ips = []
    num = 1
    while num < 3:
      hostname = self.get_hostname().lower()
      # hostname = 'T-KuberM-001'.lower()
      num +=1 
      ips.append(hostname.replace('1', str(num)))

    return ips




'''
SSH 工具类
'''
class SSHOperator(object):

  def __init__(self, host):
    self.host       = host
    self.port       = 36022
    self.sock       = self.host, self.port
    self.username   = 'uniondrug'
    self.pkey_path  = os.path.join(os.getenv('HOME'), '.ssh/id_rsa')
    self.pkey       = paramiko.RSAKey.from_private_key_file(self.pkey_path)
    self._transport = None
    self._sftp      = None
    self._client    = None
    self._connect()
    self.log        = log_func.debug_log(logger_name='ssh_operator', log_file=os.path.join(os.getcwd(), 'logs', 'ssh.log'))    


  def _connect(self):
    # 建立ssh连接传输
    transport = paramiko.Transport(sock=self.sock)
    transport.connect(username=self.username, pkey=self.pkey)
    self._transport = transport


  def download(self, remotepath, localpath):
    # sftp 下载功能

    if self._sftp is None:
        self._sftp = paramiko.SFTPClient.from_transport(self._transport)

    try:   
        self._sftp.get(remotepath=remotepath, localpath=localpath)
    except remotepath is None:
        self.log.error('remotepath is required!')
    except localpath is None:
        self.log.error('localpath is required!')
    except Exception as e:
        self.log.error(f"Error: {e}.")



  def upload(self, localpath, remotepath):
    # sfp 上传功能
    if self._sftp is None:
        self._sftp = paramiko.SFTPClient.from_transport(self._transport)

    try:
        self._sftp.put(localpath=localpath ,remotepath=remotepath)        
    except remotepath is None:
        self.log.error('remotepath is required!')
    except localpath is None:
        self.log.error('localpath is required!')
    except Exception as e:
        self.log.error(f"Error: {e}.")


  
  def exec_command(self, command):
    # 执行命令行命令
    if self._client is None:
      self._client = paramiko.SSHClient()
      self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      self._client._transport = self._transport

    stdin , stdout, stderr = self._client.exec_command(command)

    output = stdout.read().decode('utf-8')
    if len(output) > 0:
        return output.strip()
        
    error = stderr.read().decode('utf-8')
    if len(error) > 0:
        return error.strip()


  def remote_transport(self, localpath, remotepath):
    # 传输文件  
    if self._client is None:
      self._client = paramiko.SSHClient()
      self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      self._client._transport = self._transport

    with SCPClient(self._client.get_transport(), socket_timeout=15.0) as scp:
      scp.put(localpath, remotepath)





'''
Etcd 工具类
'''
class EtcdOperator(object):

  def __init__(self, *args, **kwargs):
    self.host     = '192.168.3.151'
    self.port     = 2379
    self.ssl_path = os.path.join('ssl', BasicOperator().get_running_env())
    self.ca       = os.path.join(self.ssl_path, 'ca.pem')
    self.key      = os.path.join(self.ssl_path, 'server-key.pem')
    self.cert     = os.path.join(self.ssl_path, 'server.pem')
    self.client   = self._connect()
    self.log      = log_func.debug_log(logger_name='etcd_operator', log_file=os.path.join(os.getcwd(), 'logs', 'etcd.log'))


  def _connect(self):
    # 建立连接
    try:
      return etcd3.client(host=self.host, 
                          port=self.port,
                          ca_cert=self.ca,
                          cert_key=self.key,
                          cert_cert=self.cert)
                            
    except Exception as e:
      self.log.error(f'Error: {e}.')


  def get_version(self):
    # 查看 etcd 版本信息
    self.log.debug(self.client.status().version)
    
    return self.client.status().version


  def create_snapshot(self, snapshot):
    # 创建 etcd 状态快照

    # 快照内容类型为 byte，故读写操作使用 wb/rb
    with open(snapshot, 'wb') as f:
        self.client.snapshot(f)             # snapshot 需要一个文件对象做写入




'''
OSS 操作类
'''
class OssOperator(object):

  def __init__(self):
    self.endpoint = 'oss-cn-hangzhou-internal.aliyuncs.com'
    self.public_endpoint = 'oss-cn-hangzhou.aliyuncs.com'
    self.bucket_name = "etcd-backup"
    self.ackID = 'xxxxxx'
    self.ackSc = 'xxxxxx'
    self.auth = oss2.Auth(self.ackID, self.ackSc)
    self.bucket = oss2.Bucket(self.auth, self.public_endpoint, self.bucket_name)
    self.log = log_func.debug_log(logger_name='oss_operator', log_file=os.path.join(os.getcwd(), 'logs', 'oss.log'))


  def ensure_file(self, snapshot):
  # 确认文件是否存在于oss上
    env = BasicOperator().get_running_env()
  
    if env is not None:
      global oss_filename
      oss_filename = os.path.join(env, snapshot.replace(os.path.sep, '/').replace('./', ''))
      
      if self.bucket.object_exists(oss_filename):
        self.log.debug(f'{oss_filename} is on the OSS!')
        return True

      else:
        self.log.debug(f'{oss_filename} is not on the OSS!')
        return False


  def upload_file(self, snapshot, file_path):
  # 上传文件
    # 如果文件不存在于OSS, 则上传文件
    if not self.ensure_file(snapshot):

      start_time = time.time()

      # 上传文件
      oss2.resumable_upload(self.bucket, oss_filename, file_path)

      self.log.debug('Local file {} --> {}/{} upload finished, cost {} Sec.'.format(file_path, self.bucket_name, oss_filename, time.time() - start_time))

    else:
      # 如果文件存在于oss上
      self.log.debug('{} has already existed on {}/{}'.format(file_path, self.bucket_name, oss_filename))
  


  def download_file(self, snapshot, file_path):
  # 下载文件
    if self.ensure_file(snapshot):

      start_time = time.time()

      # 下载文件
      oss2.resumable_download(self.bucket, oss_filename, file_path)

      self.log.debug('OSS file {}/{} --> {} download finished, cost {} Sec.'.format(self.bucket_name, oss_filename, file_path, time.time() - start_time))

    else:
      # 如果 oss 上不存在文件
      self.log.debug('File {}/{} not found on oss.'.format(self.bucket_name, oss_filename))



  def list_files(self, prefix):
  # 列举文件 (需要输入具体前缀)
    list = []
    
    for obj in islice(oss2.ObjectIteratorV2(self.bucket, prefix), 1000):
      list.append(obj.key)

    return list



  def remove_file(self, snapshot):
  # 删除文件
    if self.ensure_file(snapshot):
      # 删除 oss 对象
      self.bucket.delete_object(oss_filename)

      self.log.debug('OSS file {}/{} remove finished.'.format(self.bucket_name, oss_filename))

    else:
      # 如果 oss 上不存在文件
      self.log.debug('File {}/{} not found on oss.'.format(self.bucket_name, oss_filename))




'''
任务 工具类
'''
class TaskOperator(object):
  '''任务要求:
    1、创建快照备份
    2、创建的快照备份同步至其他master节点的备份目录（用于发生故障时快速恢复）
    3、快照备份上传至oss对用目录
    4、删除本地（所有节点）过期快照文件（只保留当天）
  '''
  
  def __init__(self):
    self.backup_path = '/data/backup/etcd'
    # self.backup_path = './'
    self.log = log_func.debug_log(logger_name='task_operator', log_file=os.path.join(os.getcwd(), 'logs', 'task.log'))

  def define_filename(self):
    # 定义文件名称
    return 'snapshot-{}.db'.format(arrow.now().format("YYYYMMDDHHmm"))

  
  def create_task(self, file_path):
    # 创建快照任务
    self.log.debug('Creating snapshot start...')
    self.log.debug('--------------------------------')
    # 如果不存在备份路径则自动创建
    if not os.path.exists(self.backup_path):
      os.mkdir(self.backup_path)

    try:
      start_time = time.time()

      EtcdOperator().create_snapshot(file_path)

      self.log.debug('Creating snapshot complete, cost {} Sec'.format(time.time() - start_time))

    except Exception as e:
      self.log.error(f'Error: {e}.')

    self.log.debug('--------------------------------')
    self.log.debug('Creating snapshot complete.')



  def sync_task(self, file_path):
    # 同步快照任务

    self.log.debug('Transporting snapshot start...')
    self.log.debug('--------------------------------')

    for ip in BasicOperator().get_remote_ips():
      try:           
        start_time = time.time()

        SSHOperator(host=ip).exec_command('/usr/bin/mkdir -p /data/backup/etcd')
        SSHOperator(host=ip).remote_transport(localpath=file_path, remotepath=file_path)

        self.log.debug('Transporting file to {} complete, cost {} Sec'.format(ip, time.time() - start_time))  
        
      except Exception as e:
        self.log.error(f'Error: {e}.')
    self.log.debug('--------------------------------')
    self.log.debug('Transporting snapshot complete.')



  def oss_task(self, snapshot):
    # 上传oss任务 
    self.log.debug('Uploading snapshot start...')
    self.log.debug('--------------------------------')

    try:

      OssOperator().upload_file(snapshot, os.path.join(self.backup_path, snapshot))

    except Exception as e:
      self.log.error(f'Error: {e}.')

    self.log.debug('--------------------------------')
    self.log.debug('Uploading snapshot complete.')



  def remove_task(self, snapshot):
    # 清除快照任务
    now_hour = arrow.now().format("HH")
    file_path = os.path.join(self.backup_path, snapshot)

    if now_hour == '23':
      
      time_prefix = re.findall(r"snapshot-(\d{8}).+.db", snapshot)[0]
      now_timestamp = datetime.datetime.strptime(time_prefix, '%Y%m%d').timestamp()
      pre_timestamp = datetime.datetime.strptime(arrow.now().shift(days=-1).format('YYYYMMDD'), '%Y%m%d').timestamp()

      if pre_timestamp - now_timestamp > 0:

        if os.path.exists(file_path):
          self.log.debug('Removing snapshot start...')
          self.log.debug('--------------------------------')
          try:
            # 清理本地过期快照
            os.remove(file_path)
            # 清理远程主机过期快照
            for ip in BasicOperator().get_remote_ips():
              SSHOperator(host=ip).exec_command(f'/usr/bin/rm {file_path}')

            self.log.debug('--------------------------------')
            self.log.debug('Removing snapshot complete.')
          
          except Exception as e:
            self.log.error(f'Error: {e}.')

      else:
        self.log.debug(f'{file_path} was backuped within 3 days.')



  def clear_task(self):
    # 清理 oss 对象任务
    now_hour = arrow.now().format("HH")
    env = BasicOperator.get_running_env()
    
    if now_hour == '23':

      pretime_prefix = arrow.now().shift(days=-180).format('YYYYMMDD')
      self.log.debug('Clear oss file start...')
      self.log.debug('--------------------------------')
  
      for item in OssOperator().list_files(os.path.join(env, f"snapshot-{pretime_prefix}")):
        try:
          self.log.debug(f'Removing {item} remove start.')

          OssOperator().bucket.delete_object(item)

          self.log.debug(f'OSS file {item} remove finished.')

        except Exception as e:
          self.log.error(f'Error: {e}.')
          
      self.log.debug('--------------------------------')
      self.log.debug('Removing oss file complete.')



  def main_task(self):
    # 主任务

    snapshot = self.define_filename()
    file_path = os.path.join(self.backup_path, snapshot)

    self.create_task(file_path)

    self.sync_task(file_path)

    self.oss_task(snapshot)

    self.remove_task(snapshot)

    self.clear_task()




if __name__ == '__main__':

  task = TaskOperator()
  task.main_task()


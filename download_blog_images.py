import os, socket, time, datetime, re, arrow    # pip3 install --user arrow
import oss2                # pip3 install --user etcd3 oss2 
from itertools import islice
import log_func


'''
OSS 操作类
'''
class OssOperator(object):

  def __init__(self):
    self.endpoint = 'oss-cn-hangzhou-internal.aliyuncs.com'
    self.public_endpoint = 'oss-cn-hangzhou.aliyuncs.com'
    self.bucket_name = "uniondrug-devops"
    self.ackID = 'LTAI1e3Uhc4Qa5Yv'
    self.ackSc = 'nZWjdpIgElFdVfmN09vim11aqzAfx4'
    self.auth = oss2.Auth(self.ackID, self.ackSc)
    self.bucket = oss2.Bucket(self.auth, self.public_endpoint, self.bucket_name)
    self.log = log_func.debug_log(logger_name='oss_operator', log_file=os.path.join(os.getcwd(), 'logs', 'blog.log'))


  def ensure_file(self, snapshot):
  # 确认文件是否存在于oss上
    # env = BasicOperator().get_running_env()
    env = "testing"
  
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
  


  def download_file(self, oss_filename, file_path):
  # 下载文件
    if self.bucket.object_exists(oss_filename):

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
    
    for obj in islice(oss2.ObjectIteratorV2(self.bucket, prefix), 10000):
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




if __name__ == '__main__':

  oss = OssOperator()
  
  prefix = "blog_images"
  file_dir = "/Users/shenlei/Desktop"

  for img in oss.list_files(prefix):
    
    oss.download_file(img, os.path.join(file_dir, img))
    
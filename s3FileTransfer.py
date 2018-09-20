from multiprocessing.pool import ThreadPool
import os
import boto3
import threading
import datetime

###FILL IN THESE GLOBALS TO SETUP THE PROGRAM
AWS_KEY = ""
AWS_SECRET = ""
bucket = ""
path = r''
###

files = []
totalSize = 0
bytesTransfered = 0
initialStart = datetime.datetime.now()
for root,directory,fileName in os.walk(path):
        for name in fileName:
                files.append(os.path.join(root,name))
                totalSize += os.path.getsize(os.path.join(root,name))

        #files.append(os.path.join(path,file))
        #totalSize += os.path.getsize(os.path.join(path,file))
#files = [os.path.join(path,file) for file in os.listdir(path)]
#conn = boto3.client('s3',verify="ZscalerRootCertificate-2048-SHA256.crt")
conn = boto3.client('s3')
 
def upload(myFile):
        global bytesTransfered
        global initialStart
        print(f"Uploading {myFile}")
        conn.upload_file(myFile,bucket,os.path.basename(myFile),Callback=ProgressPercentage(myFile))
        totalTransferSpeed = ((bytesTransfered * 8) / 1000000)/(datetime.datetime.now() - initialStart).total_seconds()
        print("\nRemaining Bytes: {:>15}  /  Total transfer speed: {:.1f}Mb/sec".format(totalSize,totalTransferSpeed))
        print(f"\nFinished {myFile}")


class ProgressPercentage(object):
        def __init__(self, filename):
            self._filename = filename
            self._size = float(os.path.getsize(filename))
            self._seen_so_far = 0
            self._startTime = datetime.datetime.now()
            self._lock = threading.Lock()

        def __call__(self, bytes_amount):
            global totalSize
            global bytesTransfered
            bytesTransfered += bytes_amount
            totalSize -= bytes_amount
            '''
            with self._lock:
                self._seen_so_far += bytes_amount
                percentage = (self._seen_so_far / self._size) * 100
                transferSpeed = (((self._seen_so_far * 8) / 1000000)/(datetime.datetime.now() - self._startTime).total_seconds())
                print("\r{:50}{}/{},{:.2f}%  ".format(os.path.basename(self._filename),self._seen_so_far,self._size,percentage),end='',flush=True)
                print("\rPer process transfer speed: {:.1f}Mb/sec".format(transferSpeed),end='')
                print("\r{},{}/{},{:.2f}%,{:.1f}Mb/sec".format(
                        os.path.basename(self._filename),self._seen_so_far,self._size,percentage,transferSpeed),end ='',flush=True)
'''
if __name__ == '__main__':
        pool = ThreadPool()
        pool.map(upload, files)
        print("\n**Total runtime: {}sec".format((datetime.datetime.now() - initialStart).seconds))
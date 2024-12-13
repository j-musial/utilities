#!/bin/python
#usage: CRYOHYDRO_upload.py -l /tmp/test -p dummy_bucket/test
#export RCLONE_CONFIG_CLMS_TYPE=s3
#export RCLONE_CONFIG_CLMS_ACCESS_KEY_ID=YOUR_CLMS_PUBLIC_S3_KEY
#export RCLONE_CONFIG_CLMS_SECRET_ACCESS_KEY=YOUR_CLMS_PRIVATE_S3_KEY
#export RCLONE_CONFIG_CLMS_REGION=default
#export RCLONE_CONFIG_CLMS_ENDPOINT='https://s3.waw3-1.cloudferro.com'
#export RCLONE_CONFIG_CLMS_PROVIDER='Ceph'
from rclone_python import rclone
from optparse import OptionParser
from datetime import datetime,timedelta
from os import environ,path
from hashlib import md5
from traceback import print_exc

if not rclone.is_installed():
	print('ERROR: rclone binary has not been detected. Please see: https://rclone.org/install/')
	exit(1)

parser = OptionParser()
parser.add_option("-l", "--local-file", dest="local_file",help="local path (i.e. file system) path to input file",default=False)
parser.add_option("-o", "--overwrite", dest="overwrite",help="shall the uploaded S3 product be replaced in the CLMS producer bucket.",default=True,action='store_false')
parser.add_option("-p", "--path-s3", dest="s3_path",help="relative path of a file in the S3 bucket of the CLMS producer e.g. webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily/2024/20240713",default=False)
(opt, args) = parser.parse_args()
if not bool(opt.s3_path):
	print('ERROR:No S3 relative path defined!')
	exit(1)
if opt.s3_path[-1] == '/':
	opt.s3_path=opt.s3_path[:-1]
if bool(opt.local_file):
	if not path.exists(opt.local_file):
		print('ERROR:File does not exists:'+opt.local_file)
		exit(1)			
	try:
		timestamp=datetime.now()
		last_modified=datetime.fromtimestamp(path.getmtime(opt.local_file)).strftime('%Y-%m-%dT%H:%M:%S')
		file_size=str(path.getsize(opt.local_file))
		f = open(opt.local_file, 'rb')
		md5_checksum=md5(f.read()).hexdigest()
		f.close()
		rclone.copy(opt.local_file,('CLMS:CLMS-CRYOHYDRO-INGESTION/'+opt.s3_path).replace('//','/'),ignore_existing=opt.overwrite,args=['--s3-no-check-bucket','--retries=20','--low-level-retries=20','--checksum','--s3-use-multipart-uploads=false','--metadata','--metadata-set uploaded='+str(timestamp.strftime('%Y-%m-%dT%H:%M:%S')), '--metadata-set WorkflowName="clms_upload"','--metadata-set source-s3-endpoint-url="'+environ['RCLONE_CONFIG_CLMS_ENDPOINT']+'"','--metadata-set source-s3-path=s3://'+('CLMS-CRYOHYDRO-INGESTION/'+opt.s3_path+'/'+path.basename(opt.local_file)).replace('//','/'),'--metadata-set file-size='+file_size,'--metadata-set last_modified='+last_modified])
	except:
		print('ERROR:Uploading file:'+opt.local_file)
		print_exc()
		exit(1)		
else:
	print('ERROR:No input file provided!')
	exit(1)

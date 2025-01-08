# !/usr/bin/env python3
"""
CLMS S3 File Uploader
====================

A tool for uploading files to CLMS S3 storage.

Requirements
-----------
- Python 3.6+
- rclone installed on the system (https://rclone.org/install/)
- rclone_python package (pip install rclone_python configparser)
- Access to CLMS S3 storage (credentials required)

Configuration
------------
The first time the script is run access_key and secret_key must be provided
```python CRYOHYDRO_uploader.py -l /path/to/file -p destination/path -c credential ```


Usage Examples
-------------
python CRYOHYDRO_uploader.py -l /path/to/file -p destination/path

Command Line Options
------------------
-l, --local-file       Path to the file to upload
-p, --path-s3          Destination path in S3 bucket
-i, --id               S3 access key ID (optional if using config file)
-k, --secret           S3 secret access key (optional if using config file)
-o, --overwrite        Overwrites existing files in the destination bucket


Author: CDSE & CLMS
License: MIT
Version: 0.0.9
"""

import configparser
import os
from datetime import datetime
from hashlib import md5
from optparse import OptionParser
from os import path
from pathlib import Path
from typing import Dict, Union, Optional, Tuple
from dataclasses import dataclass


from rclone_python import rclone


@dataclass
class UploadConfig:
	rclone_type: str
	provider: str
	env_auth: bool
	access_key_id: str
	secret_access_key: str
	region: str
	endpoint: str
	location_constraint: str


class UploadError(Exception):
	"""Custom exception for upload-related errors"""
	pass

class CredentialsError(Exception):
	"""Custom exception for credentials related errors"""
	pass


def calculate_file_metadata(filepath: str) -> Dict[str, str]:
	"""Calculate file metadata including timestamp, size, and MD5 checksum"""
	if not os.path.exists(filepath):
		raise UploadError(f"File does not exist: {filepath}")

	try:
		timestamp = datetime.now()
		last_modified = datetime.fromtimestamp(path.getmtime(filepath)).strftime('%Y-%m-%dT%H:%M:%S')
		file_size = str(path.getsize(filepath))

		with open(filepath, 'rb') as f:
			md5_checksum = md5(f.read()).hexdigest()

		return {
			'timestamp': timestamp.strftime('%Y-%m-%dT%H:%M:%S'),
			'last_modified': last_modified,
			'file_size': file_size,
			'md5_checksum': md5_checksum
		}
	except Exception as e:
		raise UploadError(f"Error calculating file metadata: {str(e)}")


def pusher(config: UploadConfig, local_file: str, s3_path: str, overwrite: bool = True) -> None:
	"""Upload file to S3 with metadata"""
	if not local_file or not s3_path:
		raise UploadError("Both local_file and s3_path must be provided")

	# Normalize S3 path
	s3_path = s3_path.rstrip('/')

	try:
		metadata = calculate_file_metadata(local_file)
		destination = f'CLMS:CLMS-CRYOHYDRO-INGESTION/{s3_path}'.replace('//', '/')

		rclone.copy(
			local_file,
			destination,
			ignore_existing=overwrite,
			args=[
				'--s3-no-check-bucket',
				'--retries=20',
				'--low-level-retries=20',
				'--checksum',
				'--s3-use-multipart-uploads=false',
				'--metadata',
				f'--metadata-set uploaded={metadata["timestamp"]}',
				'--metadata-set WorkflowName="clms_upload"',
				f'--metadata-set source-s3-endpoint-url={config.endpoint}',
				f'--metadata-set source-s3-path=s3://{os.path.join("CLMS-CRYOHYDRO-INGESTION", s3_path, os.path.basename(local_file)).replace("//", "/")}',
				f'--metadata-set file-size={metadata["file_size"]}',
				f'--metadata-set md5={metadata["md5_checksum"]}',
				f'--metadata-set last_modified={metadata["last_modified"]}',
				f'--metadata-set s3-public-key={config.access_key_id}'
			]
		)
	except Exception as e:
		raise UploadError(f"Error uploading file {local_file}: {str(e)}")


def parse_credentials(credentials_path: Path) -> Tuple[str, str]:
	"""Parse the credential file to extract access key ID and secret access key."""

	try:
		credentials_path = Path(credentials_path)
		if not credentials_path.exists():
			raise CredentialsError(f"Credentials file not found: {credentials_path}")

		with credentials_path.open('r') as f:
			lines = [line.strip() for line in f if line.strip()]

			if not lines:
				raise CredentialsError("Credentials file is empty")

			try:
				access_key_id, secret_access_key = lines[0].split(':')
				return access_key_id.strip(), secret_access_key.strip()
			except ValueError:
				raise CredentialsError("Invalid credentials format. Expected 'access_key_id:secret_access_key'")

	except Exception as e:
		if isinstance(e, CredentialsError):
			raise
		raise CredentialsError(f"Error reading credentials: {str(e)}")


def rclone_setup(credentials_path: Path) -> None:
	"""Configure rclone with provided credentials"""
	try:
		from rclone_python.remote_types import RemoteTypes

		access_key_id, secret_access_key = parse_credentials(credentials_path)

		rclone.create_remote(
			'CLMS',
			remote_type=RemoteTypes.s3,
			client_id=access_key_id,
			client_secret=secret_access_key,
			**{"type": 's3',
			   "provider": 'Ceph',
			   "env_auth": "True",
			   "region": 'default',
			   "endpoint": 'https://s3.waw3-1.cloudferro.com',
			   "location_constraint": 'default',
			   # "acl" : "public-read",
			   })
	except Exception as e:
		raise UploadError(f"Error setting up rclone: {str(e)}")


def config_settings(config_path: str) -> UploadConfig:
	"""Read and parse a configuration file"""
	if not os.path.exists(config_path):
		raise FileNotFoundError(f"Configuration file not found: {config_path}")

	try:
		config = configparser.ConfigParser()
		config.read(config_path)

		return UploadConfig(
			rclone_type=config['CLMS']['type'],
			provider=config['CLMS']['provider'],
			env_auth=config['CLMS'].getboolean('env_auth'),
			access_key_id=config['CLMS']['client_id'],
			secret_access_key=config['CLMS']['client_secret'],
			region=config['CLMS']['region'],
			endpoint=config['CLMS']['endpoint'],
			location_constraint=config['CLMS']['location_constraint']
		)
	except Exception as e:
		raise UploadError(f"Error parsing configuration file: {str(e)}")


def main():
	if not rclone.is_installed():
		raise UploadError("rclone binary not detected. Please see: https://rclone.org/install/")

	parser = OptionParser()
	parser.add_option("-l", "--local-file", dest="local_file",
					  help="local path (i.e. file system) path to input file")
	parser.add_option("-o", "--overwrite", dest="overwrite",
					  help="overwrite products already available in the CLMS bucket",
					  default=True, action='store_false')
	parser.add_option("-p", "--path-s3", dest="s3_path",
					  help="relative path of a file in the S3 bucket of the CLMS producer")
	parser.add_option("-c", "--credentials_path", dest="credentials_path",
					  help="file path to the S3 credentials" )

	options, _ = parser.parse_args()

	try:
		if options.credentials:
			rclone_setup(Path(options.credentials))
			config = UploadConfig(
				rclone_type='s3',
				provider='Ceph',
				env_auth=True,
				access_key_id=options.access_key_id,
				secret_access_key=options.secret_access_key,
				region='default',
				endpoint='https://s3.waw3-1.cloudferro.com',
				location_constraint='default'
			)
		else:
			config_path = os.path.join(os.path.expanduser("~"), ".config", "rclone", "rclone.conf")
			if not os.path.exists(config_path):
				raise UploadError("S3 credentials not present, please provide through --id and --secret")
			config = config_settings(config_path)

		if not options.local_file or not options.s3_path:
			raise UploadError("Both --local-file and --path-s3 must be provided")

		pusher(config, options.local_file, options.s3_path, options.overwrite)
		print(f"Successfully uploaded {options.local_file}")

	except KeyboardInterrupt:
		print("\nUpload interrupted by user")
		exit(1)
	except Exception as e:
		print(f"Error: {str(e)}")
		exit(1)


if __name__ == '__main__':
	main()
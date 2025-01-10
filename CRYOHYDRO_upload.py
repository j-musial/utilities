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
```python CRYOHYDRO_uploader.py -l /path/to/file -p destination/path -i access_key -k secret_key ```


Usage Examples
-------------
python CRYOHYDRO_uploader.py -l /path/to/file -p destination/path

Command Line Options
------------------
-l, --local-file       Path to the file to upload
-p, --path-s3          Destination path in S3 bucket
-c, --credentials_path Path to the credential file
-o, --overwrite        Overwrites existing files in the destination bucket


Author: CDSE & CLMS
License: MIT
Version: 0.0.10
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

import importlib.util

package_name = 'rclone_python'
if importlib.util.find_spec(package_name) is None:
	print(f"{package_name} is not installed. Please see: https://pypi.org/project/rclone-python/")
else:
	from rclone_python import rclone

if not rclone.is_installed():
	print("rclone binary not detected. Please see: https://rclone.org/install/")

@dataclass
class UploadConfig:
	rclone_type: str
	provider: str
	access_key_id: str
	secret_access_key: str
	region: str
	endpoint: str


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
		destination = f'CRYOHYDRO:CLMS-CRYOHYDRO-INGESTION/{s3_path}'.replace('//', '/')

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
			'CRYOHYDRO',
			remote_type=RemoteTypes.s3,
			#client_id=access_key_id,
			#client_secret=secret_access_key,
			**{"type": 's3',
			   "provider": 'Ceph',
			   "access_key_id": access_key_id,
			   "secret_access_key": secret_access_key,
			   "endpoint": 'https://s3.waw3-1.cloudferro.com',
			   "region": 'default',
			   "env_auth": "True",
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
			rclone_type=config['CRYOHYDRO']['type'],
			provider=config['CRYOHYDRO']['provider'],
			access_key_id=config['CRYOHYDRO']['access_key_id'],
			secret_access_key=config['CRYOHYDRO']['secret_access_key'],
			region=config['CRYOHYDRO']['region'],
			endpoint=config['CRYOHYDRO']['endpoint'],
		)
	except Exception as e:
		raise UploadError(f"Error parsing configuration file: {str(e)}")


def main():

	usage = """
Usage: %prog [options] 

This script uploads files to CLMS S3 storage using rclone.

Examples:
  First time usage with credentials:
    %prog -l /path/to/file.tif -p destination/path -c /path/to/credentials.txt

  Subsequent usage (after credentials are configured):
    %prog -l /path/to/file.tif -p destination/path

  Upload without overwriting existing files:
    %prog -l /path/to/file.tif -p destination/path --no-overwrite

Notes:
  - The credentials file should contain a single line with format: access_key_id:secret_access_key
  - The destination path is relative to the CLMS-CRYOHYDRO-INGESTION bucket
  - Files are uploaded with metadata including timestamp, size, and MD5 checksum
"""

	parser = OptionParser(usage=usage, version="%prog 0.0.10")

	parser.add_option(
		"-l", "--local-file",
		dest="local_file",
		help="Path to the local file to upload (REQUIRED)",
		metavar="FILE"
	)

	parser.add_option(
		"-p", "--path-s3",
		dest="s3_path",
		help="Destination path in S3 bucket relative to CLMS-CRYOHYDRO-INGESTION (REQUIRED)",
		metavar="PATH"
	)

	parser.add_option(
		"-c", "--credentials-path",
		dest="credentials_path",
		help="Path to the S3 credentials file (Required for first-time setup)",
		metavar="FILE"
	)

	parser.add_option(
		"-o", "--overwrite",
		dest="overwrite",
		help="Overwrite existing files in the destination bucket, if the MD5 checksum matches the system will anyhow skipp the file",
		default=True,
		action="store_false"
	)

	# Add sections to the help output
	parser.format_help = lambda: parser.get_usage() + """
Required Arguments:
  -l FILE, --local-file=FILE     Local file to upload
  -p PATH, --path-s3=PATH        Destination path in S3 bucket

Optional Arguments:
  -h, --help                     Show this help message and exit
  --version                      Show program's version number and exit
  -c FILE, --credentials=FILE    S3 credentials file (required for first use)
  -o, --no-overwrite            Do not overwrite existing files

Environment Setup:
  1. Ensure rclone is installed: https://rclone.org/install/
  2. For first-time use, provide credentials with -c option
  3. Subsequent uploads will use saved credentials

Return Codes:
  0 - Success
  1 - Error (credentials, file access, network, etc.)
"""

	options, args = parser.parse_args()

	try:
		if options.credentials_path:
			rclone_setup(Path(options.credentials_path))

		config_path = os.path.join(os.path.expanduser("~"), ".config", "rclone", "rclone.conf")
		if not os.path.exists(config_path):
			raise UploadError("S3 credentials not present. Please provide credentials using -c/--credentials-path")
		config = config_settings(config_path)

		if not options.local_file or not options.s3_path:
			parser.error("Both --local-file and --path-s3 are required")

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
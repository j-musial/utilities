#!/bin/bash
###############################
#release notes:
#Version 1.00 [20241220] - initial release  
###############################
version="1.00"
usage()
{
cat << EOF
#usage: $0 options
This utility copies the CLMS CRYOHYDRO products to CDSE.
example usage: 
First export environmental variables!!!!
export RCLONE_CONFIG_CLMS_TYPE=s3
export RCLONE_CONFIG_CLMS_ACCESS_KEY_ID=YOUR_CLMS_PUBLIC_S3_KEY
export RCLONE_CONFIG_CLMS_SECRET_ACCESS_KEY=YOUR_CLMS_PRIVATE_S3_KEY
export RCLONE_CONFIG_CLMS_REGION=default
export RCLONE_CONFIG_CLMS_ENDPOINT='https://s3.waw3-1.cloudferro.com'
export RCLONE_CONFIG_CLMS_PROVIDER='Ceph'
./CRYOHYDRO_upload.sh -l dummy.txt -p CLMS-CRYOHYDRO-INGESTION/test
OPTIONS:
   -h      this message
   -l      local path (i.e. file system) path to input file
   -o      Shall the destination file should be overwritten?
   -p	   Relative path of a file in the S3 bucket of the CLMS producer e.g. webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily/2024/20240713
   -v      version

EOF
}
while getopts “hl:op:v” OPTION; do
	case $OPTION in
		h)
			usage
			exit 0
			;;
		l)
			local_file=$OPTARG
			;;
		o)  
			overwrite=' --no-check-dest'
			;;
		p)  
			s3_path=$OPTARG  
			;;
		v)
			echo version $version
			exit 0
			;;
		?)
			usage
			exit 1
			;;
	esac
done
timestamp=$(date -u -d now '+%Y-%m-%dT%H:%M:%S')
last_modified=$(date -r $local_file '+%Y-%m-%dT%H:%M:%S')
file_size=$(du -smb --apparent-size $local_file | cut -f1)
md5_checksum=$(md5sum -b $local_file | cut -c-32)
rclone copy --s3-no-check-bucket --retries=20 --low-level-retries=20 --checksum --s3-use-multipart-uploads='false' --metadata --metadata-set uploaded=$timestamp --metadata-set WorkflowName="clms_upload" --metadata-set source-s3-endpoint-url=$RCLONE_CONFIG_CLMS_ENDPOINT --metadata-set file-size=$file_size --metadata-set md5=$md5_checksum --metadata-set last_modified=$last_modified --metadata-set s3-public-key=${RCLONE_CONFIG_CLMS_ACCESS_KEY_ID}${overwrite} $local_file CLMS:$s3_path

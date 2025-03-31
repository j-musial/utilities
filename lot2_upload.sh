#!/bin/bash
###############################
#release notes:
#Version 1.00 [20250318] - initial release
#Version 1.01 [20250331] - addition of batch upload example, additon of notification if upload fails or successful
###############################
version="1.01"
usage()
{
cat << EOF
#usage: $0 options
This utility copies the CLMS LOT2 products to CDSE.
example usage: 
First export environmental variables!!!!
export RCLONE_CONFIG_LOT2_TYPE=s3
export RCLONE_CONFIG_LOT2_ACCESS_KEY_ID=YOUR_CLMS_PUBLIC_S3_KEY
export RCLONE_CONFIG_LOT2_SECRET_ACCESS_KEY=YOUR_CLMS_PRIVATE_S3_KEY
export RCLONE_CONFIG_LOT2_REGION=default
export RCLONE_CONFIG_LOT2_ENDPOINT='https://s3.waw3-1.cloudferro.com'
export RCLONE_CONFIG_LOT2_PROVIDER='Ceph'
#Examples:
#
#Single file upload:
./lot2_upload.sh -l dummy.txt -p webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily/2024/20240713
#
#Batch upload if your directory structure follows CLMS convention e.g. webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily:
#Batch upload of all NetCDFs for dataset swe_5km_v2_daily residing localy in /home/johnlane/webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily:
find /home/johnlane/webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily -name "*.nc" | xargs -l -P 5 bash -c './lot2_upload.sh -l $0 -p $(dirname $0 | sed -z "s/.*webResources/webResources/")'
#Batch upload of all tiff for dataset swe_5km_v2_daily residing localy in /home/johnlane/webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily
find /home/johnlane/webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily -name "*.tif" | xargs -l -P 5 bash -c './lot2_upload.sh -l $0 -p $(dirname $0 | sed -z "s/.*webResources/webResources/")'
#Batch upload of all files residing localy in /home/johnlane/webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily
find /home/johnlane/webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily -type f | xargs -l -P 5 bash -c './lot2_upload.sh -l $0 -p $(dirname $0 | sed -z "s/.*webResources/webResources/")'

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
			s3_path="${OPTARG%/}"  
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
#########################################sanity checks
#verify if file exists in the local storage
if [ ! -r $local_file ]; then
	echo "ERROR: File $local_file does not exist in the local storage!"
	exit 2
fi

timestamp=$(date -u -d now '+%Y-%m-%dT%H:%M:%S')
last_modified=$(date -r $local_file '+%Y-%m-%dT%H:%M:%S')
file_size=$(du -smb --apparent-size $local_file | cut -f1)
md5_checksum=$(md5sum -b $local_file | cut -c-32)
rclone copy --s3-no-check-bucket --retries=20 --low-level-retries=20 --checksum --s3-use-multipart-uploads='false' --metadata --metadata-set uploaded=$timestamp --metadata-set WorkflowName="lot2_upload" --metadata-set source-s3-endpoint-url=$RCLONE_CONFIG_CLMS_ENDPOINT --metadata-set file-size=$file_size --metadata-set md5=$md5_checksum --metadata-set last_modified=$last_modified --metadata-set s3-public-key=${RCLONE_CONFIG_CLMS_ACCESS_KEY_ID} --metadata-set source_s3_path='s3://CLMS-ARCHIVE-LOT2/'${s3_path}/$(basename $local_file) --metadata-set source_cleanup=true --metadata-set product_to_replace='' $local_file LOT2:CLMS-ARCHIVE-LOT2/$s3_path
[ $? == 1 ] && echo "ERROR: Failed to upload $local_file" || echo "SUCCESS: Uploaded $local_file"

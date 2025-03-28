#!/bin/bash
###############################
#release notes:
#Version 1.00 [20250318] - initial release
#Version 1.10 [20250328] - added parallel upload support for files
#Version 1.20 [20250328] - added parallel upload support for folders with directory structure preservation
###############################
version="1.20"
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

# For single file upload:
./lot2_upload.sh -l file.nc -p webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily/2024/20240713

# For multiple files upload:
./lot2_upload.sh -d /path/to/files/*.nc -p webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily/2024/20240713 -j 4

# For directory upload (preserving structure):
./lot2_upload.sh -r /path/to/directory -p webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily/2024 -j 4

# For directory upload with file pattern filtering:
./lot2_upload.sh -r /path/to/directory -e "*.nc" -p webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily/2024 -j 4

OPTIONS:
   -h      This message
   -l      Local path to a single input file
   -d      Pattern for multiple input files (e.g., "/path/to/files/*.nc")
   -r      Root directory to upload recursively (preserves directory structure)
   -e      File extension/pattern filter when using -r (e.g., "*.nc" or "*.tif")
   -o      Shall the destination file should be overwritten?
   -p      Relative path of a file in the S3 bucket of the CLMS producer e.g. webResources/catalogTree/netcdf/snow_water_equivalent/swe_5km_v2_daily/2024/20240713
   -j      Number of parallel jobs (default: 3)
   -v      Version

EOF
}

# Default values
parallel_jobs=3
overwrite=""
file_filter="*"

while getopts "hl:d:r:e:op:j:v" OPTION; do
	case $OPTION in
		h)
			usage
			exit 0
			;;
		l)
			local_file=$OPTARG
			mode="single"
			;;
		d)
			file_pattern=$OPTARG
			mode="multiple"
			;;
		r)
			root_dir="${OPTARG%/}" # Remove trailing slash if present
			mode="recursive"
			;;
		e)
			file_filter=$OPTARG
			;;
		o)
			overwrite=' --no-check-dest'
			;;
		p)
			s3_path=$OPTARG
			;;
		j)
			parallel_jobs=$OPTARG
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

# Function to upload a single file
upload_file() {
    local local_file=$1
    local target_s3_path=$2

    # Verify if file exists in the local storage
    if [ ! -r "$local_file" ]; then
        echo "ERROR: File $local_file does not exist in the local storage!"
        return 2
    fi

    timestamp=$(date -u -d now '+%Y-%m-%dT%H:%M:%S')
    last_modified=$(date -r "$local_file" '+%Y-%m-%dT%H:%M:%S')
    file_size=$(du -smb --apparent-size "$local_file" | cut -f1)
    md5_checksum=$(md5sum -b "$local_file" | cut -c-32)

    echo "Uploading: $local_file to $target_s3_path"

    # Get the target directory
    local target_dir=$(dirname "$target_s3_path")

    rclone copy --s3-no-check-bucket --retries=20 --low-level-retries=20 --checksum --s3-use-multipart-uploads='false' \
        --metadata --metadata-set uploaded=$timestamp --metadata-set WorkflowName="lot2_upload" \
        --metadata-set source-s3-endpoint-url=$RCLONE_CONFIG_CLMS_ENDPOINT --metadata-set file-size=$file_size \
        --metadata-set md5=$md5_checksum --metadata-set last_modified=$last_modified \
        --metadata-set s3-public-key=${RCLONE_CONFIG_CLMS_ACCESS_KEY_ID} \
        --metadata-set source_s3_path='s3://CLMS-ARCHIVE-LOT2/'${target_s3_path} \
        --metadata-set source_cleanup=true --metadata-set product_to_replace='' "$local_file" "LOT2:CLMS-ARCHIVE-LOT2/$target_dir"

    local status=$?
    if [ $status -eq 0 ]; then
        echo "Success: $local_file"
    else
        echo "Failed: $local_file with status $status"
    fi

    return $status
}

# Check if s3_path is provided
if [ -z "$s3_path" ]; then
    echo "ERROR: S3 path (-p) is required!"
    usage
    exit 3
fi

# Create a temporary file for the file list in recursive mode
if [ "$mode" = "recursive" ]; then
    tmp_file_list=$(mktemp)
    trap "rm -f $tmp_file_list" EXIT
fi

# Main execution
if [ "$mode" = "single" ]; then
    # Single file mode
    upload_file "$local_file" "$s3_path/$(basename "$local_file")"
    exit $?

elif [ "$mode" = "multiple" ]; then
    # Multiple files mode
    echo "Starting parallel upload with $parallel_jobs jobs"

    # Expand the file pattern
    files=($(ls -1 $file_pattern 2>/dev/null))

    if [ ${#files[@]} -eq 0 ]; then
        echo "ERROR: No files found matching pattern: $file_pattern"
        exit 4
    fi

    echo "Found ${#files[@]} files to upload"

    # Prepare file and target path pairs
    file_list=()
    for file in "${files[@]}"; do
        file_list+=("$file:$s3_path/$(basename "$file")")
    done

elif [ "$mode" = "recursive" ]; then
    # Recursive directory mode
    echo "Starting recursive upload from $root_dir with $parallel_jobs jobs"

    if [ ! -d "$root_dir" ]; then
        echo "ERROR: Directory $root_dir does not exist!"
        exit 5
    fi

    # Find all files that match the filter, excluding hidden files/dirs
    echo "Scanning for files matching pattern: $file_filter"
    find "$root_dir" -type f -name "$file_filter" ! -path "*/\.*" | while read file; do
        # Get the relative path from root_dir
        rel_path="${file#$root_dir/}"
        # Get the directory part of the relative path
        rel_dir=$(dirname "$rel_path")

        # Construct the target S3 path
        if [ "$rel_dir" = "." ]; then
            # File is directly in root_dir
            target="${s3_path}/$(basename "$file")"
        else
            # File is in subdirectory
            target="${s3_path}/${rel_path}"
        fi

        # Add to temporary file list
        echo "$file:$target" >> "$tmp_file_list"
    done

    # Count the number of files
    file_count=$(wc -l < "$tmp_file_list")
    if [ $file_count -eq 0 ]; then
        echo "ERROR: No files found matching pattern: $file_filter in directory: $root_dir"
        exit 6
    fi

    echo "Found $file_count files to upload"

else
    echo "ERROR: One of -l (single file), -d (multiple files), or -r (recursive directory) must be specified"
    usage
    exit 7
fi

# Process the uploads in parallel
if [ "$mode" = "multiple" ] || [ "$mode" = "recursive" ]; then
    # Check if GNU Parallel is installed
    if command -v parallel &> /dev/null; then
        # Use GNU Parallel if available
        echo "Using GNU Parallel for uploads"
        export -f upload_file

        if [ "$mode" = "multiple" ]; then
            echo "${file_list[@]}" | tr ' ' '\n' | parallel --jobs $parallel_jobs --colsep ':' upload_file {1} {2}
        else
            cat "$tmp_file_list" | parallel --jobs $parallel_jobs --colsep ':' upload_file {1} {2}
        fi

        parallel_exit=$?
        if [ $parallel_exit -ne 0 ]; then
            echo "Some uploads failed. Check the output for details."
            exit 8
        fi
    else
        # Fallback to background processes with job control
        echo "GNU Parallel not found, using background processes"

        # Prepare the file list
        if [ "$mode" = "multiple" ]; then
            all_files=("${file_list[@]}")
        else
            mapfile -t all_files < "$tmp_file_list"
        fi

        active=0
        failed=0
        total=${#all_files[@]}
        completed=0

        for pair in "${all_files[@]}"; do
            # Split the pair into file and target
            IFS=':' read -r file target <<< "$pair"

            # Wait if we've reached the maximum number of parallel jobs
            while [ $active -ge $parallel_jobs ]; do
                wait -n
                status=$?
                active=$((active - 1))
                completed=$((completed + 1))
                if [ $status -ne 0 ]; then
                    failed=$((failed + 1))
                fi
                echo "Progress: $completed/$total completed, $failed failed, $active active"
            done

            # Start a new job
            upload_file "$file" "$target" &
            active=$((active + 1))
        done

        # Wait for remaining jobs to finish
        while [ $active -gt 0 ]; do
            wait -n
            status=$?
            active=$((active - 1))
            completed=$((completed + 1))
            if [ $status -ne 0 ]; then
                failed=$((failed + 1))
            fi
            echo "Progress: $completed/$total completed, $failed failed, $active active"
        done

        if [ $failed -gt 0 ]; then
            echo "$failed uploads failed out of $total"
            exit 9
        fi
    fi

    echo "All uploads completed successfully"
fi
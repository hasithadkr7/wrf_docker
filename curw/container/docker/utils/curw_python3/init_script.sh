#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=/wrf/gcs.json

echo "#### Reading running args..."
while getopts ":i:c:d:o:k:v:" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 c) CONFIG=$OPTARG;;
 d) DB=$OPTARG;;
 o) OW="True";;
 k) echo $OPTARG > /wrf/gcs.json;;
 v) bucket=$(echo $OPTARG | cut -d':' -f1)
    path=$(echo $OPTARG | cut -d':' -f2)
    echo "#### mounting $bucket to $path"
    gcsfuse ${bucket} ${path} ;;
 esac
done

echo "#### Pulling curwsl changes..."
cd /wrf/curwsl
git pull
cd /wrf

echo "#### Pulling curwsl-adapter changes..."
cd /wrf/curwsl_adapter
git pull
cd /wrf

echo "#### Running the data extraction procedures..."
python3.6 /wrf/curwsl/curw/container/docker/rainfall/ncar_wrf_extract/extract_data_wrf.py -run_id="$ID" \
-wrf_config="$CONFIG" -db_config="$DB" -overwrite="$OW"

#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=/wrf/gcs.json

echo "#### Reading running args..."
while getopts ":i:c:d:o:k:v:t:p:" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 c) CONFIG=$OPTARG;;
 d) DB=$OPTARG;;
 o) OW="True";;
 t) DT=$OPTARG;;
 k) GCS_KEY=$OPTARG
    if [ -f "$GCS_KEY" ]; then
        echo "#### using GCS KEY file location"
        cat "$GCS_KEY" > ${GOOGLE_APPLICATION_CREDENTIALS}
    else
        echo "#### using GCS KEY file content"
        echo "$GCS_KEY" > ${GOOGLE_APPLICATION_CREDENTIALS}
    fi
    ;;
 v) bucket=$(echo $OPTARG | cut -d':' -f1)
    path=$(echo $OPTARG | cut -d':' -f2)
    echo "#### mounting $bucket to $path"
    gcsfuse ${bucket} ${path} ;;
 p) PROCS=$OPTARG;;
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

encode_base64 () {
    [ -z "$1" ] && echo "" || echo "-$2=$( echo "$1" | base64  --wrap=0 )"
}

check_empty () {
        [ -z "$1" ] && echo "" || echo "-$2=$1"
}

echo "#### Running the data extraction procedures..."
#python3.6 /wrf/curwsl/curw/container/docker/rainfall/ncar_wrf_extract/extract_data_wrf.py -run_id="$ID" \
#-wrf_config="$CONFIG" -db_config="$DB" -overwrite="$OW" -data_type="$DT"
python3.6 /wrf/curwsl/curw/container/docker/rainfall/ncar_wrf_extract/extract_data_wrf.py \
                                $( check_empty "$ID" run_id ) \
                                $( check_empty "$OW" overwrite ) \
                                $( check_empty "$DT" data_type ) \
                                $( check_empty "$CONFIG" wrf_config ) \
                                $( check_empty "$DB" db_config ) \
                                $( check_empty "$PROCS" procedures )

#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=/wrf/gcs.json

echo "#### Reading running args..."
while getopts ":i:c:m:x:y:k:v:" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 c) CONFIG=$OPTARG;;
 m) MODE=$OPTARG;;
 x) WPS=$OPTARG;;
 y) INPUT=$OPTARG;;
 k) GCS_KEY=$OPTARG
    if [ -f "$GCS_KEY" ]; then
        echo "#### using GCS KEY file location"
        cat "$GCS_KEY" > ${GOOGLE_APPLICATION_CREDENTIALS}
    else
        echo "#### using GCS KEY file content"
        echo "$GCS_KEY" > ${GOOGLE_APPLICATION_CREDENTIALS}
    fi
    ;;
 v) bucket=$(echo "$OPTARG" | cut -d':' -f1)
    path=$(echo "$OPTARG" | cut -d':' -f2)
    echo "#### mounting $bucket to $path"
    gcsfuse "$bucket" "$path" ;;
 esac
done

echo "#### Pulling curwsl changes..."
cd /wrf/curwsl || exit
git pull
cd /wrf || exit

encode_base64 () {
    [ -z "$1" ] && echo "" || echo "-$2=$( echo "$1" | base64  --wrap=0 )"
}

check_empty () {
        [ -z "$1" ] && echo "" || echo "-$2=$1"
}

echo "#### Running WRF procedures..."
#python3.6 /wrf/curwsl/curw/container/docker/rainfall/ncar_wrf/run_wrf.py -run_id="$ID" -wrf_config="$CONFIG" -mode="$MODE" -nl_wps="$WPS" -nl_input="$INPUT"
python3.6 /wrf/curwsl/curw/container/docker/rainfall/ncar_wrf/run_wrf.py \
                                $( check_empty "$ID" run_id ) \
                                $( check_empty "$CONFIG" wrf_config ) \
                                $( check_empty "$MODE" mode ) \
                                $( check_empty "$WPS" nl_wps ) \
                                $( check_empty "$INPUT" nl_input )


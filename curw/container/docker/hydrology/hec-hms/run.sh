#!/usr/bin/env bash

err() { 1>&2 echo "$0: error $*"; return 1; }

mkdir -pf /hec-hms/config
export GOOGLE_APPLICATION_CREDENTIALS=/hec-hms/config/gcs.json

echo "#### Reading running args..."
while getopts ":i:w:f:k:v:c:p:h" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 w) WRF_ID=$OPTARG;;
 f) FORECAST_CMD=$OPTARG;;
 c) CONFIG=$OPTARG;;
 p) CONFIG_PATH=$OPTARG;;
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
    mkdir -pf "$path"
    gcsfuse "$bucket" "$path" ;;
 h) echo "Help text goes here!"
    exit 1 ;;
 :) err "Option -$OPTARG requires an argument." || exit 1 ;;
 \?) err "Invalid option: -$OPTARG" || exit 1 ;;
 esac
done

echo "#### Pulling curwsl changes..."
git pull

echo "#### Copying config for HEC-HMS..."
[ -n "$CONFIG" ] && [ -n "$CONFIG_PATH" ] && echo "$CONFIG" | base64 --decode  > "$CONFIG_PATH"

echo "WRF ID:".$WRF_ID
echo "HEC-HMS ID:".$ID
echo "#### Running HEC-HMS..."
#/hec-hms/Forecast.sh -u "$ID" -w "$WRF_ID" "$( echo "$FORECAST_CMD" | base64 --decode )"

/hec-hms/Forecast_150.sh -i -f -u "$ID" -w "$WRF_ID" "$( echo "$FORECAST_CMD" | base64 --decode )"

#!/usr/bin/env bash

export CURW_wrf_config=$(cat << EOM
{
	"wrf_home": "/wrf",
	"gfs_dir": "/wrf/gfs",
	"nfs_dir": "/wrf/output",
	"period": 0.25,
	"geog_dir": "/wrf/geog",
	"start_date": "2017-09-05_06:00"
}
EOM
)

export CURW_db_config=$(cat << EOM
{
  "host": "localhost",
  "user": "test",
  "password": "password",
  "db": "testdb"
}
EOM
)

export CURW_run_id=WRF_test_run1

encode_base64 () {
    [ -z "$1" ] && echo "" || echo "-$2=$( echo "$1" | base64  --wrap=0 )"
}

check_empty () {
        [ -z "$1" ] && echo "" || echo "-$2=$1"
}

#python3 ../extract_data_wrf.py
python3 ../extract_data_wrf.py  $( check_empty "$CURW_run_id" run_id ) \
                                $( encode_base64 "$CURW_wrf_config" wrf_config ) \
                                $( encode_base64 "$CURW_db_config" db_config )





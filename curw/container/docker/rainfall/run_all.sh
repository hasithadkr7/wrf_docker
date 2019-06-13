#!/usr/bin/env bash

#start_date=$(date +"%Y-%m-%d_%H:%M")
#run_id=wrf0_"$start_date"_$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 4 ; echo '')
#extract=1
#wrf=1

while getopts ":d:i:w:e:" option
do
 case "${option}"
 in
 p) prefix=$OPTARG;;
 d) start_date=$OPTARG;;
 i) run_id=$OPTARG;;
 e) extract=$OPTARG;;
 w) wrf=$OPTARG;;
 esac
done

[ -z "$prefix" ] && prefix="wrf0-6d"
[ -z "$start_date" ] && start_date=$(date +"%Y-%m-%d_%H:%M")
[ -z "$run_id" ] && run_id="$prefix"_"$start_date"_$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 4 ; echo '')
[ -z "$extract" ] && extract=1
[ -z "$wrf" ] && wrf=1

echo "start date: $start_date"

export CURW_run_id=${run_id}
echo "run id: $CURW_run_id"

CURW_nl_wps=$(cat << EOM
&share
 wrf_core = 'ARW',
 max_dom = 3,
 start_date = 'YYYY1-MM1-DD1_hh1:mm1:00','YYYY1-MM1-DD1_hh1:mm1:00','YYYY1-MM1-DD1_hh1:mm1:00',
 end_date   = 'YYYY2-MM2-DD2_hh2:mm2:00','YYYY2-MM2-DD2_hh2:mm2:00','YYYY2-MM2-DD2_hh2:mm2:00',
 interval_seconds = 10800,
 io_form_geogrid = 2,
/

&geogrid
 parent_id         =   1,   1, 2,
 parent_grid_ratio =   1,   3, 3,
 i_parent_start    =   1,  24, 35,
 j_parent_start    =   1,  26, 35,
 e_we              =  80, 103, 100,
 e_sn              =  90, 121, 163,
 geog_data_res     = '10m','5m','2m',
 dx = 27000,
 dy = 27000,
 map_proj = 'mercator',
 ref_lat   =  7.697,
 ref_lon   =  80.774,
 truelat1  =  7.697,
 truelat2  =  0,
 stand_lon =  80.774,
 geog_data_path = 'GEOG'
/

&ungrib
 out_format = 'WPS',
 prefix = 'FILE',
/

&metgrid
 fg_name = 'FILE'
 io_form_metgrid = 2,
/

EOM
)
export CURW_nl_wps

CURW_nl_input=$(cat << EOM
 &time_control
 run_days                            = RD0,
 run_hours                           = RH0,
 run_minutes                         = RM0,
 run_seconds                         = 0,
 start_year                          = YYYY1, YYYY1,  YYYY1,
 start_month                         = MM1, MM1,  MM1,
 start_day                           = DD1, DD1,  DD1,
 start_hour                          = hh1,   hh1,   hh1,
 start_minute                        = mm1,   mm1,   mm1,
 start_second                        = 00,   00,   00,
 end_year                            = YYYY2, YYYY2,  YYYY2,
 end_month                           = MM2, MM2,  MM2,
 end_day                             = DD2, DD2,  DD2,
 end_hour                            = hh2,   hh2,   hh2,
 end_minute                          = mm2,   mm2,   mm2,
 end_second                          = 00,   00,   00,
 interval_seconds                    = 10800
 input_from_file                     = .true.,.true.,.true.,
 history_interval                    = 180,  60,   60,
 frames_per_outfile                  = 1000, 1000, 1000,
 restart                             = .false.,
 restart_interval                    = 5000,
 io_form_history                     = 2
 io_form_restart                     = 2
 io_form_input                       = 2
 io_form_boundary                    = 2
 debug_level                         = 0
 /

 &domains
 time_step                           = 180,
 time_step_fract_num                 = 0,
 time_step_fract_den                 = 1,
 max_dom                             = 3,
 e_we                                = 80,    103,   100,
 e_sn                                = 90,    121,    163,
 e_vert                              = 30,    30,    30,
 p_top_requested                     = 5000,
 num_metgrid_levels                  = 32,
 num_metgrid_soil_levels             = 4,
 dx                                  = 27000, 9000,  3000,
 dy                                  = 27000, 9000,  3000,
 grid_id                             = 1,     2,     3,
 parent_id                           = 1,     1,     2,
 i_parent_start                      = 1,     24,    35,
 j_parent_start                      = 1,     26,    35,
 parent_grid_ratio                   = 1,     3,     3,
 parent_time_step_ratio              = 1,     3,     3,
 feedback                            = 1,
 smooth_option                       = 0
 /

 &physics
 mp_physics                          = 3,     3,     3,
 ra_lw_physics                       = 1,     1,     1,
 ra_sw_physics                       = 1,     1,     1,
 radt                                = 30,    10,    10,
 sf_sfclay_physics                   = 1,     1,     1,
 sf_surface_physics                  = 2,     2,     2,
 bl_pbl_physics                      = 1,     1,     1,
 bldt                                = 0,     0,     0,
 cu_physics                          = 1,     1,     1,
 cudt                                = 5,     5,     5,
 isfflx                              = 1,
 ifsnow                              = 0,
 icloud                              = 1,
 surface_input_source                = 1,
 num_soil_layers                     = 4,
 sf_urban_physics                    = 0,     0,     0,
 /

 &fdda
 /

 &dynamics
 w_damping                           = 0,
 diff_opt                            = 1,
 km_opt                              = 4,
 diff_6th_opt                        = 0,      0,      0,
 diff_6th_factor                     = 0.12,   0.12,   0.12,
 base_temp                           = 290.
 damp_opt                            = 0,
 zdamp                               = 5000.,  5000.,  5000.,
 dampcoef                            = 0.2,    0.2,    0.2
 khdif                               = 0,      0,      0,
 kvdif                               = 0,      0,      0,
 non_hydrostatic                     = .true., .true., .true.,
 moist_adv_opt                       = 1,      1,      1,
 scalar_adv_opt                      = 1,      1,      1,
 /

 &bdy_control
 spec_bdy_width                      = 5,
 spec_zone                           = 1,
 relax_zone                          = 4,
 specified                           = .true., .false.,.false.,
 nested                              = .false., .true., .true.,
 /

 &grib2
 /

 &namelist_quilt
 nio_tasks_per_group = 0,
 nio_groups = 1,
 /

EOM
)
export CURW_nl_input

CURW_wrf_config=$(cat << EOM
{
	"wrf_home": "/wrf",
	"gfs_dir": "/wrf/gfs",
	"nfs_dir": "/wrf/output",
	"period": 3,
	"geog_dir": "/wrf/geog",
	"start_date": "$start_date",
	"archive_dir": "/wrf/archive"
}
EOM
)
export CURW_wrf_config

CURW_db_config=$(cat << EOM
{
  "host": "localhost",
  "user": "test",
  "password": "password",
  "db": "testdb"
}
EOM
)
export CURW_db_config

export CURW_mode=all

gcs_key=$(cat << EOM
Add this!!!
EOM
)

# curw_nfs="/mnt/disks/curwsl_nfs"
# curw_archive="/mnt/disks/curwsl_archive"
geog_dir="/mnt/disks/workspace1/wrf-data/geog"

[ "$wrf" != 0 ] && echo "Running WPS and WRF" && docker run -i --rm --privileged -v $geog_dir:/wrf/geog nirandaperera/curw-wrf-391  /wrf/run_wrf.sh -c "$CURW_wrf_config" -m "$CURW_mode" -i "$CURW_run_id" -x "$CURW_nl_wps" -y "$CURW_nl_input" -k "$gcs_key" -v curwsl_nfs_1:/wrf/output -v curwsl_archive_1:/wrf/archive || echo "Running WPS and WRF Skipped"

[ "$extract" != 0 ] && echo "Running WRF extraction" && docker run -i --rm --privileged nirandaperera/curw-wrf-391-extract /wrf/extract_data_wrf.sh -c "$CURW_wrf_config" -i "$CURW_run_id" -d "$CURW_db_config" -k "$gcs_key" -o true -v curwsl_nfs_1:/wrf/output || echo "Running WRF extraction skipped"

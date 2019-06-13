#!/usr/bin/env bash

export CURW_nl_wps=$(cat << EOM
&share
 wrf_core = 'ARW',
 max_dom = 3,
 start_date = '2017-09-05_06:00:00','2017-09-05_06:00:00','2017-09-05_06:00:00',
 end_date   = '2017-09-05_12:00:00','2017-09-05_12:00:00','2017-09-05_12:00:00',
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

export CURW_nl_input=$(cat << EOM
 &time_control
 run_days                            = 0,
 run_hours                           = 6,
 run_minutes                         = 0,
 run_seconds                         = 0,
 start_year                          = 2017, 2017,  2017,
 start_month                         = 09, 09,  09,
 start_day                           = 05, 05,  05,
 start_hour                          = 06,   06,   06,
 start_minute                        = 00,   00,   00,
 start_second                        = 00,   00,   00,
 end_year                            = 2017, 2017,  2017,
 end_month                           = 09, 09,  09,
 end_day                             = 05, 05,  05,
 end_hour                            = 12,   12,   12,
 end_minute                          = 00,   00,   00,
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

export CURW_mode=test

export CURW_run_id=test_run1

encode_base64 () {
    [ -z "$1" ] && echo "" || echo "-$2=$( echo "$1" | base64  --wrap=0 )"
}

check_empty () {
        [ -z "$1" ] && echo "" || echo "-$2=$1"
}

#python3 ../run_wrf.py
python3 ../run_wrf.py $( check_empty "$CURW_run_id" run_id ) \
                      $( encode_base64 "$CURW_wrf_config" wrf_config ) \
                      $( check_empty "$CURW_mode" mode ) \
                      $( encode_base64 "$CURW_nl_wps" nl_wps ) \
                      $( encode_base64 "$CURW_nl_input" nl_input )

#export CURW_wrf_config=$(cat << EOM
#{
#	"wrf_home": "/mnt/disks/wrf-mod/temp",
#	"gfs_dir": "/mnt/disks/wrf-mod/temp/gfs",
#	"nfs_dir": "/mnt/disks/wrf-mod/temp/output",
#	"period": 0.25,
#	"geog_dir": "/mnt/disks/wrf-mod/DATA/geog",
#	"start_date": "2017-09-05_06:00"
#}
#EOM
#)





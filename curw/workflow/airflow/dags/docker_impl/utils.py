import json
import random
import string
import logging
import datetime as dt

from airflow.models import Variable


def get_exec_date_template():
    return '{{ execution_date.strftime(\'%%Y-%%m-%%d_%%H:%%M\') }}'


def get_run_id(run_name, suffix=None):
    return run_name + '_' + get_exec_date_template() + (('_' + suffix) if suffix else '')


def id_generator(size=4, chars=string.ascii_uppercase + string.digits + string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


def get_docker_cmd(run_id, wrf_config, mode, nl_wps, nl_input, gcs_volumes=None):
    cmd = '/wrf/run_wrf.sh -i \"%s\" -c \"%s\" -m \"%s\" -x \"%s\" -y \"%s\" ' % (
        run_id, wrf_config, mode, nl_wps, nl_input)

    if gcs_volumes:
        for vol in gcs_volumes:
            cmd = cmd + ' -v %s ' % vol

    return cmd


def get_start_date_from_context(context, period_hours=24):
    return context['execution_date'] + dt.timedelta(hours=period_hours)


def get_start_date_template(period_hours=24):
    return "{{ macros.datetime.strftime(execution_date + macros.timedelta(hours=%d), \'%%Y-%%m-%%d_%%H:%%M\') }}" \
           % period_hours


def get_docker_extract_cmd(run_id, wrf_config, db_config, gcs_volumes=None, overwrite=True):
    cmd = '/wrf/extract_data_wrf.sh -i \"%s\" -c \"%s\" -d \"%s\" -o \"%s\" ' % (
        run_id, wrf_config, db_config, str(overwrite))

    if gcs_volumes:
        for vol in gcs_volumes:
            cmd = cmd + ' -v %s ' % vol

    return cmd


def read_file(file_path, ignore_errors=False, json_file=False):
    data = ''
    try:
        with open(file_path, 'r') as f:
            if json_file:
                data = json.dumps(json.load(f))
            else:
                data = f.read()
    except FileNotFoundError as e:
        logging.error('File %s not found!' % file_path)
        if not ignore_errors:
            raise e

    return data


def check_airflow_variables(var_list, ignore_error=False, deserialize_json=False):
    out = {}
    for v in var_list:
        try:
            out[v] = Variable.get(v, deserialize_json=deserialize_json)
        except KeyError as e:
            logging.error('Key %s not found!' % v)
            out[v] = None
            if not ignore_error:
                raise e
    return out

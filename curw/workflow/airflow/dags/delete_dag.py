import argparse
import MySQLdb
import logging


def connect(_query, host, user, passwd, db):
    db = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
    cur = db.cursor()
    cur.execute(_query)
    db.commit()
    db.close()
    return

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('dag')
    parser.add_argument('u')
    parser.add_argument('h')
    parser.add_argument('p')
    parser.add_argument('db')
    args = parser.parse_args()
 
    query = {'delete from xcom where dag_id = "' + args.dag + '"',
             'delete from task_instance where dag_id = "' + args.dag + '"',
             'delete from sla_miss where dag_id = "' + args.dag + '"',
             'delete from log where dag_id = "' + args.dag + '"',
             'delete from job where dag_id = "' + args.dag + '"',
             'delete from dag_run where dag_id = "' + args.dag + '"',
             'delete from dag where dag_id = "' + args.dag + '"'}

    for value in query:
        logging.info(value)
        connect(value, args.h, args.u, args.p, args.db)

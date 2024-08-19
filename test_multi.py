import clickhouse_connect
import logging
import os
import multiprocessing

# HOST = "mmmx-master.idfaws.com"
HOST = "clickhouse-master.prod.mmmx"
PORT = 8123
USERNAME = "nail_kulanbaev"
PASSWORD = "BMZHmnVE"

THREADS = 16

def cdc_nn(params: list) -> str:
    thread_id = os.getpid()
    logging.basicConfig(format='%(asctime)s %(message)s', filename=f'multi_test_ch_{thread_id}.log', encoding='utf-8', level=logging.DEBUG, filemode='w')
    logger = multiprocessing.get_logger()
    cl = clickhouse_connect.get_client(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
    try:
        credit_id = params[0]
        date_requested = params[1]
        sql = f"select score from mx_scoring.score_cdc_nn(credit_id = {credit_id}, credit_number = 1) limit 1"
        score = cl.query(sql)
        result = f"credit_id = {credit_id}, date_requested = {date_requested}, score = {score.first_row[0]}"
        logger.info(result)
        return result
    except Exception as e:
        logger.error("Exception: ", exc_info=e)
    finally:
        cl.close()

if __name__ == "__main__":
    print("Start testing")
    client = clickhouse_connect.get_client(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
    result = client.query('select creditId, uploadDate from mx_master.cdc_parsed_response where uploadDate >= today()-2 limit 600')
    print(f"Found {len(result.result_rows)}")
    with multiprocessing.Pool(processes=THREADS) as pool:
        outputs = pool.map(cdc_nn, result.result_rows)
    print(outputs)

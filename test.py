import clickhouse_connect
import logging

HOST = "mmmx-master.idfaws.com"
PORT = 8123
USERNAME = ""
PASSWORD = ""

def test_ch_score(log: logging.Logger):
    print("Start testing")
    client = clickhouse_connect.get_client(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
    result = client.query('select creditId, uploadDate from mx_master.cdc_parsed_response where uploadDate >= today() limit 100')
    print(f"Found {len(result.result_rows)}")
    for r in result.result_rows:
        credit_id = r[0]
        date_requested = r[1]
        sql = f"select score from mx_scoring.score_cdc_nn(credit_id = {credit_id}, credit_number = 1, dt = '{date_requested}') limit 1"
        cl = clickhouse_connect.get_client(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
        score = cl.query(sql)
        log.info(f"credit_id = {credit_id}, date_requested = {date_requested}, score = {score.first_row[0]}")
        cl.close()

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', filename='test_ch.log', encoding='utf-8', level=logging.DEBUG, filemode='w')
    logger = logging.getLogger(__name__)
    test_ch_score(logger)

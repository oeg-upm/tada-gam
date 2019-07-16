import requests
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_elected_tables(gold_tables, elect_ports, host_url):
    """
    :param gold_tables:
    :param elect_ports:
    :param host_url:
    :return:
    """
    processed_tables = []
    for elect_port in elect_ports:
        url = host_url+":"+str(elect_port)+"/status"
        response = requests.get(url)
        j = response.json()
        for table in j["apples"]:
            if table["status"] != "Complete":
                logger.info("processed until now: %d" % len(processed_tables))
                return None
            processed_tables.append(table)
    if len(processed_tables) == len(gold_tables.keys()):
        return processed_tables
    else:
        logger.info("processed: %d from %d" % (len(processed_tables), len(gold_tables.keys())))
    return None


def get_scores(gold_tables, processed_tables):
    """
    :param gold_tables: dict [fname]=col_id
    :param processed_tables: list of dicts
    :return:
    """
    correct = 0
    incorrect = 0
    notfound = 0
    for ptable in processed_tables:
        fname = ptable["apple"]
        if ptable["elected"] < 0 and gold_tables[fname] >= 0 :
            logger.info("notfound: "+fname)
            notfound += 1
        elif ptable["elected"] == gold_tables[fname]:
            correct += 1
        else:
            incorrect +=1
            logger.info("incorrect: "+fname)
    prec = correct/(correct+incorrect*1.0)
    rec = correct/(correct+notfound*1.0)
    f1 = prec * rec * 2 / (prec+rec)
    logger.info("correct: %d, incorrect: %d, notfound: %d" % (correct, incorrect, notfound))
    logger.info("precision: %1.3f, recall: %1.3f, f1: %1.3f" % (prec, rec, f1))
    return prec, rec, f1
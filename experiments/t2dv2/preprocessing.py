from __init__ import *
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import json
import unicodecsv as csv
# import csv
import logging
from commons.logger import set_config
import chardet

logger = set_config(logging.getLogger(__name__))

DATA_DIR = "data"
DEST_DIR = "../../local_data/t2dv2"


def json_to_csv(fname, overwrite=False):
    """
    :param fname: of the json file
    :return:
    """
    csv_fname = fname[:-4] + "csv"
    csv_dest = os.path.join(DEST_DIR, csv_fname)
    if os.path.exists(csv_dest) and not overwrite:
        logger.info("%s already exists" % csv_dest)
        return
    json_fdir = os.path.join(DATA_DIR, "tables", fname)
    f = open(json_fdir)
    s = f.read()
    detected_encoding = chardet.detect(s)['encoding']
    logger.debug("detected encoding %s for %s" % (detected_encoding, fname))
    decoded_s = s.decode(detected_encoding)
    j = json.loads(decoded_s)
    f.close()
    # print("relation: ")
    # print(j["relation"])
#    table = j["relation"]
    table = zip(*j["relation"])
    # print("table: ")
    # print(table)
    # sep = ","
    # rows = []
    # for row in table:
    #     # print(type(row[0]))
    #     # print(row)
    #     r = sep.join(row)
    #     rows.append(r)
    # csv_content = "\n".join(rows)
    # f = open(csv_dest, "w")
    # f.write(csv_content.encode('utf8'))
    # f.close()
    # logger.debug("generate csv %s" % csv_dest)

    with open(csv_dest, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        for row in table:
            writer.writerow(row)

    logger.debug("generate csv %s" % csv_dest)


def export_files_to_csv():
    if not os.path.exists(DEST_DIR):
        os.mkdir(DEST_DIR)
    classes_dir = os.path.join(DATA_DIR, "classes_GS.csv")
    with open(classes_dir, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            json_fname = row[0].strip()[:-6]+"json"
            json_to_csv(json_fname, overwrite=False)


if __name__ == "__main__":
    export_files_to_csv()

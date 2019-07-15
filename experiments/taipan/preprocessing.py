from subprocess import call
import commons
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


T2D_TABLES_DIR = "data/TAIPAN-develop/data/tables_complete"


def download_taipian_repo():
    comm = "wget https://github.com/dice-group/TAIPAN/archive/develop.zip"
    call(comm, shell=True)
    comm = "unzip develop.zip"
    call(comm, shell=True)
    comm = "rm develop.zip"
    call(comm, shell=True)


def get_t2d_relevant_data():
    comm = "cp ./data/TAIPAN-develop/data/subject_column/subject_column_gold.csv ./data/"
    call(comm, shell=True)
    comm = "mkdir data/t2d"
    call(comm, shell=True)
    pairs = commons.get_t2d_gold()
    print("pairs: %d" % len(pairs))
    for pair in pairs:
        fname = pair['fname']
        fdir = os.path.join(T2D_TABLES_DIR, fname)
        comm = "cp %s %s" % (fdir, 'data/t2d')
        call(comm, shell=True)


def prepare():
    if os.path.exists("./data/subject_column_gold.csv"):
        logger.info("the subject column gold for t2d is already there")
        return

    download_taipian_repo()
    get_t2d_relevant_data()


if __name__ == '__main__':
    prepare()

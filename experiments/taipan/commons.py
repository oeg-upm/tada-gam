import csv

T2D_DIR = "data/subject_column_gold.csv"


def get_t2d_gold():
    """
    :return: list of dict {'fname': <the name of the file>, 'col_id': <int col_id>}
    """
    pairs = []
    with open(T2D_DIR, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            print row
            print len(row)
            row_data = row #row.split(',')
            if len(row_data) == 2:
                d = {'fname': row[0], 'col_id': int(row[1])}
                pairs.append(d)
            else:
                break
    return pairs
import pandas as pd
import sys
import os
import numpy as np
from label_experiment import UPLOAD_DIR


def extract_subject_column(fdir, column_idx):
    df = pd.read_csv(fdir, header=None)
    s = df[column_idx]
    content = "\t".join([str(ss) for ss in s if ss is not np.nan])
    f = open("generated_sample.tsv","w")
    f.write(content)
    f.close()

if __name__ == '__main__':
    fname = sys.argv[1]
    colidx = int(sys.argv[2])
    extract_subject_column(os.path.join(UPLOAD_DIR, fname), colidx)




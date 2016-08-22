#!/usr/bin/python
# -*- utf8 -*-
import pandas as pd
from collections import Counter
import json
import multiprocessing as mp
import os
from functools import partial
import itertools
import glob

def read_all():
    flist = glob.glob("features/part_feature/part_feature_x??_brand_info_brand_info_label.csv")
    print len(flist)
    df_list = [pd.read_csv(f, sep='\x01') for f in flist]
    result = pd.concat(df_list)

if __name__ == "__main__":
    read_all()


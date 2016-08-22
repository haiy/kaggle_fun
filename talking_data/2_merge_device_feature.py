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

def part_file_merge(in_mem_df, part_file_path):
    print 'input part:', part_file_path
    part_df = pd.read_csv(part_file_path,sep = '\x01')
    part_df['device_id'] = part_df['device_id'].astype(str)
    merge_df = pd.merge(in_mem_df, part_df, on = 'device_id', how='right')
    merge_df.to_csv(part_file_path.strip('.csv') + "_brand_info_label.csv", sep='\x01', index=False)

def map_files():
    #part_files = glob.glob("features/part_feature/part_feature_x??.csv")
    #in_mem_df = pd.read_csv("raw_csv/phone_brand_device_model.csv")

    part_files = glob.glob("features/part_feature/part_feature_x??_brand_info.csv")
    in_mem_df = pd.read_csv("features/train_test.csv")
    in_mem_df['device_id'] = in_mem_df['device_id'].astype(str)

    #part_files = part_files[:3]
    #print part_files

    pool = mp.Pool(16)
    partial_func = partial(part_file_merge, in_mem_df)

    pool.map(partial_func, part_files)
    mp.freeze_support()

def train_test_merge():
    train = pd.read_csv('raw_csv/gender_age_train.csv')
    test = pd.read_csv('raw_csv/gender_age_test.csv')
    train['is_train'] = 1
    train['device_id'] = train['device_id'].astype(str)
    test['is_test'] = 1
    test['device_id'] = test['device_id'].astype(str)
    train_test_info = pd.merge(train, test, how='outer', on='device_id')
    train_test_info.to_csv('features/train_test.csv', index=False)


if __name__ == '__main__':
    map_files()
    #train_test_merge()




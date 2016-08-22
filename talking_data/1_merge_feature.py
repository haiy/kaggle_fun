#!/usr/bin/python
# -*- utf8 -*-
import pandas as pd
from collections import Counter
import json
import multiprocessing as mp
import os
from functools import partial
import itertools


#http://stackoverflow.com/questions/24980437/pandas-groupby-and-then-merge-on-original-table
#http://stackoverflow.com/questions/13854476/pandas-transform-doesnt-work-sorting-groupby-output/13854901#13854901
#
dir_path = "/home/h/Desktop/talking_data/csv/"
out_path = "/home/h/Desktop/talking_data/features/"

def app_label_feature():
    app_labels = pd.read_csv(dir_path + "app_labels.csv")
    grouped_labels = app_labels.groupby("app_id", as_index=False)
    app_label_info = grouped_labels.agg(lambda x: ",".join(list(x['label_id'].apply(str))))
    return app_label_info

def app_events_label_feature(app_label_info):
    app_events = pd.read_csv(dir_path + "app_events.csv")
    merged_events = pd.merge(app_events, app_label_info, how = 'inner', on = 'app_id')
    merged_events.to_csv(out_path + "app_events_label.csv", index = False)
    #grouped_events = merged_events.groupby("event_id", as_index=False)
    #app_event_info = grouped_events.apply(events_group_stat)
    #return app_event_info

def event_group_stat(group_df):
    installed_apps = []
    installed_labels = []
    active_apps = []
    active_labels = []
    for row in group_df.itertuples(index=False):
        #print type(row), row
        (event_id,app_id,is_installed,is_active,labels) = row
        if is_installed == 1:
            installed_apps.append(str(app_id))
            installed_labels.append(str(labels))

        if is_active == 1:
            active_apps.append(str(app_id))
            active_labels.append(str(labels))

    result_dict = {}
    result_dict["installed_apps"] = ",".join(installed_apps)
    result_dict["installed_labels"] = str(Counter(",".join(installed_labels).split(",")))
    result_dict["active_apps"] = ",".join(active_apps)
    result_dict["active_labels"] = str(Counter(",".join(active_labels).split(",")))
    #print result_dict
    return result_dict
    #return pd.DataFrame(result_dict)


def merged_events_features():
    merged_events = pd.read_csv(dir_path + "merged_events.csv")
    grouped_events = merged_events.groupby("event_id", as_index=False)#.agg(event_group_stat)
    fp = open(out_path + "merged_events_features.csv", 'w')
    i = 0
    for name, group in grouped_events:
        #print type(name), type(group)
        #print name
        #print group
        single_df = event_group_stat(group)
        single_df["event_id"] = name
        json.dump(single_df, fp)
        fp.write("\n")

        #single_df.to_csv("merged_events_features.csv", mode='a')
    #grouped_events.to_csv("merged_events_features.csv")
    fp.close()

def read_merged_event_features(file_path):
    #fp = open("merged_events_features.csv")
    fp = open(file_path)
    data = fp.readlines()
    data_striped = map(lambda x: x.rstrip("\n"), data)
    data_json_str =  "[" + ','.join(data_striped) + "]"
    data_df = pd.read_json(data_json_str)
    return data_df

def merge_all_feature(file_name, events_df):
    merged_events_df = read_merged_event_features(out_path + "merge_feature/"+ file_name)
    all_merged_features = pd.merge(events_df, merged_events_df, on="event_id", how = "inner")
    all_merged_features.to_csv(out_path + "part_feature/part_feature_"+file_name + ".csv", sep='\x01', index=False)

def merge_all_feature_star(a_b):
    return merge_all_feature(*a_b)

def map_join():
    pool = mp.Pool(16)
    file_list = os.listdir(out_path + "/merge_feature/")
    print file_list
    events_df = pd.read_csv(dir_path + "events.csv")
    pool.map(partial(merge_all_feature, events_df=events_df),file_list)
    #pool.map(merge_all_feature_star, itertools.izip(file_list, itertools.repeat(events_df)))

def merge_brand_train_test():
    phone_bran_info = pd.read_csv(dir_path + "phone_brand_device_model.csv")
    gender_age_train = pd.read_csv(dir_path + "gender_age_train.csv")
    gender_age_test = pd.read_csv(dir_path + "gender_age_test.csv")

def test():
    #app_label_info = app_label_feature()
    #app_events_cat_feature(app_label_info)
    #merged_events_features()
    #read_merged_event_features()
    pass

def test2():
      file_list = os.listdir(out_path + "/merge_feature/")
      events_df = pd.read_csv(dir_path + "events.csv")

      merge_all_feature(file_list[0], events_df)
      #pool.map(partial(merge_all_feature, events=events_df),file_list)
      #pool.map(merge_all_feature_star, itertools.izip(file_list, itertools.repeat(events_df)))
      pass

if __name__ == '__main__':
    #test2()
    map_join()
    mp.freeze_support()

import numpy as np
import pandas as pd
import multiprocessing as mp
import os
from functools import partial
import glob
import itertools
import shutil

def in_mem_read_f(in_p):
    pd.read_csv(in_p, sep='\x01', names = list('ABCD'))

def part_read_f(in_p):
    pd.read_csv(in_p, sep='\x01', name = list('ABCD'))

def out_write_f(in_df, single_part_path):
    out = single_part_path + "_out"
    in_df.to_csv(out_path, sep='\x01', index=False)

def split_big(big_path, out_dir, prefix, chunk_size):
    cmd = "split -l %s %s %s " %(chunk_size, big_path, out_dir+ "/" + prefix)
    os.system(cmd)
    flist =  glob.glob(out_dir+"/"+prefix + "*")
    return flist

def act(in_mem_part_df, join_col, join_method, part_read_f, out_write_f, single_part_path):
    print join_col
    #single_part_df = part_read_f(single_part_path)
    #merged_df = pd.merge(in_mem_part_df, single_part_df, on=join_col, how = join_method)
    #out_write_f(merged_df, single_part_path)

def map_join(in_mem_path, in_mem_read_f,\
        part_files, part_read_f,\
        join_col, join_method,\
        out_write_f, pool_size):

    in_mem_df = in_mem_read_f(in_mem_path)

    print type(in_mem_df), in_mem_path
    print in_mem_df
    print join_col
    print join_method
    print part_read_f

    p_func = partial(act, in_mem_df, join_col, join_method,\
            part_read_f, out_write_f)

    pool = mp.Pool(pool_size)
    pool.map(p_func, part_files)
    mp.freeze_support()

def single_file_handle(in_mem_df, join_col, join_method, single_file_path):
    single_df = pd.read_csv(single_file_path, index=False, sep = '\x01')
    merge_df = pd.merge(in_mem_df, single_df, on=join_col, how = join_method)
    merge_df.

def test():
    tmp_split_dir = "tmp_split"
    try:
        shutil.rmtree(tmp_split_dir)
    except OSError as err:
        print(err)

    os.mkdir(tmp_split_dir)

    small_df = pd.DataFrame(np.random.randint(1,10,(10,4)), columns = list('ABCD'))
    big_df = pd.DataFrame(np.random.randint(1,10,(50,4)), columns = list('ABCD'))

    big_path = tmp_split_dir + "/big_df.csv"
    small_path = tmp_split_dir + "/small_df.csv"
    small_df.to_csv(small_path, index=False, sep='\x01')
    big_df.to_csv(big_path, index=False, sep='\x01')

    part_files = split_big(big_path, tmp_split_dir, "raw_", 10)

    print part_files

    map_join( small_path, in_mem_read_f, part_files, \
            part_read_f, 'A', 'inner', out_write_f, 16)


if __name__ == '__main__':
    test()

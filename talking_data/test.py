import numpy as np
import pandas as pd
import multiprocessing
import os
from functools import partial
import glob
import itertools
import shutil

def f(a, b, c):
    print("{} {} {}".format(a, b, c))

def main():
    iterable = [1, 2, 3, 4, 5]
    pool = multiprocessing.Pool()
    a = "hi"
    b = "there"
    func = partial(f, a, b)
    pool.map(func, iterable)
    pool.close()
    pool.join()

if __name__ == "__main__":
    main()

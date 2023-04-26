import csv
from datetime import datetime
import json
import os
from os import makedirs
from os.path import exists
from tqdm import tqdm
import pandas as pd
from re import sub, match

from base import Preprocessing

"""
Here the aim is to build a unique dataframe coming from the different csv files contained in COCI
"""

INPUT_PATH = "/Volumes/Extreme SSD/Data OpenScience/COCI/6741422/"
OUTPUT_PATH = "/Volumes/Extreme SSD/Data OpenScience/COCI/6741422/merged.csv"

coci_directories = os.listdir(INPUT_PATH)
coci_directories = [x for x in coci_directories if not x.startswith('._')]
#print(coci_directories)

for coci_directory in coci_directories:
    directory_path = INPUT_PATH+coci_directory
    files_in_directory = os.listdir(directory_path)
    files_in_directory = [x for x in files_in_directory if not x.startswith('._')]


def file_merge():


#data1_import = pd.read_csv('data1.csv')             # Read first CSV file
#data2_import = pd.read_csv('data2.csv')             # Read second CSV file
#
#data_merge = pd.merge(data1_import,                 # Full outer join
#                      data2_import,
#                      on = "ID",
#                      how = "outer")
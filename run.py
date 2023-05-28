import csv
from datetime import datetime
import os
from os import makedirs, walk
from os.path import exists, isdir, basename, join, sep
from typing import Dict
from tqdm import tqdm
import pandas as pd
from zipfile import ZipFile
import zstandard as zstd
from abc import ABCMeta, abstractmethod
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from lib.csv_manager import CSVManager
from preprocess.base import Preprocessing
from re import sub, match

from preprocess.meta_preprocess import MetaPreProcessing
from preprocess.erih_preprocess import ErihPreProcessing
from preprocess.coci_preprocess import CociPreProcessing
from erih_meta import ErihMeta
from counter import Counter
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description='Citation counter')
    
    # META preprocessor -- parameters
    parser.add_argument('--OCMeta_input_path', type=str, required=True,
                        help='Path to unprocessed Meta zip file')
    parser.add_argument('--OCMeta_processed_path', type=str, required=True,
                        help='Path in which you want to store the processed Meta data')
    parser.add_argument('--OCMeta_interval', type=int, required=False, default=10000,
                        help='Desired number of lines for processed Meta files')

    # ERIH preprocessor -- parameters  
    parser.add_argument('--ErihPlus_input_path', type=str, required=True,
                        help='Path to unprocessed ErihPlus csv file')
    parser.add_argument('--ErihPlus_processed_path', type=str, required=True,
                        help='Path in which you want to store the processed ErihPlus data')  
    
    # COCI preprocessor -- parameters
    parser.add_argument('--COCI_input_path', type=str, required=True,
                        help='Path to unprocessed COCI zip file')
    parser.add_argument('--COCI_processed_path', type=str, required=True,
                        help='Path in which you want to store the processed COCI data')
    parser.add_argument('--COCI_interval', type=int, required=False, default=10000,
                        help='Desired number of lines for processed COCI files')
    parser.add_argument('--COCI_meta_list_dois', type=str, required=True,
                        help='Path to folder with files containing the list of DOIs excluded from Meta')
    parser.add_argument('--list_DOI_not_in_Meta', type=bool, required=False, default=True,
                        help='Specify if you want to produce a list of DOIs not in Meta but in COCI')
    
    
    # ERIH-META preprocessor -- parameters
    parser.add_argument('--ERIH_META_processed_path', type=str, required=True,
                        help='Path in which you want to store the processed ERIH-META data')
    parser.add_argument('--ERIH_META_interval', type=int, required=False, default=10000,
                        help='Desired number of lines for processed ERIH-META files')
    
    # COUNTER -- parameters
    parser.add_argument('--num_cpus', type=int, required=False, default=None,
                        help='Number of cpus to use for parallel processing')
    parser.add_argument('--output_dir', type=str, required=False, default=r'OutputFiles/',
                        help='Path in which you want to store the output additional files created by the counter')
    parser.add_argument('--create_subfiles', type=bool, required=False, default=False,
                        help='Specify if you want to create additional subfiles during the process')
    parser.add_argument('--answer_to_q1', type=bool, required=False, default=True,
                        help='Specify if you want to answer to question 1')
    parser.add_argument('--answer_to_q2', type=bool, required=False, default=False,
                        help='Specify if you want to answer to question 2')
    parser.add_argument('--answer_to_q3', type=bool, required=False, default=False,
                        help='Specify if you want to answer to question 3')
    parser.add_argument('--interval', type=int, required=False, default=10000,
                        help='Desired number of lines for processed files')
    
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    print(args)

    meta_processor = MetaPreProcessing(args.OCMeta_input_path, args.OCMeta_processed_path, args.OCMeta_interval)
    meta_processor.split_input()
    meta_processor.create_list_dois()

    erih_preprocessor = ErihPreProcessing(args.ErihPlus_input_path, args.ErihPlus_processed_path)
    erih_preprocessor.write_csv()

    coci_preprocessor = CociPreProcessing(args.COCI_input_path, args.COCI_processed_path, args.COCI_interval, args.COCI_meta_list_dois)
    coci_preprocessor.split_input(args.list_DOI_not_in_Meta)

    erih_meta = ErihMeta(args.OCMeta_processed_path, args.ErihPlus_processed_path, args.ERIH_META_processed_path, args.ERIH_META_interval)
    erih_meta.erih_meta()

    counter = Counter(args.COCI_processed_path, args.ERIH_META_processed_path, args.num_cpus)
    count = counter.execute_count(args.output_dir, args.create_subfiles, args.answer_to_q1, args.answer_to_q2, args.answer_to_q3, args.interval)
    print(count)


if __name__ == '__main__':
    main()

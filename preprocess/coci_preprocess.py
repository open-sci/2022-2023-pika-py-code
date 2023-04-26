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


class CociPreProcessing(Preprocessing):
    _req_type = ".csv"
    _accepted_ids = "doi"
    _entity_columns_to_discard = ["oci", "creation", "timespan", "journal_sc", "author_sc"]
    _entity_columns_to_keep = ["citing", "cited"]

    def __init__(self, input_dir, output_dir, interval):
        self._input_dir = input_dir
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        super(CociPreProcessing, self).__init__()

    def splitted_to_file(self, cur_n, lines, type=None):
        if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
            filename = "filtered_" + str(cur_n // self._interval) + self._req_type
            if exists(os.path.join(self._output_dir, filename)):
                cur_datetime = datetime.now()
                dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type
            with open(os.path.join(self._output_dir, filename), "w", encoding="utf8", newline="") as f_out:
                keys = self._entity_columns_to_keep
                dict_writer = csv.DictWriter(f_out, delimiter=",", quoting=csv.QUOTE_ALL, escapechar="\\",
                                             fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(lines)
                f_out.close()
            lines = []
            return lines
        else:
            return lines

    def split_input(self):
        # an empty list to store the filtered entities to be saved in the output files is created
        lines = []
        count = 0
        # iterate over the input data
        all_files, targz_fd = self.get_all_files(self._input_dir, ".zip")
        for i, el in enumerate(tqdm(all_files), 1):
            if el:
                all_files_unzipped, targz_fd_el = self.get_all_files(el, self._req_type)
                for file_idx, file in enumerate(tqdm(all_files_unzipped), 1):
                    chunksize = 100000
                    with pd.read_csv(file, usecols= self._entity_columns_to_keep, chunksize=chunksize, sep=",") as reader:
                        for chunk in reader:
                            chunk.fillna("", inplace=True)
                            df_dict_list = chunk.to_dict("records")
                            for line in df_dict_list:
                                count += 1
                                doi_citing = line.get("citing")
                                doi_cited = line.get("cited")
                                line["citing"] = str(doi_citing)
                                line["cited"] = str(doi_cited)
                                lines.append(line)
                                if int(count) != 0 and int(count) % int(self._interval) == 0:
                                    lines = self.splitted_to_file(count, lines)
        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines)

###################################################################

import argparse

parser = argparse.ArgumentParser(description='COCI Preprocessing')
parser.add_argument('--input-dir', type=str, help='Input directory path')
parser.add_argument('--output-dir', type=str, help='Output directory path')
parser.add_argument('--interval', type=int, default=3000000, help='Interval for splitting input files')

args = parser.parse_args()

coci_preprocessing = CociPreProcessing(input_dir=args.input_dir, output_dir=args.output_dir, interval=args.interval)
coci_preprocessing.split_input()
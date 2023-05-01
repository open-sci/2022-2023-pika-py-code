from csv import reader
from csv import writer
import os
import sys
from argparse import ArgumentParser
from tarfile import TarInfo
import pandas as pd
from tqdm import tqdm
import zipfile
import re
import datetime
import csv


class ErihMeta:
    _entity_columns_to_keep = ["id", "title", "author", "issue", "volume", "venue", "page", "pub_date", "type", "publisher", "editor", "erih_disciplines"]

    def __init__(self, meta_preprocessed_path, erih_preprocessed_path, output_erih_meta, interval):
        self._meta_preprocessed_path = meta_preprocessed_path
        self._erih_preprocessed_path = erih_preprocessed_path
        self._output_erih_meta = output_erih_meta
        self._interval = interval
        if not os.path.exists(self._output_erih_meta):
            os.makedirs(self._output_erih_meta)
        self._interval = interval
        super(ErihMeta, self).__init__()


    def find_erih_venue(self, issn_list):
        erih_disciplines = ""
        erih = pd.read_csv(self._erih_preprocessed_path, sep=";")
        for index, row in erih.iterrows():
            for issn in issn_list:
                if issn in row['venue_id']:
                    erih_disciplines = row['ERIH_disciplines']
        return erih_disciplines

    def get_all_files(self, i_dir_or_compr, req_type):
        result = []
        if os.path.isdir(i_dir_or_compr):
                for cur_dir, cur_subdir, cur_files in os.walk(i_dir_or_compr):
                    for cur_file in cur_files:
                        if cur_file.endswith(req_type) and not os.path.basename(cur_file).startswith("."):
                            result.append(os.path.join(cur_dir, cur_file))
        return result

    def splitted_to_file(self, cur_n, lines, type=None):
            if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
                filename = "filtered_" + str(cur_n // self._interval) + '.csv'
                if os.path.exists(os.path.join(self._output_erih_meta, filename)):
                    cur_datetime = datetime.now()
                    dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                    filename = filename[:-len('.csv')] + "_" + dt_string + '.csv'
                with open(os.path.join(self._output_erih_meta, filename), "w", encoding="utf8", newline="") as f_out:
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

    def erih_meta(self):
        lines = []
        count = 0
        meta_csv = self.get_all_files(self._meta_preprocessed_path, '.csv')
        for file_idx, file in enumerate(tqdm(meta_csv), 1):
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        count += 1
                        issn_meta = line.get('venue')
                        if issn_meta:
                            issn_meta = issn_meta.split(" ")
                            erih_disciplines = self.find_erih_venue(issn_meta)
                            line['erih_disciplines'] = erih_disciplines
                        else:
                            line['erih_disciplines'] = ""
                        lines.append(line)
                        if int(count) != 0 and int(count) % int(self._interval) == 0:
                            lines = self.splitted_to_file(count, lines)
        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines) 
    

                
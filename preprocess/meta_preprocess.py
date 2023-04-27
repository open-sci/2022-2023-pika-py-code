import csv
from datetime import datetime
import json
import os
from os import makedirs
from os.path import exists
from tqdm import tqdm
import pandas as pd
from re import sub, match

from preprocess.base import Preprocessing


class MetaPreProcessing(Preprocessing):
    _req_type = ".csv"
    _accepted_ids = "doi"
    _accepted_ids_venue = "issn"
    _entity_columns_to_update = ["id", "venue"]
    _all_the_columns = ["id", "title", "author", "issue", "volume", "venue", "page", "pub_date", "type", "publisher", "editor"]

    def __init__(self, input_dir, output_dir, interval):
        self._input_dir = input_dir
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        super(MetaPreProcessing, self).__init__()

    def splitted_to_file(self, cur_n, lines, type=None):
        if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
            filename = "filtered_" + str(cur_n // self._interval) + self._req_type
            if exists(os.path.join(self._output_dir, filename)):
                cur_datetime = datetime.now()
                dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type
            with open(os.path.join(self._output_dir, filename), "w", encoding="utf8", newline="") as f_out:
                keys = self._all_the_columns
                dict_writer = csv.DictWriter(f_out, delimiter=",", quoting=csv.QUOTE_ALL, escapechar="\\",
                                             fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(lines)
                f_out.close()
            lines = []
            return lines
        else:
            return lines

    '''def preprocess_ERIH_plus(self, input_csv_file):
        ERIH_preprocessed = list()
        with open(input_csv_file, newline='', encoding="utf-8") as csvfile:
            next(csvfile)
            reader = csv.reader(csvfile, delimiter=';')
            for row in reader:
                venue_dict = dict()
                issn = list()
                # venue_id
                if row[1] and row[2]:
                    issn.append('issn:' + str(row[1]))
                    issn.append('issn:' + str(row[2]))
                    venue_dict["venue_id"] = issn
                elif row[1]:
                    venue_dict["venue_id"] = 'issn:' + str(row[1])
                elif row[2]:
                    venue_dict["venue_id"] = 'issn:' + str(row[2])
                # ERIH PLUS Disciplines
                if row[6]:
                    venue_dict["ERIH_disciplines"] = str(row[6])
                ERIH_preprocessed.append(venue_dict)
        return ERIH_preprocessed'''

    def split_input(self):
        # an empty list to store the filtered entities to be saved in the output files is created
        lines = []
        count = 0
        # iterate over the input data
        all_files, targz_fd = self.get_all_files(self._input_dir, self._req_type)
        for file_idx, file in enumerate(tqdm(all_files), 1):
            chunksize = self._interval
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        count += 1
                        ids_list = line.get("id").split(" ")
                        new_doi_key = ""
                        for id in ids_list:
                            # remove identifiers != doi
                            if match("^doi:10\.(\d{4,9}|[^\s/]+(\.[^\s/]+)*)/[^\s]+$", id):
                                new_doi_key += id + " "
                        line["id"] = sub("\s$", "", new_doi_key)
                        if line.get("venue"):
                            venue_ids_list = line.get("venue")
                            venue_ids_list = sub("\[", "", line.get("venue"))
                            venue_ids_list = sub("\]", "", line.get("venue"))
                            venue_ids_list = venue_ids_list.split(" ")
                            new_issn_key = ""
                            for venue_id in venue_ids_list:
                                # remove identifier != issn
                                if match("^issn:[0-9]{4}-[0-9]{3}[0-9X]$", venue_id):
                                    new_issn_key += venue_id + " "
                            line["venue"] = sub("\s$", "", new_issn_key)
                        # we are interested just in publications with DOIs, so we remove data that are not identified by a DOI
                        if line["id"] != "":
                            lines.append(line)
                        if int(count) != 0 and int(count) % int(self._interval) == 0:
                            lines = self.splitted_to_file(count, lines)
        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines)

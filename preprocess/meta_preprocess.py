import csv
from datetime import datetime
import os
from os import makedirs
from os.path import exists, basename
from tqdm import tqdm
import pandas as pd
from re import sub, match
import zipfile

from preprocess.base import Preprocessing


class MetaPreProcessing(Preprocessing):
    """This class is responsible for the preprocessing of the META dataset. It keeps all the columns of the original
    files, but the columns "id" and "venue" are modified. In particular, all the identifiers different from DOIs and ISSNs are removed.
    This passage is fundamental for merging the META_preprocessed dataset with ERIH_preprocessed.
    The class creates also a series of CSVs with just a column (id) listing all the dois of the publication saved in META. This CSVs files
    are used by COCIPreProcessing, to check the presence of the citation in META."""
    _req_type = ".csv"
    _accepted_ids = "doi"
    _accepted_ids_venue = "issn"
    _entity_columns_to_update = ["id", "venue"]
    _all_the_columns = ["id", "title", "author", "issue", "volume", "venue", "page", "pub_date", "type", "publisher", "editor"]
    _column_list_meta_dois = ["id"]

    def __init__(self, input_dir, output_dir, interval):
        self._input_dir = input_dir
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._output_meta_preprocessed = os.path.join(self._output_dir, 'meta_preprocessed')
        self._output_meta_dois = os.path.join(self._output_dir, 'list_meta_dois')
        self._interval = interval
        super(MetaPreProcessing, self).__init__()

    def splitted_to_file(self, cur_n, lines, columns_to_use, output_dir_path):
        """https://archive.softwareheritage.org/swh:1:cnt:2faf157225885e5420cdd740bee5311649c1b1a1;origin=https://pypi.org/project/oc-preprocessing/;visit=swh:1:snp:b429746305d915b577b0ed022b2650b70ecf5dc2;anchor=swh:1:rel:44fb3b0a058877ea4ef15490a499391c910a384f;path=/oc_preprocessing-0.0.5/preprocessing/base.py;lines=141"""
        if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
            filename = "filtered_" + str(cur_n // self._interval) + self._req_type
            if exists(os.path.join(output_dir_path, filename)):
                cur_datetime = datetime.now()
                dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type
            with open(os.path.join(output_dir_path, filename), "w", encoding="utf8", newline="") as f_out:
                dict_writer = csv.DictWriter(f_out, delimiter=",", quoting=csv.QUOTE_ALL, escapechar="\\",
                                             fieldnames=columns_to_use)
                dict_writer.writeheader()
                dict_writer.writerows(lines)
                f_out.close()
            lines = []
            return lines
        else:
            return lines

    def split_input(self):
        if not exists(self._output_meta_preprocessed):
            os.makedirs(self._output_meta_preprocessed)
        lines = []
        count = 0

        zip_meta_dump = zipfile.ZipFile(self._input_dir)
        list_files = zip_meta_dump.namelist()
        for cur_file in tqdm(list_files):
            if cur_file.endswith('.csv') and not basename(cur_file).startswith("."):
                f = zip_meta_dump.open(cur_file)
                chunksize = 10000
                with pd.read_csv(f, chunksize=chunksize, sep=",") as reader:
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
                                lines = self.splitted_to_file(count, lines, self._all_the_columns, self._output_meta_preprocessed)
        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines, self._all_the_columns, self._output_meta_preprocessed)

    def create_list_dois(self):
        if not exists(self._output_meta_dois):
            os.makedirs(self._output_meta_dois)
        # an empty list to store the filtered entities to be saved in the output files is created
        lines = []
        count = 0
        # iterate over the input data
        all_files, targz_fd = self.get_all_files(self._output_meta_preprocessed, self._req_type)
        for file_idx, file in enumerate(tqdm(all_files), 1):
            chunksize = self._interval
            with pd.read_csv(file, usecols=['id'], chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        ids_list = line.get("id").split(" ")
                        for id in ids_list:
                            new_line = {'id': id}
                            lines.append(new_line)
                            count += 1

                            if int(count) != 0 and int(count) % int(self._interval) == 0:
                                lines = self.splitted_to_file(count, lines, self._column_list_meta_dois, self._output_meta_dois)
        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines, self._column_list_meta_dois, self._output_meta_dois)

m = MetaPreProcessing(r"D:\OpenScience_project\csv.zip", r"D:\OpenScience_project\output_meta_24_05", 10000)
m.split_input()
m.create_list_dois()

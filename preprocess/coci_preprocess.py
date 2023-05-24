import csv
from datetime import datetime
import os
from os import makedirs
from os.path import exists
from tqdm import tqdm
import pandas as pd
import zipfile

from preprocess.base import Preprocessing
from lib.csv_manager import CSVManager


class CociPreProcessing(Preprocessing):
    """This class is responsible for preprocessing the COCI dataset. In particular, it removes from
    the original csv files the unnecessary information, that are contained in the columns "oci", "creation",
    "timespan", "journal_sc" and "author_sc", creating new csv files with just two columns "citing" and "cited".
    The user has the possibility to set an optional boolean parameter (list_dois_excluded_from_meta) that controls
     the creation of additional files containing information about the COCI citations not contained or partially contained
     in META."""
    _req_type = ".csv"
    _entity_columns_to_keep = ["citing", "cited"]
    _columns_excluded_dois = ["citing", "is_citing_in_meta", "cited", "is_cited_in_meta"]

    def __init__(self, input_dir, output_dir, interval, list_meta_dois_path):
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._list_meta_dois_path = list_meta_dois_path
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._output_coci_preprocessed = os.path.join(output_dir, 'coci_preprocessed')
        self._list_excluded_dois = os.path.join(output_dir, 'excluded_dois_from_meta')
        self._interval = interval
        super(CociPreProcessing, self).__init__()

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

    def split_input(self, list_dois_excluded_from_meta):
        if not exists(self._output_coci_preprocessed):
            os.makedirs(self._output_coci_preprocessed)
        if list_dois_excluded_from_meta:
            if not exists(self._list_excluded_dois):
                os.makedirs(self._list_excluded_dois)
        # an empty list to store the filtered entities to be saved in the output files is created
        lines_coci_pre = []
        count = 0
        if list_dois_excluded_from_meta:
            lines_dois_excluded = []
            count_dois_excluded = 0
        set_meta_id = CSVManager.load_csv_column_as_set(self._list_meta_dois_path, "id")
        # iterate over the input data
        all_files, targz_fd = self.get_all_files(self._input_dir, ".zip")
        for zipped_folder in tqdm(all_files):
            zip_f = zipfile.ZipFile(zipped_folder)
            list_csv = zip_f.namelist()
            for csv_file in tqdm(list_csv):
                    f = zip_f.open(csv_file)
                    chunksize = 10000
                    with pd.read_csv(f, usecols= self._entity_columns_to_keep, chunksize=chunksize, sep=",") as reader:
                        for chunk in reader:
                            chunk.fillna("", inplace=True)
                            df_dict_list = chunk.to_dict("records")
                            for line in df_dict_list:
                                doi_citing = line.get("citing")
                                doi_cited = line.get("cited")
                                citing_in_meta = "doi:"+doi_citing in set_meta_id
                                cited_in_meta = "doi:"+doi_cited in set_meta_id
                                citing_not_in_meta = "doi:"+doi_citing not in set_meta_id
                                cited_not_in_meta = "doi:"+doi_cited not in set_meta_id
                                if citing_in_meta and cited_in_meta:
                                    count += 1
                                    line["citing"] = "doi:"+doi_citing
                                    line["cited"] = "doi:"+doi_cited
                                    lines_coci_pre.append(line)
                                    if int(count) != 0 and int(count) % int(self._interval) == 0:
                                        lines_coci_pre = self.splitted_to_file(count, lines_coci_pre, self._entity_columns_to_keep, self._output_coci_preprocessed)
                                if list_dois_excluded_from_meta:
                                    if citing_not_in_meta or cited_not_in_meta:
                                        count_dois_excluded += 1
                                        if citing_not_in_meta and cited_not_in_meta:
                                            new_line = {'citing': "doi:"+doi_citing, 'is_citing_in_meta': False, 'cited': "doi:"+doi_cited, 'is_cited_in_meta': False}
                                            lines_dois_excluded.append(new_line)
                                        elif citing_not_in_meta:
                                            new_line = {'citing': "doi:"+doi_citing, 'is_citing_in_meta': False, 'cited': "doi:"+doi_cited, 'is_cited_in_meta': True}
                                            lines_dois_excluded.append(new_line)
                                        elif cited_not_in_meta:
                                            new_line = {'citing': "doi:"+doi_citing, 'is_citing_in_meta': True, 'cited': "doi:"+doi_cited, 'is_cited_in_meta': False}
                                            lines_dois_excluded.append(new_line)
                                        if int(count_dois_excluded) != 0 and int(count_dois_excluded) % int(self._interval) == 0:
                                            lines_dois_excluded = self.splitted_to_file(count_dois_excluded, lines_dois_excluded, self._columns_excluded_dois, self._list_excluded_dois)

        if len(lines_coci_pre) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines_coci_pre, self._entity_columns_to_keep, self._output_coci_preprocessed)

        if list_dois_excluded_from_meta:
            if len(lines_dois_excluded) > 0:
                count_dois_excluded = count_dois_excluded + (self._interval - (int(count_dois_excluded) % int(self._interval)))
                self.splitted_to_file(count_dois_excluded, lines_dois_excluded, self._columns_excluded_dois,
                                      self._list_excluded_dois)


c = CociPreProcessing(r"C:\Users\marta\Desktop\2020-08-20T18_12_28_2 (2).zip", r"C:\Users\marta\Desktop\output_prova_check_meta", 10000, r"C:\Users\marta\Desktop\meta_list_dois")
c.split_input(list_dois_excluded_from_meta=True)



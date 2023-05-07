
import os
from os.path import exists
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import csv
import json
from lib.csv_manager_coci import CSVManager
class ErihMetaWithDisciplines:
    _entity_columns_to_keep = ['id', 'erih_disciplines']

    def __init__(self, coci_preprocessed_path, erih_meta_path, output_dir, interval):
        self._coci_preprocessed_path = self.get_all_files(coci_preprocessed_path, '.csv')
        self._ERIH_META_path = self.get_all_files(erih_meta_path, '.csv')
        self._interval = interval
        self._output_dir = output_dir
        if not exists(self._output_dir):
            os.makedirs(self._output_dir)
        super(count_COCI_citations_in_SSH_journals, self).__init__()


    def get_all_files(self, i_dir_or_compr, req_type):
        result = []
        if os.path.isdir(i_dir_or_compr):
            for cur_dir, cur_subdir, cur_files in os.walk(i_dir_or_compr):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not os.path.basename(cur_file).startswith("."):
                        result.append(os.path.join(cur_dir, cur_file))
        return result

    def find_doi_COCI(self, doi):
        count = 0
        for file in tqdm(self._coci_preprocessed_path):
            with open(file, 'r', encoding="utf8") as csvfile:
                next(csvfile)
                datareader = csv.reader(csvfile)
                for row in datareader:
                    if doi in row:
                        count += 1
        return count

    def create_dictionary_citations_count(self):
        count = 0
        for file in tqdm(self._coci_preprocessed_path):
            count += 1
            citations_dict = dict()
            with open(file, 'r', encoding="utf8") as csvfile:
                next(csvfile)
                datareader = csv.reader(csvfile)
                for row in datareader:
                    if row[0] not in citations_dict:
                        citations_dict[row[0]] = 1
                    else:
                        citations_dict[row[0]] += 1
                    if row[1] not in citations_dict:
                        citations_dict[row[1]] = 1
                    else:
                        citations_dict[row[1]] += 1
            with open(os.path.join(self._output_dir, f"citations_count_{count}.json"), "w") as outfile:
                json.dump(citations_dict, outfile)



    def find_doi_COCI2(self, doi):
        count = 0
        for file in self._coci_preprocessed_path:
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        if doi == line['citing'] or doi == line['cited']:
                            count += 1
        return count

    def search_for_meta_doi_in_coci(self, path):
        count_citations = 0
        for file in tqdm(self.get_all_files(path, '.csv')):
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        doi_meta_erih = line.get('id')
                        count_citations += int(self.find_doi_COCI(doi_meta_erih))
        return count_citations

    def splitted_to_file(self, cur_n, lines, type=None):
        if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
            filename = "filtered_" + str(cur_n // self._interval) + '.csv'
            if os.path.exists(os.path.join(self._output_dir, filename)):
                cur_datetime = datetime.now()
                dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                filename = filename[:-len('.csv')] + "_" + dt_string + '.csv'
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

    def erih_meta_dois_with_disciplines(self):
        data = []
        count = 0
        for file_idx, file in enumerate(tqdm(self._ERIH_META_path), 1):
            chunksize = 10000
            with pd.read_csv(file, usecols=['id', 'erih_disciplines'],chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        discipline = line.get('erih_disciplines')
                        if discipline:
                            data.append(line)
                            count += 1
                            if int(count) != 0 and int(count) % int(self._interval) == 0:
                                data = self.splitted_to_file(count, data)
        if len(data) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, data)


    def sum_citations_count(self, path_json_dir, doi):
        count = 0
        for file in tqdm(self.get_all_files(path_json_dir, '.json')):
            f = open(file, encoding="utf-8")
            my_dict = json.load(f)
            if my_dict.get(doi):
                count += int(my_dict[doi])
        return count

    def create_dataframe(self, coci_pre_path):
        files = self.get_all_files(coci_pre_path, '.csv')
        df = pd.concat([pd.read_csv(f) for f in tqdm(files)])
        return df




































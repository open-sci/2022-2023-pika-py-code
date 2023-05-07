import os
from os.path import exists
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import csv

class ErihMetaWithDisciplines:
    _entity_columns_to_keep = ['id', 'erih_disciplines']

    def __init__(self, erih_meta_path, output_dir, interval):
        self._ERIH_META_path = self.get_all_files(erih_meta_path, '.csv')
        self._interval = interval
        self._output_dir = output_dir
        if not exists(self._output_dir):
            os.makedirs(self._output_dir)
        super(ErihMetaWithDisciplines, self).__init__()

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

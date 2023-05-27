import csv
from datetime import datetime
import os
from os import makedirs
from os.path import exists, basename
from tqdm import tqdm
import pandas as pd
from re import match



class MetaYears:
    """This class is responsible for the preprocessing of the META dataset. It keeps all the columns of the original
    files, but the columns "id" and "venue" are modified. In particular, all the identifiers different from DOIs and ISSNs are removed.
    This passage is fundamental for merging the META_preprocessed dataset with ERIH_preprocessed.
    The class creates also a series of CSVs with just a column (id) listing all the dois of the publication saved in META. This CSVs files
    are used by COCIPreProcessing, to check the presence of the citation in META."""
    _req_type = ".csv"
    _all_the_columns = ['id', 'pub_date']

    def __init__(self, input_dir, output_dir, interval):
        self._input_dir = self.get_all_files(input_dir, self._req_type)
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        super(MetaYears, self).__init__()

    def get_all_files(self, i_dir_or_compr, req_type):
        """It returns a list containing all the files found
        in the input folder and with the extension required, like ".csv"."""
        result = []
        if os.path.isdir(i_dir_or_compr):
            for cur_dir, cur_subdir, cur_files in os.walk(i_dir_or_compr):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not os.path.basename(cur_file).startswith("."):
                        result.append(os.path.join(cur_dir, cur_file))
        return result

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

        lines = []
        count = 0

        for cur_file in tqdm(self._input_dir):
            chunksize = 10000
            with pd.read_csv(cur_file, usecols=['id', 'pub_date'], chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        count += 1
                        pub_date = line.get('pub_date')
                        if pub_date:
                            if match("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", str(pub_date)) or match("^[0-9]{4}-[0-9]{2}$", str(pub_date)):
                                pub_date = str(pub_date).split('-')[0]
                            elif match("^[0-9].", str(pub_date)):
                                pub_date = str(pub_date).split('.')[0]
                        new_line = {'id': line['id'], 'pub_date': pub_date}
                        lines.append(new_line)
                        if int(count) != 0 and int(count) % int(self._interval) == 0:
                            lines = self.splitted_to_file(count, lines, self._all_the_columns, self._output_dir)
        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines, self._all_the_columns, self._output_dir)



m = MetaYears("/Volumes/CruciAlex8/2meta_preprocessed/meta_preprocessed", "/Volumes/CruciAlex8/meta_years", 10000)
m.split_input()


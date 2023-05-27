import os
import pandas as pd
from tqdm import tqdm

from csv_manager_meta_years import CSVManager
class CountYears:
    def __init__(self, meta_pre_years_path, dataset_SSH_path):
        self.CSVManager_meta = CSVManager(meta_pre_years_path)
        self._list_SSH = self.get_all_files(dataset_SSH_path, '.csv')


    def get_all_files(sef, i_dir_or_compr, req_type):
        """It returns a list containing all the files found
        in the input folder and with the extension required, like ".csv"."""
        result = []
        if os.path.isdir(i_dir_or_compr):
            for cur_dir, cur_subdir, cur_files in os.walk(i_dir_or_compr):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not os.path.basename(cur_file).startswith("."):
                        result.append(os.path.join(cur_dir, cur_file))
        return result


    def iterate_SSH(self):
        citing_years = dict()
        cited_years = dict()
        for file in tqdm(self._list_SSH, '.csv'):
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        if line.get('is_citing_SSH'):
                            citing_SSH = line.get('citing')
                            year1 = self.CSVManager_meta.get_value(citing_SSH)
                            year1_list = list(year1)
                            if year1_list[0] and year1_list[0] not in citing_years:
                                citing_years[year1_list[0]] = 1
                            else:
                                citing_years[year1_list[0]] += 1
                        if line.get('is_cited_SSH'):
                            cited_SSH = line.get('cited')
                            year2 = self.CSVManager_meta.get_value(cited_SSH)
                            year2_list = list(year2)
                            if year2_list[0] and year2_list[0] not in cited_years:
                                cited_years[year2_list[0]] = 1
                            else:
                                cited_years[year2_list[0]] += 1
        return citing_years, cited_years


c = CountYears("/Volumes/CruciAlex8/meta_years", "/Volumes/CruciAlex8/answers_to_questions/dataset_SSH")
print(c.iterate_SSH())

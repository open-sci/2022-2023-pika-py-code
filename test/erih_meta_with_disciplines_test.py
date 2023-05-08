import shutil
import unittest
import os
from erih_meta_with_disciplines import ErihMetaWithDisciplines
import pandas as pd

BASE = os.path.join('test', 'erih_meta_disciplines_data')
ERIH_META = os.path.join(BASE, 'erih_meta')
OUTPUT = os.path.join(BASE, 'output')


class TestErihMetaWithDisciplines(unittest.TestCase):

    def test_erih_meta_dois_with_disciplines(self):
        self.maxDiff = None
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)

        e = ErihMetaWithDisciplines(ERIH_META, OUTPUT, 100)
        output = e.erih_meta_dois_with_disciplines()
        output_csv_folder = e.get_all_files(OUTPUT, '.csv')
        data = list()
        for file in output_csv_folder:
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        data.append(line)
        expected_out = [{'id': 'doi:10.1007/3540608052_78', 'erih_meta_with_disciplines': 'Psychology'}]
        self.assertEqual(data, expected_out)

# python -m unittest discover -s test -p "erih_meta_with_disciplines_test.py"



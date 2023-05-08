import shutil
import unittest
import os
from question_1 import CountCitations
import pandas as pd

BASE = os.path.join('test', 'question_1_data')
ERIH_META_DISCIPLINES = os.path.join(BASE, 'erih_meta_with_disciplines')
COCI_PRE = os.path.join(BASE, 'coci_pre_piccolo')
OUTPUT = os.path.join(BASE, 'output')
COUNT_OUTPUT = os.path.join(BASE, 'count_output')

class Question1Test(unittest.TestCase):

    def test_create_citations_map(self):
        # citing column : "doi:10.1002/1097-0142(20010815)92:4<796::aid-cncr1385>3.0.co;2-3"
        # cited column : "doi:10.1016/j.ssc.2014.03.020"
        # citing and cited in the same column: "doi:10.1002/1097-0142(20010815)92:4<814::aid-cncr1387>3.0.co;2-8", "id": "doi:10.3322/canjclin.48.1.6"
        self.maxDiff = None
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)

        c = CountCitations(COCI_PRE, ERIH_META_DISCIPLINES, OUTPUT, 200)
        c.create_citations_map()
        data = list()
        for file in c.get_all_files(OUTPUT, '.csv'):
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize) as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        data.append(line)
        expected_out = [{"id": "doi:10.1002/1097-0142(20010815)92:4<796::aid-cncr1385>3.0.co;2-3"}, {"id": "doi:10.1002/1097-0142(20010815)92:4<796::aid-cncr1385>3.0.co;2-3"}, {"id": "doi:10.1002/1097-0142(20010815)92:4<814::aid-cncr1387>3.0.co;2-8"}, {"id": "doi:10.3322/canjclin.48.1.6"}, {"id": "doi:10.1016/j.ssc.2014.03.020"}]
        self.assertEqual(data, expected_out)

    def test_count_lines(self):
        c = CountCitations(COCI_PRE, ERIH_META_DISCIPLINES, OUTPUT, 200)
        output = c.count_citations(COUNT_OUTPUT)
        expected_out = 9
        self.assertEqual(output, expected_out)





#python -m unittest discover -s test -p "question_1_test.py"

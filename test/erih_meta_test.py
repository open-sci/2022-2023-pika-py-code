import shutil
import unittest
from erih_meta import ErihMeta
import os
import pandas as pd

BASE = os.path.join('test', 'erih_meta_data')
META_PRE = os.path.join(BASE, 'meta_preprocessed')
ERIH_PRE = os.path.join(BASE, 'erih_pre')
OUTPUT = os.path.join(BASE, 'output')

class TestJalcProcess(unittest.TestCase):

    def test_find_erih_venue(self):
        e = ErihMeta(META_PRE, ERIH_PRE, OUTPUT, 20)
        # in the cell erih['venue_id'] there are two issn and both are in erih_preprocessed but in different cells
        issn1 = ['issn:1984-4182', 'issn:2362-6097']
        output1 = e.find_erih_venue(issn1)
        expected_out1 = 'Art and Art History, Human Geography and Urban Studies, Interdisciplinary research in the Social Sciences, Law'
        # in the cell erih['venue_id'] there is a single issn that is in erih_preprocessed
        issn2 = ['issn:2065-1430']
        output2 = e.find_erih_venue(issn2)
        expected_out2 = 'Pedagogical & Educational Research'
        #the issn are not in erih
        issn3 = ['issn:0000-0000', 'issn:0000-000X']
        output3 = e.find_erih_venue(issn3)
        expected_out3 = ""
        #the issn are both in erih in the same cell
        issn4 = ['issn:2362-6089', 'issn:2362-6097']
        output4 = e.find_erih_venue(issn4)
        expected_out4 = 'Art and Art History, Human Geography and Urban Studies, Interdisciplinary research in the Social Sciences'
        #just one issn is in erih
        issn5 = ['issn:1984-2090', 'issn:0374-6577', 'issn:1522-239X']
        output5 = e.find_erih_venue(issn5)
        expected_out5 = 'Literature'
        #two issn are in erih in two different cells
        issn6 = ['issn:1984-2090', 'issn:2065-1430', 'issn:1522-239X']
        output6 = e.find_erih_venue(issn6)
        expected_out6 = 'Literature, Pedagogical & Educational Research'
        self.assertEqual(output1, expected_out1)
        self.assertEqual(output2, expected_out2)
        self.assertEqual(output3, expected_out3)
        self.assertEqual(output4, expected_out4)
        self.assertEqual(output5, expected_out5)
        self.assertEqual(output6, expected_out6)

    def test_erih_meta(self):
        self.maxDiff = None
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)
        e = ErihMeta(META_PRE, ERIH_PRE, OUTPUT, 20)
        output = e.erih_meta()
        output_csv_folder = e.get_all_files(OUTPUT, '.csv')
        for file in output_csv_folder:
            erih_meta = pd.read_csv(file, sep=",")
            erih_meta.fillna('', inplace=True)
            erih_meta_disciplines = list(erih_meta['erih_disciplines'])
            expected_out = ['Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            '',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            '',
                            'Literature',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            '',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences',
                            'Gender Studies, History, Cultural Studies, Media Studies and Communication, Film and Theatre Studies, Sociology, Literature, Interdisciplinary research in the Humanities, Interdisciplinary research in the Social Sciences']
            self.assertEqual(erih_meta_disciplines, expected_out)


#python -m unittest discover -s test -p "erih_meta_test.py"
import unittest
from erih_meta import ErihMeta
import os

BASE = os.path.join('test', 'erih_meta_data')
ERIH_PRE = os.path.join(BASE, 'erih_preprocessed.csv')
META_PRE = os.path.join(BASE, 'meta_preprocessed')
OUTPUT = os.path.join(BASE, 'output')

class TestJalcProcess(unittest.TestCase):

    def test_find_erih_venue(self):
        e = ErihMeta(META_PRE, ERIH_PRE, OUTPUT, 20)
        # in the cell erih['venue_id'] there are two issn and just one is passed as input of the method
        issn1 = 'issn:2362-6097'
        output1 = e.find_erih_venue(issn1)
        expected_out1 = 'Art and Art History, Human Geography and Urban Studies, Interdisciplinary research in the Social Sciences'
        # in the cell erih['venue_id'] there is a single issn
        issn2 = 'issn:2065-1430'
        output2 = e.find_erih_venue(issn2)
        expected_out2 = 'Pedagogical & Educational Research'
        self.assertEqual(output1, expected_out1)
        self.assertEqual(output2, expected_out2)
        #the issn is not in erih
        issn3 = 'issn:0000-0000'
        output3 = e.find_erih_venue(issn3)
        expected_out3 = ""
        self.assertEqual(output3, expected_out3)

#python -m unittest discover -s test -p "erih_meta_test.py"
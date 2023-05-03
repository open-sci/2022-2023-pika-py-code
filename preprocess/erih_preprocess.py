import csv
import os
from os import makedirs
from os.path import exists
class ErihPreProcessing():
    def __init__(self, input_file_path, output_dir):
        self._input_file_path = input_file_path
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        super(ErihPreProcessing, self).__init__()

    def preprocess_ERIH_plus(self):
        ERIH_preprocessed = list()
        with open(self._input_file_path, newline='', encoding="utf-8") as csvfile:
            next(csvfile)
            reader = csv.reader(csvfile, delimiter=';')
            for row in reader:
                venue_dict = dict()
                venue_dict2 = dict()
                # venue_id
                if row[1] and row[2]:
                    issn1 = 'issn:' + str(row[1])
                    issn2 = 'issn:' + str(row[2])
                    venue_dict["venue_id"] = str(issn1)
                    venue_dict2["venue_id"] = str(issn2)
                elif row[1]:
                    venue_dict["venue_id"] = 'issn:' + str(row[1])
                elif row[2]:
                    venue_dict["venue_id"] = 'issn:' + str(row[2])
                # ERIH PLUS Disciplines
                if row[6]:
                    venue_dict["ERIH_disciplines"] = str(row[6])
                    venue_dict2["ERIH_disciplines"] = str(row[6])
                ERIH_preprocessed.append(venue_dict)
                if venue_dict2.get("venue_id"):
                    ERIH_preprocessed.append(venue_dict2)
        return ERIH_preprocessed

    def write_csv(self):
        filename = "erih_preprocessed.csv"
        with open(os.path.join(self._output_dir, filename), "w", encoding="utf8", newline="") as csv_output:
            writer = csv.writer(csv_output, delimiter=';')
            writer.writerow(['venue_id', 'ERIH_disciplines'])
            for dictionary in self.preprocess_ERIH_plus():
                writer.writerow(dictionary.values())
        csv_output.close()

from csv import DictReader, writer
from io import StringIO
from os import mkdir, sep, walk
from os.path import exists, isdir, join
from typing import Dict


class CSVManager(object):
    """https://archive.softwareheritage.org/swh:1:cnt:8362d20804ab87cf6862ffe37f2624e26634eff2;origin=https://github.com/opencitations/oc_meta;visit=swh:1:snp:dcbd1d5baf07b98504292230c53215f1e080e782;anchor=swh:1:rev:8510cdd12e3693202231fef27b0311ca7b88414d;path=/oc_meta/lib/csvmanager.py;lines=24"""
    """
    This class is able to load a simple CSV composed by two fields, 'venue_id' and
    'ERIH_disciplines', and then to index all its items in a structured form so as to be
    easily queried. In addition, it allows one to store new information in the CSV,
    if needed.
    """

    def __init__(self, output_path: str = None, line_threshold=10000, low_memory: bool = False):
        self.output_path = output_path
        self.data: Dict[str, set] = {}
        self.data_to_store = list()
        if output_path is not None:
            self.existing_files = self.__get_existing_files()
            if low_memory:
                self.__load_all_csv_files(self.existing_files, fun=self.__low_memory_load,
                                          line_threshold=line_threshold)
            else:
                self.__load_csv()

    def __get_existing_files(self) -> list:
        files_to_process = []
        if exists(self.output_path):
            for cur_dir, _, cur_files in walk(self.output_path):
                for cur_file in cur_files:
                    if cur_file.endswith('.csv'):
                        files_to_process.append(cur_dir + sep + cur_file)
        else:
            mkdir(self.output_path)
        return files_to_process

    @staticmethod
    def load_csv_column_as_set(file_or_dir_path: str, key: str, line_threshold: int = 10000):
        result = set()
        if exists(file_or_dir_path):
            file_to_process = []
            if isdir(file_or_dir_path):
                for cur_dir, _, cur_files in walk(file_or_dir_path):
                    for cur_file in cur_files:
                        if cur_file.endswith('.csv'):
                            file_to_process.append(cur_dir + sep + cur_file)
            else:
                file_to_process.append(file_or_dir_path)
            for item in CSVManager.__load_all_csv_files(file_to_process, CSVManager.__load_csv_by_key,
                                                        line_threshold=line_threshold, key=key):
                result.update(item)
        return result

    @staticmethod
    def __load_csv_by_key(csv_string, key):
        result = set()
        csv_metadata = DictReader(StringIO(csv_string), delimiter=',')
        for row in csv_metadata:
            result.add(row[key])
        return result

    @staticmethod
    def __load_all_csv_files(file_to_process, fun, line_threshold, **params):
        result = []
        header = None
        for csv_path in file_to_process:
            with open(csv_path, encoding='utf-8') as f:
                csv_content = ''
                for idx, line in enumerate(f.readlines()):
                    if header is None:
                        header = line
                        csv_content = header
                    else:
                        if idx % line_threshold == 0:
                            result.append(fun(csv_content, **params))
                            csv_content = header
                        csv_content += line
            result.append(fun(csv_content, **params))
        return result

    def dump_data(self, file_name: str) -> None:
        path = join(self.output_path, file_name)
        if not exists(path):
            with open(path, 'w', encoding='utf-8', newline='') as f:
                f.write('"venue_id","ERIH_disciplines"\n')
        with open(path, 'a', encoding='utf-8', newline='') as f:
            csv_writer = writer(f, delimiter=';')
            for el in self.data_to_store:
                csv_writer.writerow([el[0].replace('"', '""'), el[1].replace('"', '""')])
        self.data_to_store = list()

    def get_value(self, id_string):
        '''
        It returns the set of values associated to the input 'id_string',
        or None if 'id_string' is not included in the CSV.
        '''
        if id_string in self.data:
            return set(self.data[id_string])

    def add_value(self, id_string, value):
        '''
        It adds the value specified in the set of values associated to 'id_string'.
        If the object was created with the option of storing also the data in a CSV
        ('store_new' = True, default behaviour), then it also add new data in the CSV.
        '''
        self.data.setdefault(id_string, set())
        if value not in self.data[id_string]:
            self.data[id_string].add(value)
            self.data_to_store.append([id_string, value])

    def __load_csv(self):
        for file in self.existing_files:
            with open(file, 'r', encoding='utf-8') as f:
                reader = DictReader(f, delimiter=";")
                for row in reader:
                    self.data.setdefault(row['venue_id'], set())
                    self.data[row['venue_id']].add(row['ERIH_disciplines'])

    def __low_memory_load(self, csv_string: str):
        csv_metadata = DictReader(StringIO(csv_string), delimiter=';')
        for row in csv_metadata:
            cur_id = row['venue_id']
            if cur_id not in self.data:
                self.data[cur_id] = set()
            self.data[cur_id].add(row['ERIH_disciplines'])



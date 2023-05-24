import os
from os.path import exists
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import csv
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from lib.csv_manager_erih_meta_disciplines import CSVManager


class Counter(object):
    """
    This class is responsible for answering to the following research questions by following
    two different approaches, one more concise and another one that produces files that may result
    useful also for additional researches.

    1. How many citations (according to COCI) involve, either as citing or cited entities, publications in SSH journals
    (according to ERIH-PLUS) included in OpenCitations META?
    2. What are the disciplines that cites the most and those cited the most?
    3. How many citations start from and go to publications in OpenCitations META that are not included in SSH journals?

    The majority of operations are I/O bound, therefore the load is distributed among the available threads.
    ...

    Init Attributes
    ----------
    These must be added when the class is instantiated.

    - coci_preprocessed_path : str
        path of the folder containing the preprocessed COCI files
    - erih_meta_path : str
        path of the folder containing the ERIH-PLUS files
    - num_cpu : int
        number of cpu available for the execution of the program,
        by default it is set as the entire number of cpu available
        in the machine. This is also usefult to define the number of
        threads to use for the execution of the program, which is
        defined as num_cpu * 10.

    Callable Methods
    -------
    Please, do not call other methods than the ones listed below.

    - get_all_files(i_dir_or_compr, req_type):
        It returns a list containing all the files found
        in the input folder and with the extension required, like ".csv".
    - execute_count(output_dir='OutputFiles/', create_subfiles=False, answer_to_q1=True, answer_to_q2=True, answer_to_q3=True, interval=10000):
        It executes the count of the citations according to the research questions.
    
    """

    _entity_columns_to_use_erih_meta_disciplines = ['id', 'erih_disciplines']
    _entity_columns_to_use_erih_meta_without_disciplines = ['id']
    _entity_columns_to_use_q1_q3 = ['citing', 'is_citing_SSH', 'cited', 'is_cited_SSH']
    _entity_columns_to_use_q2 = ['id', 'citing', 'cited', 'disciplines']

    def __init__(self, coci_preprocessed_path, erih_meta_path, num_cpus=None):
        self._list_coci_files = self.get_all_files(coci_preprocessed_path, '.csv')
        self._list_erih_meta_files = self.get_all_files(erih_meta_path, '.csv')
        self.num_cpu = num_cpus if num_cpus!=None else multiprocessing.cpu_count()
        self.num_threads = self.num_cpu * 4 # 4 threads per cpu

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
        """This method takes in input an integer number which represent the number of
        lines already processed (cur_n), a list of processed lines (lines),
        a list containing the names of the columns to use in the new csv files (columns_to_use)
        and the path of the output directory where the new files will be stored (output_dir_path).
        It performs two tasks: it checks if the current number of processed lines can be divided by the target
        number of lines to store in each file, defined as an input parameter: if it is not the case,
        it returns the list of lines in input (lines); otherwise, it stores the lines in the list data to
        a csv output file and it returns an empty list."""

        if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
            filename = "count_" + str(cur_n // self._interval) + '.csv'
            if os.path.exists(os.path.join(output_dir_path, filename)):
                cur_datetime = datetime.now()
                dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                filename = filename[:-len('.csv')] + "_" + dt_string + '.csv'
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

    def create_additional_files(self, with_disciplines):
        """This method is responsible for creating two different subsets starting from erih_meta: erih_meta_with_disciplines and erih_meta_without_disciplines.
        It iterates over erih_meta, in particular over the columns "id" and "erih_disciplines", and if the last one is filled, the two values, "id" and "erih_disciplines",
        are added as values of the corresponding columns in the file of the subset erih_meta_with_disciplines. On the contrary, if the column "erih_disciplines" is empty, the corresponding
        doi is added to the file of the subset "erih_meta_without_disciplines".
        The creation of the two subsets is controlled by the input parameter "with_disciplines", if it is set to True the method
        creates "erih_meta_with_disciplines", otherwise the other subset.
        """
        process_output_dir = self._path_erih_meta_with_disciplines if with_disciplines else self._path_erih_meta_without_disciplines
        entity_columns_to_use = self._entity_columns_to_use_erih_meta_disciplines if with_disciplines else self._entity_columns_to_use_erih_meta_without_disciplines
        
        if not exists(process_output_dir):
            os.makedirs(process_output_dir)

        data = []
        count = 0

        if with_disciplines:
            for _, file in enumerate(tqdm(self._list_erih_meta_files), 1):
                chunksize = 10000
                with pd.read_csv(file, usecols=['id', 'erih_disciplines'], chunksize=chunksize, sep=",") as reader:
                    for chunk in reader:
                        chunk.fillna("", inplace=True)
                        df_dict_list = chunk.to_dict("records")
                        for line in df_dict_list:
                            discipline = line.get('erih_disciplines')
                            if discipline:
                                line_id = line.get('id')
                                list_id = line_id.split(" ")
                                if len(list_id) > 1:
                                    for id in list_id:
                                        new_line_many_ids = {'id': id, 'erih_disciplines': discipline}
                                        data.append(new_line_many_ids)
                                        count += 1
                                        if int(count) != 0 and int(count) % int(self._interval) == 0:
                                            data = self.splitted_to_file(count, data, entity_columns_to_use, process_output_dir)
                                else:
                                    data.append(line)
                                    count += 1
                                    if int(count) != 0 and int(count) % int(self._interval) == 0:
                                        data = self.splitted_to_file(count, data, entity_columns_to_use, process_output_dir)
        else:
            for _, file in enumerate(tqdm(self._list_erih_meta_files), 1):
                chunksize = 10000
                with pd.read_csv(file, usecols=['id', 'erih_disciplines'], chunksize=chunksize, sep=",") as reader:
                    for chunk in reader:
                        chunk.fillna("", inplace=True)
                        df_dict_list = chunk.to_dict("records")
                        for line in df_dict_list:
                            discipline = line.get('erih_disciplines')
                            if not discipline:
                                line_id = line.get('id')
                                list_id = line_id.split(" ")
                                # check if in the column 'id' of erih_meta there are more than one id
                                if len(list_id) > 1:
                                    for id in list_id:
                                        new_line_many_ids = {'id': id}
                                        data.append(new_line_many_ids)
                                        count += 1
                                        if int(count) != 0 and int(count) % int(self._interval) == 0:
                                            data = self.splitted_to_file(count, data, entity_columns_to_use,
                                                                         process_output_dir)
                                else:
                                    new_line = {'id': line_id}
                                    data.append(new_line)
                                    count += 1
                                    if int(count) != 0 and int(count) % int(self._interval) == 0:
                                        data = self.splitted_to_file(count, data, entity_columns_to_use, process_output_dir)
            
        if len(data) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, data, entity_columns_to_use, process_output_dir)    

    def create_disciplines_map(self, ):
        """This method iterates over COCI_preprocessed and uses the class CSVManager for searching in "erih_meta_with_disciplines"
        the dois found in COCI and the disciplines associated to them. It creates new files with four columns: "id", "citing", "cited" and "disciplines".
        The two columns "citing" and "cited" contain boolean values, according to the role that the doi has in the citation according to COCI.
        These files are then used in the method "create_count_dictionaries".
        """

        self._CSVManager_erih_meta_with_disciplines = CSVManager(self._path_erih_meta_with_disciplines)

        if not exists(self._path_dataset_map_disciplines):
            os.makedirs(self._path_dataset_map_disciplines)

        data = []
        count = 0
        for file_idx, file in enumerate(tqdm(self._list_coci_files), 1):
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        citing = line.get('citing')
                        cited = line.get('cited')
                        if self._CSVManager_erih_meta_with_disciplines.get_value(citing) or self._CSVManager_erih_meta_with_disciplines.get_value(cited):
                            if self._CSVManager_erih_meta_with_disciplines.get_value(citing):
                                set_discipline1 = self._CSVManager_erih_meta_with_disciplines.get_value(citing)
                                str_disciplines1 = ', '.join(set_discipline1)
                                list_disciplines1 = str_disciplines1.split(', ')
                                for discipline in list_disciplines1:
                                    entity_dict1 = dict()
                                    entity_dict1['id'] = citing
                                    entity_dict1['citing'] = True
                                    entity_dict1['cited'] = False
                                    entity_dict1['disciplines'] = discipline
                                    data.append(entity_dict1)
                                    count += 1
                                    if int(count) != 0 and int(count) % int(self._interval) == 0:
                                        data = self.splitted_to_file(count, data, self._entity_columns_to_use_q2, self._path_dataset_map_disciplines)
                            if self._CSVManager_erih_meta_with_disciplines.get_value(cited):
                                set_discipline2 = self._CSVManager_erih_meta_with_disciplines.get_value(cited)
                                str_disciplines2 = ', '.join(set_discipline2)
                                list_disciplines2 = str_disciplines2.split(', ')
                                for discipline in list_disciplines2:
                                    entity_dict2 = dict()
                                    entity_dict2['id'] = cited
                                    entity_dict2['citing'] = False
                                    entity_dict2['cited'] = True
                                    entity_dict2['disciplines'] = discipline
                                    data.append(entity_dict2)
                                    count += 1
                                    if int(count) != 0 and int(count) % int(self._interval) == 0:
                                        data = self.splitted_to_file(count, data, self._entity_columns_to_use_q2, self._path_dataset_map_disciplines)
        if len(data) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, data, self._entity_columns_to_use_q2, self._path_dataset_map_disciplines)

    def create_datasets_for_count(self, is_SSH=True):
        """This method creates two datasets, dataset_SSH and dataset_no_SSH, that are then used for answering to
        the first and third research questions. They are composed by four columns: "citing", "is_citing_SSH", "cited", "is_cited_SSH".
        The "is_citing_SSH" and "is_cited_SSH" contain boolean values: "True" if the considered entity is associated to a SSH discipline, "False" otherwise.
        The method builds the two datasets starting from COCI_preprocessed and using the class CSVManager for managing the
        subsets of erih_meta, erih_meta_with_disciplines and erih_meta_without_disciplines.
        """
        output_process_dir = self._path_dataset_SSH if is_SSH else self._path_dataset_no_SSH
        
        if not exists(output_process_dir):
            os.makedirs(output_process_dir)

        if is_SSH:
            self._CSVManager_erih_meta_with_disciplines = CSVManager(self._path_erih_meta_with_disciplines)
        else:
            self._set_erih_meta_without_disciplines = CSVManager.load_csv_column_as_set(self._path_erih_meta_without_disciplines, 'id')

        data = []
        count = 0
        for _, file in enumerate(tqdm(self._list_coci_files), 1):
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        citing = line.get('citing')
                        cited = line.get('cited')

                        if is_SSH:
                            condition = self._CSVManager_erih_meta_with_disciplines.get_value(citing) or self._CSVManager_erih_meta_with_disciplines.get_value(cited)
                        else:
                            condition = citing in self._set_erih_meta_without_disciplines and cited in self._set_erih_meta_without_disciplines

                        if condition:
                            count += 1
                            if is_SSH:
                                entity_condition1 = self._CSVManager_erih_meta_with_disciplines.get_value(citing)
                                entity_condition2 = self._CSVManager_erih_meta_with_disciplines.get_value(cited)
                            else:
                                entity_condition1 = citing in self._set_erih_meta_without_disciplines
                                entity_condition2 = cited in self._set_erih_meta_without_disciplines

                            if is_SSH:
                                if entity_condition1 and entity_condition2:
                                    data.append({'citing': citing, 'is_citing_SSH': True, 'cited': cited, 'is_cited_SSH': True})
                                elif entity_condition1:
                                    entity_dict1 = {'citing': citing, 'is_citing_SSH': True, 'cited': cited, 'is_cited_SSH': False}
                                    data.append(entity_dict1)
                                elif entity_condition2:
                                    entity_dict2 = {'citing': citing, 'is_citing_SSH': False, 'cited': cited, 'is_cited_SSH': True}
                                    data.append(entity_dict2)
                            else:
                                if entity_condition1 and entity_condition2:
                                    data.append({'citing': citing, 'is_citing_SSH': False, 'cited': cited, 'is_cited_SSH': False})
                                elif entity_condition1:
                                    entity_dict1 = {'citing': citing, 'is_citing_SSH': False, 'cited': cited, 'is_cited_SSH': True}
                                    data.append(entity_dict1)
                                elif entity_condition2:
                                    entity_dict2 = {'citing': citing, 'is_citing_SSH': True, 'cited': cited, 'is_cited_SSH': False}
                                    data.append(entity_dict2)

                            if int(count) != 0 and int(count) % int(self._interval) == 0:
                                data = self.splitted_to_file(count, data, self._entity_columns_to_use_q1_q3, output_process_dir)

        if len(data) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, data, self._entity_columns_to_use_q1_q3, output_process_dir)

    def create_count_dictionaries(self):
        """This method creates two dictionaries, dict_citing and dict_cited. In each dictionary the keys are
        the SSH disciplines and the values correspond to the total count of the discipline associated to a citing or a cited entity"""
        dict_citing = dict()
        dict_cited = dict()
        for file in tqdm(self.get_all_files(self._path_dataset_map_disciplines, '.csv')):
            chunksize = 10000
            with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    for line in df_dict_list:
                        if line['citing']:
                            citing_discipline = line.get('disciplines')
                            if citing_discipline not in dict_citing:
                                dict_citing[citing_discipline] = 1
                            else:
                                dict_citing[citing_discipline] += 1
                        if line['cited']:
                            cited_discipline = line.get('disciplines')
                            if cited_discipline not in dict_cited:
                                dict_cited[cited_discipline] = 1
                            else:
                                dict_cited[cited_discipline] += 1
        max_value_citing = max(dict_citing.values())
        max_value_cited = max(dict_cited.values())
        max_discipline_cited = max(dict_cited, key=dict_cited.get)
        max_discipline_citing = max(dict_citing, key=dict_citing.get)
        return max_value_citing, max_value_cited, max_discipline_cited, max_discipline_citing

    def count_lines(self, path):
        """This method simply counts and sums the lines of csv files contained in the folder, the path of which is passed as input."""
        citations_count = 0
        for file in tqdm(self.get_all_files(path, '.csv')):
            results = pd.read_csv(file, sep=",")
            citations_count += len(results)
        return citations_count

    def iterate_erih_meta(self):
        ssh_papers = list()
        not_ssh_papers = list()
        id_disciplines_map = dict()
        ssh_disciplines = set()

        for filename in tqdm(self._list_erih_meta_files, total=len(self._list_erih_meta_files), desc='Building lists of DOIs over ERIH-PLUS and META...', colour='red', smoothing=0.1):
            df = pd.read_csv(filename) # it was -> os.path.join(erih_meta_dir_path, filename))
            df = df[['id', 'erih_disciplines']] # Attention to the name given to the erih_disciplines column, if erih or ERIH
            # fill all the possible NaN or None with ""
            df = df.fillna('')
            # create boolean mask for erih_disciplines column
            mask = df['erih_disciplines'] != ''
            # filter the dataframe with the above mask
            ssh_df = df[mask]
            ssh_df = ssh_df.reset_index(drop=True)

            for _, row in ssh_df.iterrows():
                disciplines = row['erih_disciplines'].split(',')
                disciplines = [discipline.strip() for discipline in disciplines]
                doi = row['id']
                if doi not in id_disciplines_map:
                    id_disciplines_map[doi] = disciplines
                else:
                    id_disciplines_map[doi].extend(disciplines)
                for discipline in disciplines:
                    if discipline not in ssh_disciplines:
                        ssh_disciplines.add(discipline)
                    
            # Create a second dataframe from the above mask, where are kept only the False rows in the mask
            not_ssh_df = df[~mask]
            not_ssh_df = not_ssh_df.reset_index(drop=True)
            # Get the unique values of the id column
            unique_ssh = ssh_df['id'].unique().tolist()
            unique_not_ssh = not_ssh_df['id'].unique().tolist()
            # Append the unique values to the list
            ssh_papers.extend(unique_ssh)
            not_ssh_papers.extend(unique_not_ssh)

        print('Decoupling DOIs...')
        ssh_papers_unique = []
        for paper in ssh_papers:
            papers = paper.split(' ')
            ssh_papers_unique.extend(papers)

        not_ssh_papers_unique = []
        for paper in not_ssh_papers:
            papers = paper.split(' ')
            not_ssh_papers_unique.extend(papers)

        unique_id_disciplines_map = dict()
        for key, value in id_disciplines_map.items():
            multiple_keys = key.split(' ')
            for k in multiple_keys:
                unique_id_disciplines_map[k] = value

        print('Creating sets for unique DOIs...')
        ssh_set = set(ssh_papers_unique)
        not_ssh_set = set(not_ssh_papers_unique)

        return ssh_set, not_ssh_set, unique_id_disciplines_map, ssh_disciplines

    def execute_count(self, output_dir='OutputFiles/', create_subfiles=False, answer_to_q1=True, answer_to_q2=True, answer_to_q3=True, interval=10000):
        """
        This is the main method of the class, the one that orchestrates the entire process. It is infact the method that
        is called by the final user to answer all the research questions.
        The method takes as input six parameters. The first is the path of the
        output folder where all the produced files will be stored (output_dir).
        The method allows two approaches that can be controlled by the second parameter (create_subfiles): if it is set to "True"
        a series of files, that are produced by the methods "create_additional_files" (responsible for creating "erih_meta_with_disciplines"
        and "erih_meta_without_disciplines") and "create_datasets_for_count" (it creates "dataset_SSH" and "dataset_no_SSH"), will be saved in
        subfolders inside the output folder specified by the user; if it is set to "False" the answers will be provided without producing
        any file. Then there are three boolean parameters (answer_to_q1, answer_to_q2, answer_to_q3) for deciding to which questions answer.
        The last parameter (interval) has the function to control the number of lines that will compose each file produced.
        """

        if create_subfiles:
            self._interval = interval
            self._output_dir = output_dir
            if not exists(self._output_dir):
                os.makedirs(self._output_dir)
            self._path_erih_meta_with_disciplines = os.path.join(output_dir, 'erih_meta_with_disciplines')
            self._path_dataset_SSH = os.path.join(output_dir, 'dataset_SSH')
            self._path_erih_meta_without_disciplines = os.path.join(output_dir, 'erih_meta_without_disciplines')
            self._path_dataset_no_SSH = os.path.join(output_dir, 'dataset_no_SSH')
            self._path_dataset_map_disciplines = os.path.join(output_dir, 'dataset_map_disciplines')

        if create_subfiles:
            if answer_to_q1:
                # Answer to question 1
                self.create_additional_files(with_disciplines=True)
                self.create_datasets_for_count(is_SSH=True)
                ssh_citations = self.count_lines(self._path_dataset_SSH)
                print('Number of citations that (according to COCI) involve, either as citing or cited entities, publications in SSH journals (according to ERIH-PLUS) included in OpenCitations Meta: %d' %ssh_citations)

            if answer_to_q2:
                # Answer to question 2
                if not exists(self._path_erih_meta_with_disciplines):
                    self.create_additional_files(with_disciplines=True)
                self.create_disciplines_map()
                count_disciplines = self.create_count_dictionaries()
                print(f"The most citing discipline is {count_disciplines[3]}: {count_disciplines[0]}", f"The most cited discipline is {count_disciplines[2]}: {count_disciplines[1]}")

            if answer_to_q3:
                # Answer to question 3
                self.create_additional_files(with_disciplines=False)
                self.create_datasets_for_count(is_SSH=False)
                not_ssh_citations = self.count_lines(self._path_dataset_no_SSH)
                print('Number of citations that (according to COCI) start from and go to publications in OpenCitations Meta that are not included in SSH journals: %d' %not_ssh_citations)

        else:
            print('\nSarting the process, be patient, it will take a while...\n')
            
            ssh_set, not_ssh_set, id_disciplines_map, ssh_disciplines = self.iterate_erih_meta()

            ssh_citations = 0
            not_ssh_citations = 0

            def count_citations(ssh_set, not_ssh_set, row):
                if row['citing'] in ssh_set or row['cited'] in ssh_set:
                    return 'ssh'
                elif row['citing'] in not_ssh_set and row['cited'] in not_ssh_set:
                    return 'not_ssh'
                else:
                    return 'other'

            def count_citations_in_file(id_disciplines_map, ssh_disciplines, ssh_set, not_ssh_set, filepath):
                df = pd.read_csv(filepath, usecols=['citing', 'cited'])
                # These below are just placeholders in case someone do not need the 3 answers but just some of them
                citation_counts = {'ssh': 0, 'not_ssh': 0}
                discipline_counter = dict()
                for discipline in ssh_disciplines:
                    discipline_counter[discipline] = {'citing': 0, 'cited': 0}
                citing_disciplines = df['citing'].tolist()
                cited_disciplines = df['cited'].tolist()

                for i in range(len(citing_disciplines)):
                    if answer_to_q1 or answer_to_q3:
                        if citing_disciplines[i] in ssh_set or cited_disciplines[i] in ssh_set:
                            citation_counts['ssh'] += 1
                        elif citing_disciplines[i] in not_ssh_set and cited_disciplines[i] in not_ssh_set:
                            citation_counts['not_ssh'] += 1
                    if answer_to_q2:
                        if citing_disciplines[i] in ssh_set:
                            for discipline in id_disciplines_map[citing_disciplines[i]]:
                                discipline_counter[discipline]['citing'] += 1
                        if cited_disciplines[i] in ssh_set:
                            for discipline in id_disciplines_map[cited_disciplines[i]]:
                                discipline_counter[discipline]['cited'] += 1

                return discipline_counter, citation_counts.get('ssh', 0), citation_counts.get('not_ssh', 0)
            

            print('Starting to count...\n')
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                count_citations_partial = partial(count_citations_in_file, id_disciplines_map, ssh_disciplines, ssh_set, not_ssh_set)
                results = list(tqdm(executor.map(count_citations_partial, self._list_coci_files), total=len(self._list_coci_files), desc='Iterating files...', colour='cyan', smoothing=0.1))

            print('Updating results...\n')
            discipline_counter = {}
            not_ssh_citations = 0
            ssh_citations = 0
            separator = '#' * 50
            if answer_to_q1 and answer_to_q2 and answer_to_q3:
                for discipline_dict, ssh_count, not_ssh_count in results:
                    ssh_citations += ssh_count
                    not_ssh_citations += not_ssh_count
                    for discipline, counts in discipline_dict.items():
                        if discipline not in discipline_counter:
                            discipline_counter[discipline] = {'citing': 0, 'cited': 0}
                        discipline_counter[discipline]['citing'] += counts['citing']
                        discipline_counter[discipline]['cited'] += counts['cited']

                discipline_with_highest_citing = max(discipline_counter, key=lambda x: discipline_counter[x]['citing'])
                discipline_with_highest_cited = max(discipline_counter, key=lambda x: discipline_counter[x]['cited'])

                print(f'ANSWER TO QUESTION 1:\nNumber of citations that (according to COCI) involve, either as citing or cited entities, publications in SSH journals (according to ERIH-PLUS) included in OpenCitations Meta: {ssh_citations}')
                print(f'\n{separator}\n')
                print(f'ANSWER TO QUESTION 2:\nThe most citing discipline is {discipline_with_highest_citing}: {discipline_counter[discipline_with_highest_citing]["citing"]}\nThe most cited discipline is {discipline_with_highest_cited}: {discipline_counter[discipline_with_highest_cited]["cited"]}')
                print(f'\n{separator}\n')
                print(f'ANSWER TO QUESTION 3:\nNumber of citations that (according to COCI) start from and go to publications in OpenCitations Meta that are not included in SSH journals: {not_ssh_citations}')
                print('\nDone...')
            else:
                if answer_to_q1:
                    for _, ssh_count, _ in results:
                        ssh_citations += ssh_count
                    print(f'ANSWER TO QUESTION 1:\nNumber of citations that (according to COCI) involve, either as citing or cited entities, publications in SSH journals (according to ERIH-PLUS) included in OpenCitations Meta: {ssh_citations}')
                    print(f'\n{separator}\n')
                if answer_to_q2:
                    for discipline_dict, _, _ in results:
                        for discipline, counts in discipline_dict.items():
                            if discipline not in discipline_counter:
                                discipline_counter[discipline] = {'citing': 0, 'cited': 0}
                            discipline_counter[discipline]['citing'] += counts['citing']
                            discipline_counter[discipline]['cited'] += counts['cited']
                    discipline_with_highest_citing = max(discipline_counter, key=lambda x: discipline_counter[x]['citing'])
                    discipline_with_highest_cited = max(discipline_counter, key=lambda x: discipline_counter[x]['cited'])
                    print(f'ANSWER TO QUESTION 2:\nThe most citing discipline is {discipline_with_highest_citing}: {discipline_counter[discipline_with_highest_citing]["citing"]}\nThe most cited discipline is {discipline_with_highest_cited}: {discipline_counter[discipline_with_highest_cited]["cited"]}')
                    print(f'\n{separator}\n')
                if answer_to_q3:
                    for _, _, not_ssh_count in results:
                        not_ssh_citations += not_ssh_count
                    print(f'ANSWER TO QUESTION 3:\nNumber of citations that (according to COCI) start from and go to publications in OpenCitations Meta that are not included in SSH journals: {not_ssh_citations}')
                    print('\nDone...')

            return ssh_citations, not_ssh_citations, discipline_with_highest_citing, discipline_with_highest_cited, discipline_counter




c = Counter("/Volumes/Extreme SSD/OS_data/Processed_data/smaller_COCI/", "/Volumes/Extreme SSD/OS_data/Processed_data/ERIH_META_Marta/")
count = c.execute_count()
print(count)

"""
c = Counter(r"D:\OpenScience_project\coci_preprocessed", r"D:\OpenScience_project\erih_meta2_10000")

#files = c.get_all_files("/Volumes/Extreme SSD/OS_data/Processed_data/ERIH_META_Marta/", '.csv')
#print(files)

count = c.execute_count(output_dir=r"D:\OpenScience_project\output_counter_modified_files", create_subfiles=True, answer_to_q1=True, answer_to_q2=False, answer_to_q3=False, interval=10000)
print(count)

#print(c._list_erih_meta_files)
#for file in c._list_erih_meta_files:
#    if file.startswith(" "):
#        print(file)

"""

import os
import pandas as pd
from tqdm import tqdm

def get_all_files(i_dir_or_compr, req_type):
    """It returns a list containing all the files found
    in the input folder and with the extension required, like ".csv"."""
    result = []
    if os.path.isdir(i_dir_or_compr):
        for cur_dir, cur_subdir, cur_files in os.walk(i_dir_or_compr):
            for cur_file in cur_files:
                if cur_file.endswith(req_type) and not os.path.basename(cur_file).startswith("."):
                    result.append(os.path.join(cur_dir, cur_file))
    return result

def count_excluded_citations(path_dois_excluded_from_META):
    count_complete_citation = 0
    count_not_complete_citation = 0
    for file in tqdm(get_all_files(path_dois_excluded_from_META, '.csv')):
        chunksize = 10000
        with pd.read_csv(file, chunksize=chunksize, sep=",") as reader:
            for chunk in reader:
                chunk.fillna("", inplace=True)
                df_dict_list = chunk.to_dict("records")
                for line in df_dict_list:
                    citing_in_meta = line['is_citing_in_meta'] == False
                    cited_in_meta = line['is_cited_in_meta'] == False
                    if citing_in_meta and citing_in_meta:
                        count_complete_citation += 1
                    elif cited_in_meta or citing_in_meta:
                        count_not_complete_citation += 1
    return f"count_not_complete_citation:{count_not_complete_citation}, count_complete_citation{count_complete_citation}"



def count_lines(path):
    citations_count = 0
    for file in tqdm(get_all_files(path, '.csv')):
        results = pd.read_csv(file, sep=",")
        citations_count += len(results)
    return citations_count


def sort(a_dictionary):
    sorted_dict = dict(sorted(a_dictionary.items(), key=lambda item: item[1]))
    return sorted_dict


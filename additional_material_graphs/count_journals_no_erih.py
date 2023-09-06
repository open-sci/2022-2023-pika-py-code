import pandas as pd
from os.path import isdir, basename
from os import walk
import os
from tqdm import tqdm

def count_journals_no_erih_and_erih(path_erih_meta, path_txt_no_erih, path_txt_erih):
    '''This function, starting from the dataset obtained from the union of OC Meta and Erih Plus, returns both the number
    of journals that are in Meta but not classified as SSH journals by Erih Plus and the number of journals that are in Meta and
    in Erih. The function needs as input parameters also the path of two txt files where it can save the ISSN of both the journals excluded from
    Erih and the ones included.'''
    result = []

    if isdir(path_erih_meta):
        for cur_dir, cur_subdir, cur_files in walk(path_erih_meta):
            for cur_file in cur_files:
                if cur_file.endswith(".csv") and not basename(cur_file).startswith("."):
                    result.append(os.path.join(cur_dir, cur_file))

    set_issn_no_erih = set()
    set_issn_erih = set()
    for file in tqdm(result):
        df_erih_meta = pd.read_csv(file, keep_default_na=False,
                           dtype={
                               "id": "string",
                               "title": "string",
                               "author": "string",
                               "pub_date": "string",
                               "venue": "string",
                               "page": "string",
                               "type": "string",
                               "issue": "string",
                               "volume": "string",
                               "publisher": "string",
                               "editor": "string",
                               "erih_disciplines": "string"
                           })

        filtered_df_no_erih = df_erih_meta[df_erih_meta['erih_disciplines'] == '']
        unique_ids_no_erih = set(filtered_df_no_erih['venue'])
        set_issn_no_erih.update(unique_ids_no_erih)

        filtered_df_erih = df_erih_meta[df_erih_meta['erih_disciplines'] != '']
        unique_ids_erih = set(filtered_df_erih['venue'])
        set_issn_erih.update(unique_ids_erih)

    with open(path_txt_no_erih, "w") as txt_file:
        txt_file.write(str(set_issn_no_erih))
    with open(path_txt_erih, "w") as txt_file2:
        txt_file2.write(str(set_issn_erih))

    return f"The number of journals that are in Meta but not in Erih Plus are {len(set_issn_no_erih)}", f"The number of journals that are in Meta and classified as SSH journals by Erih Plus are {len(set_issn_erih)}"


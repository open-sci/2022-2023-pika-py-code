import csv

def find_doi(doi, path1):
    found = False
    with open(path1, 'r', encoding="utf8") as csvfile:
        next(csvfile)
        datareader = csv.reader(csvfile)
        for row in datareader:
            if doi in row:
                found = True
    return found

print(find_doi("doi:10.1016/j.procbio.2022.08.014", r"D:\Rottura di Palle\output_meta\filtered_11.csv"))
# 2022-2023-pika-py-code
 The repository for the team Pika.py of the Open Science course a.a. 2022/2023

 The software repository is published on Zenodo at the following link https://doi.org/10.5281/zenodo.8324961.
 
## Reproduce the experiment (`run.py`)

This code is designed to process and analyze different datasets related to citations. It's structured to preprocess the datasets, split them into smaller files, and finally count the citations according to specific criteria. Three main datasets are supported: Meta, ErihPlus, and COCI. There's also the possibility to build and store an additional dataset composed by the union of ErihPlus and Meta file: ERIH-META.

### Features
- Meta Data Processing: Input data in the form of a zip file is split into smaller chunks. Additionally, a list of DOIs is generated.
- ErihPlus Data Processing: The raw CSV input file is processed.
- COCI Data Processing: Similar to Meta data, the COCI zip file is split into smaller chunks. It also provides an option to generate a list of DOIs not found in the Meta dataset.
- ERIH-META Data Processing: Combines processed Meta and ErihPlus data to create a new dataset.
- Citation Counter: Performs citation counting based on the processed datasets.

### Dependencies
To run the script, make sure you have all the necessary python libraries installed.</br>
To collect them all, just 
`pip install -r requirements.txt`

### How to reproduce our results
Navigate to the folder containing the `run.py` file.
Use the following command to run the script:
```
python run.py \
--OCMeta_input_path /path/to/OCMeta_input_file \
--OCMeta_processed_path /path/to/OCMeta_output_directory \
--OCMeta_interval 10000 \
--ErihPlus_input_path /path/to/ErihPlus_input_file \
--ErihPlus_processed_path /path/to/ErihPlus_output_directory \
--COCI_input_path /path/to/COCI_input_file \
--COCI_processed_path /path/to/COCI_output_directory \
--COCI_meta_list_dois /path/to/COCI_meta_list_dois \
--ERIH_META_processed_path /path/to/ERIH_META_output_directory \
--num_cpus 4 \
--output_dir /path/to/output_directory \
--create_subfiles False \
--answer_to_q1 True \
--answer_to_q2 True \
--answer_to_q3 True \
--interval 10000
```
Replace /path/to/... with your actual paths.

After the script runs, check the specified output directory for results.
#### Parameters
- OCMeta_input_path: Path to unprocessed Meta zip file.
- OCMeta_processed_path: Path to store the processed Meta data.
- OCMeta_interval: Desired number of lines for processed Meta files. Default is 10000.
... and so on for other datasets and functionalities.
Refer to the code's parse_args function for a complete list of parameters and their descriptions.

### Note
Make sure to have all the necessary library dependencies installed and ensure that all the imported local modules (lib.csv_manager, preprocess.base, etc.) are available in the respective directories.

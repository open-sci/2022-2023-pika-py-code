#!/usr/bin/env bash
echo You are in: $PWD #Just to print out the PWD (Present Working Directory) for the final user

#set -x
set -e # Used to exit the process if something goes wrong

read -p "Enter input directory path: " INPUT_DIR
read -p "Enter output directory path: " OUTPUT_DIR

python -u "../preprocess/coci_preprocess.py" \
--input-dir "${INPUT_DIR}" \
--output-dir "${OUTPUT_DIR}" \
--interval 3000000

# The following is aimed at deleting the zipped file in order to maintain 
# only the extracted directory with its zipped .csv files inside
for file in "${INPUT_DIR}"/*.zip; do
  rm "$file"
done

python -u "../preprocess/coci_preprocess.py" \
--input-dir "${INPUT_DIR}" \
--output-dir "${OUTPUT_DIR}" \
--interval 3000000
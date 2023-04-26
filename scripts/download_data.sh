#!/usr/bin/env bash

set -e

echo "###############################################################################"
echo "This downloads will require a bit of time, please be patient."
echo "###############################################################################"

# Prompt the user for the 'DATA' general directory
echo "###############################################################################"
echo "You will be asked to provide the path to a general directory which will contain all the data downloaded by means of this script."
echo "Inside this directory, the script will create 3 sub-directories containing the downloaded data."
echo "###############################################################################"

read -p "Enter the general directory to save the data in: " GENERAL_DIR

# Create subdirectories for each URL
COCI="${GENERAL_DIR}/COCI"
META="${GENERAL_DIR}/META"
ERIH_PLUS="${GENERAL_DIR}/ERIH_PLUS"

mkdir -p "${COCI}" "${META}" "${ERIH_PLUS}"

echo "###############################################################################"
# Download the data from the first URL and save it to the first subdirectory
echo "Downloading COCI..."
#wget -O "${COCI}/coci_data.zip" https://figshare.com/ndownloader/articles/6741422/versions/19

echo "###############################################################################"
# Download the data from the second URL and save it to the second subdirectory
echo "Downloading META..."
wget -O "${META}/meta_data.zip" https://figshare.com/ndownloader/files/39266384

echo "###############################################################################"
# Download the data from the third URL and save it to the third subdirectory
echo "Downloading ERIH-PLUS..."
wget -O "${ERIH_PLUS}/erih_plus_data.csv" https://kanalregister.hkdir.no/publiseringskanaler/erihplus/periodical/listApprovedAsCsv

echo "###############################################################################"
echo "Downloads completed, you can now run the next executable (preprocessing.sh)."
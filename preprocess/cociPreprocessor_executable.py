#from coci_preprocess import CociPreProcessing
#CociPreProcessing(input_dir="/Volumes/Extreme SSD/RawData_OpenScience/COCI", output_dir="/Volumes/Extreme SSD/RawData_OpenScience/OUTPUT_COCI", interval = 3000000).split_input()

#/Volumes/Extreme SSD/Data OpenScience/COCI/6741422/

#IN: /Volumes/Extreme SSD/RawData_OpenScience/COCI/
#OUT: /Volumes/Extreme SSD/RawData_OpenScience/OUTPUT_COCI/

#-----------------------
import argparse
from coci_preprocess import CociPreProcessing

parser = argparse.ArgumentParser(description='COCI Preprocessing')
parser.add_argument('--input-dir', type=str, help='Input directory path')
parser.add_argument('--output-dir', type=str, help='Output directory path')
parser.add_argument('--interval', type=int, default=3000000, help='Interval for splitting input files')

args = parser.parse_args()

coci_preprocessing = CociPreProcessing(input_dir=args.input_dir, output_dir=args.output_dir, interval=args.interval)
coci_preprocessing.split_input()
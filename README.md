# 2022-2023-pika-py-code 
## Branch containing executables
 The repository for the team Pika.py of the Open Science course a.a. 2022/2023

## Executable scripts
In the folder ```scripts``` you will find some executable bash (.sh) files. The aim of these files is to allow an easier reproduction of the code we wrote, without the need to go inside python scripts.</br>
In order to execute such files, you first have to launch them from inside their folder. Thus, as shown below, just go straight into the folder within your terminal:
```
bash cd full_path_to_scripts_folder
```
Once you are in the correct directory, simply launch the file you want with the following command:
```
bash ./preprocessing.sh
```
The same applies for the other files.

### Steps and executables
The following, are the steps required to reproduce the final output. Each step will be accompanied by its own executable.

#### 0. Download data
The first step is to download the data. This is carried out by the script ```download_data.sh```.
In order to execute it, you have to launch it from inside the folder ```scripts``` as explained above.

This executable requires the user to specify the general directory in which he wants to store the data. The data will be automatically downloaded from the original sources and stored inside automatically created sub-folders inside the general one which is defined by the user. 

Please, be aware that you will need ```wget``` installed. If you are running this on a macOS based system, you can download ```wget``` with the following command:
```
brew install wget
```
Clearly, you will first need to have ```brew``` installed. You can install it by running the following in your terminal:
```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
For different OS, just search online the best way to download ```wget```.</br></br>
To check whether everything is fine, type in your terminal ```which wget```, it should return something like ```/usr/local/bin/wget```.

#### 1. Preprocessing
The following step concerns data preprocessing. This is carried out by the script ```preprocessing.sh```. 
This script will take into account all the preprocessing steps useful to produce the cleaned dataset on which we will perform some analysis.
<b>For now, it contains only the preprocessing operations required in order to clean, and extract, all the data inside COCI.</b></br>
In order to execute it, you have to launch it from inside the folder ```scripts``` as explained above.

<span style="color:red"><b>PLEASE, PAY ATTENTION:</b></span> This execution takes into account the raw data extracted from the original source as they are. Thus, by running this file, you will first extract the main .zip file. Then, <i>this first .zip file will be <b>deleted from the original directory</b>, which will now contain the extracted folder</i>. Such new folder contains many different sub-folders, each of which will be extracted and processed, resulting in many cleaned .csv files which will be stored inside the directory specified by the end user. <span style="color:red"><b>This file is just a FIRST VERSION of the final preprocessing.sh, the final file will return to the user the final dataset</b></span>.

This executable requires the user to specify input and output directories. The former, is the directory containing COCI's data exactly as they are, the latter is the directory in which you want to store the produced .csv files.

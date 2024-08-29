"""
Name: ledger.py
Purpose: log the input files for a set of model runs and copy the unique files needed to run them


Author: Ibrahim Itani
Last Updated: March 2024
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""

#Import libraries
import os
from path import Path
import datetime
import pandas
import shutil
import csv

#model start end time

# Goal of this script:
#  - Remove file redundancy from old model runs to free up space.
#  - It catalogs every version of every file in all the model runs.
#  - It identifies the minimum set of files needed to recreate any model run and saves them.
#  - Once this script is run, other scripts can be used to rebuild these model runs.


#function to identify model run start and end time based on specific files
def start_end_time(run_folder):
    start_times = []
    end_times = []
    for file in run_folder.files():
        file_name = os.path.split(file)[1]
        if file_name in ["gapstats.txt", "timelog.start.txt"]:
            start_times.append(os.stat(file).st_mtime)
        elif file_name in ["ixxi_taz.csv", "cveh_taz.csv", "worker_ixxifractions.csv", "ixxi_taz.dbf"]:
            end_times.append(os.stat(file).st_mtime)
    #identify model start time
    if len(start_times) > 0:
        start_time = min(start_times)
        start_time = int(start_time)

    else:
        print("Model run did not start. Files will not be copied")
        start_time = 0
    #identify model end time
    if len(end_times) > 0:
        end_time = min(end_times)
        end_time=int(end_time)
    else:
        print("Model did not end. Files will not be copied")
        end_time = 2000000000
    return start_time,end_time,start_times, end_times

#function to make a log of all model run folder input files and copy the one that were already stored
def run_folder_logger(run_folder,unique_run_files,ledger_dir,outputs_to_keep):
    run_combos = []
    if os.path.isdir(run_folder):
        # get model start and end times
        start_time, end_time, start_times, end_times = start_end_time(run_folder)
        #go through all the files in the model run folder
        for file in run_folder.files():
            #separate the names from the extension
            file_name_no_ext = os.path.splitext(os.path.basename(file))[0]
            file_name_ext = os.path.splitext(os.path.basename(file))[1]
            #find last modified date
            last_Mod = os.stat(file).st_mtime
            keep_output_flag = 0
            if len(outputs_to_keep) > 0 and last_Mod > end_time:
                for output_file in outputs_to_keep:
                    output_file_no_ext=output_file.split(".")[0]
                    output_file_ext = "." + output_file.split(".")[-1]
                    if output_file_no_ext in file_name_no_ext and file_name_ext == output_file_ext:
                        print(output_file_no_ext)
                        keep_output_flag = 1
                        break

            #if the file was last modified before the model run started then it is an input file that might need to be copied
            if int(last_Mod) < start_time or keep_output_flag == 1:
                #rename the file as name---date.extension
                last_Mod_pretty1 = datetime.datetime.utcfromtimestamp(last_Mod).strftime("%a %b %d %Y %H-%M-%S ")
                combo = file_name_no_ext + str("---") + str(last_Mod_pretty1) + file_name_ext
                #add this new file to the list of input files for this model runs
                run_combos.append(combo)

                # if new to the unique list of files add it to the set of stored files
                if combo not in unique_run_files:
                    unique_run_files.append(combo)
                    file_name_record = file_name_no_ext + str("---") + str(last_Mod_pretty1) + file_name_ext
                    dest_folder = os.path.join(ledger_dir, "run_folder")
                    if not os.path.exists(dest_folder):
                        os.makedirs(dest_folder)
                    destination = os.path.join(dest_folder,file_name_record)
                    origin = os.path.abspath(file)
                    if os.path.isfile(destination)==False:
                        shutil.copy2(origin, destination)

    return unique_run_files, run_combos

#function to make a log of all supporting folder input files and copy the one that were already stored
def supporting_files_logger(parent_wd,supporting_folder,unique_list,ledger_dir):
    #go to supporting folder: input, daysim, daysim/coefficients, daysim/software
    supp_wd = os.path.join(parent_wd, supporting_folder)
    supp_wd = Path(supp_wd)
    supp_combos = []
    if os.path.isdir(supp_wd):
        #Go through all the files
        for file in supp_wd.files():
            file_name_no_ext = os.path.splitext(os.path.basename(file))[0]
            file_name_ext = os.path.splitext(os.path.basename(file))[1]
            #find last modified date
            last_Mod = os.stat(file).st_mtime
            last_Mod_pretty1 = datetime.datetime.utcfromtimestamp(last_Mod).strftime("%a %b %d %Y %H-%M-%S ")
            combo = file_name_no_ext + str("---") + str(last_Mod_pretty1) + file_name_ext
            supp_combos.append(combo)

            # if new to the unique list add it as name+---date+extension and add it to the sorted files
            if combo not in unique_list:
                unique_list.append(combo)
                file_name_record = file_name_no_ext + str("---") + str(last_Mod_pretty1) + file_name_ext
                dest_folder=os.path.join(ledger_dir,supporting_folder)
                if not os.path.exists(dest_folder):
                    os.makedirs(dest_folder)
                destination = os.path.join(dest_folder,file_name_record)
                origin = os.path.abspath(file)
                if os.path.isfile(destination)==False:
                    shutil.copy2(origin, destination)
    return unique_list, supp_combos

#Function to write out a log of all the input files. These logs are needed for rebuilding the model runs
def write_full_log(ledger_dir,supporting_folder,specific_master_log):
    ledger_file_name=supporting_folder+"_file_list.csv"
    ledger_file_full_name=os.path.join(ledger_dir, ledger_file_name)
    # Check if the file exists
    file_exists = os.path.isfile(ledger_file_full_name)

    # Initialize a set to store existing parent folders
    existing_parent_folders = set()

    # If the file exists, read the existing parent folders
    if file_exists:
        with open(ledger_file_full_name, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_parent_folders.add(row['Parent Folder'])

    # Open the file in append mode if it exists, otherwise create a new file
    with open(ledger_file_full_name, 'a', newline='') as csvfile:
        fieldnames = ['Parent Folder', 'File Name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # If the file doesn't exist, write the header
        if not file_exists:
            writer.writeheader()

        # Write data
        for parent_folder, files in specific_master_log.items():
            # Check if the parent folder already exists
            if parent_folder not in existing_parent_folders:
                for file_name in files:
                    writer.writerow({'Parent Folder': str(parent_folder), 'File Name': file_name})

#function to utilize the other other functions in the correct order on a list of model runs
def run_ledger(run_folders_csv,ledger_dir,source,outputs_to_keep):
    #get list of directories to check
    run_folders_csv = pandas.read_csv(run_folders_csv)
    run_folders=run_folders_csv["model_runs"]
    master_log= {}
    master_run_log = {}
    master_input_log = {}
    master_daysim_log = {}
    master_daysim_coef_log = {}
    master_daysim_soft_log = {}
    unique_run_files=[]
    unique_daysim_files=[]
    unique_daysim_coef_files=[]
    unique_daysim_soft_files=[]
    unique_input_files=[]

    #for every directory check if it exists
    for run_folder in run_folders:
        if os.path.isdir(run_folder) == False and run_folder.split(os.sep)[2].lower() != "win10-lelizondo":
            print("Could not find: "+run_folder)
        else:
            split_path = run_folder.split(os.sep)
            if split_path[2].lower()=="win10-lelizondo":
                if "D" in split_path:
                    # Find the indices of elements to replace
                    replace_indices = [i for i, x in enumerate(split_path) if x == "D"]
                    for i in replace_indices:
                        split_path[i] = "D_model_run"
                    b = r"\\"
                    fixed_path = os.path.join(*split_path)
                    run_folder = os.path.join(b, fixed_path)
                    print("Adjusted win10-lelizondo\D to win10-lelizondo\D_model_run")
            print("Logging and copying over files from: "+run_folder)
            parent_wd = os.path.dirname(run_folder)
            run_folder = Path(run_folder)

            #copy unique run_folder_files
            unique_run_files, run_combos = run_folder_logger(run_folder,unique_run_files, ledger_dir,outputs_to_keep)

            #do the same for supporting files
            unique_input_files, input_combos = supporting_files_logger(parent_wd, "input", unique_input_files,ledger_dir)
            unique_daysim_files, daysim_combos = supporting_files_logger(parent_wd, "daysim", unique_daysim_files, ledger_dir)
            unique_daysim_coef_files, daysim_coef_combos = supporting_files_logger(parent_wd, "daysim\coefficients", unique_daysim_coef_files, ledger_dir)
            unique_daysim_soft_files, daysim_soft_combos = supporting_files_logger(parent_wd, "daysim\software", unique_daysim_soft_files, ledger_dir)


            #add this entry into a dictionary/ log
            log = {run_folder: (run_combos,input_combos,daysim_combos,daysim_coef_combos,daysim_soft_combos)}

            run_log={run_folder:run_combos}
            input_log={run_folder:input_combos}
            daysim_log={run_folder:daysim_combos}
            daysim_coef_log = {run_folder: daysim_coef_combos}
            daysim_soft_log = {run_folder: daysim_soft_combos}

            master_log.update(log)

            master_run_log.update(run_log)
            master_input_log.update(input_log)
            master_daysim_log.update(daysim_log)
            master_daysim_coef_log.update(daysim_coef_log)
            master_daysim_soft_log.update(daysim_soft_log)



    if os.path.isdir(ledger_dir) == False:
        source=Path(source)
        shutil.copytree(source, ledger_dir)

    #write out the logs
    write_full_log(ledger_dir,"run",master_run_log)
    write_full_log(ledger_dir,"input",master_input_log)
    write_full_log(ledger_dir,"daysim",master_daysim_log)
    write_full_log(ledger_dir,"daysim_coef",master_daysim_coef_log)
    write_full_log(ledger_dir,"daysim_software",master_daysim_soft_log)



if __name__ == '__main__':
    #list of run folders
    run_folders_csv = r"\\data-svr\Modeling\SACSIM23\Model_Catalog_Tool\run_list.csv"
    #Output files to keep file_name+extension needed. ex: daynet.net will look for files that have daynet in the name (2035_daynet.net)
    outputs_to_keep=["daynet.net","pathwayind.csv"]
    #select where you want your ledger directory to be
    ledger_dir = r"\\data-svr\Modeling\SACSIM23\Model_Catalog_Tool"
    # template of file structure
    source = r".\file_output"
    run_ledger(run_folders_csv, ledger_dir,source,outputs_to_keep)
    print("Done!")

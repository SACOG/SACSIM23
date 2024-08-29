"""
Name: rebuilder.py
Purpose: Rebuild model run input files for runs that are logged in the ledger


Author: Ibrahim Itani
Last Updated: March 2024
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""

# Import libraries
from path import Path
import pandas
import shutil
import os

#Function that looks for files in the ledger and copied them over
def copy_files(ledger_csv, existing_model_run, subfolder, destination):

    #for a specific run that you want to rebuild look for all the file names in the ledger that belong to this run
    file_names = ledger_csv["File Name"][ledger_csv["Parent Folder"] == existing_model_run]
    #specify which path you want to paste your files in
    new_run_folder = os.path.join(destination, subfolder)
    new_run_folder = Path(new_run_folder)
    #for each file, find it's appropriate subfolder and rename it to exclude the date from the name and copy it there
    for file_name in file_names:

        file_name_no_ext = file_name.split(".")[0]
        if len(file_name.split("."))>1:
            file_name_no_ext = ".".join(file_name.split(".")[:-1])
            file_ext = file_name.split(".")[-1]
        else:
            #in the rare case of a missing extension this sets the extension to txt directly
            file_ext = "txt"
        #remove date from file name and attach it to the destination file path
        deconstructed_file_name = file_name_no_ext.split("---")[0] + str(".") + file_ext
        file_full_path = os.path.join(new_run_folder, deconstructed_file_name)

        # orgin
        if subfolder == "run_folder":
            orig1 = os.path.join(ledger_folder, "run_folder")
        else:
            orig1 = os.path.join(ledger_folder, subfolder)

        orig = os.path.join(orig1, file_name)
        if os.path.isfile(orig):
            shutil.copy2(orig, file_full_path)

def run_rebuilder(run_folders_csv,input_folders_csv,daysim_folders_csv,daysim_coef_folders_csv,daysim_software_folders_csv,existing_model_run,new_path,source):
    run_folders = set(run_folders_csv["Parent Folder"])
    split_path = existing_model_run.split(os.sep)
    if split_path[2].lower() == "win10-lelizondo":
        if "D" in split_path:
            # Find the indices of elements to replace
            replace_indices = [i for i, x in enumerate(split_path) if x == "D"]
            for i in replace_indices:
                split_path[i] = "D_model_run"
            b = r"\\"
            fixed_path = os.path.join(*split_path)
            existing_model_run = os.path.join(b, fixed_path)
            print("Adjusted win10-lelizondo\D to win10-lelizondo\D_model_run")
    if existing_model_run in run_folders:
        print("Model run was found")
        existing_model_run = Path(existing_model_run)
    else:
        raise Exception("Could not find run")

    if os.path.isdir(new_path):
        print("Valid directory path:", new_path)
        os.chdir(new_path)
        new_path = Path(new_path)
    else:
        os.makedirs(dest_folder)

    name_new_run = os.path.basename(existing_model_run)

    destination = os.path.join(new_path, name_new_run)
    destination = Path(destination)
    source=Path(source)
    # copy model run folder structure
    if os.path.isdir(destination) == False:
        shutil.copytree(source, destination)

    copy_files(run_folders_csv, existing_model_run,  "run_folder", destination)
    copy_files(input_folders_csv, existing_model_run,  "input", destination)
    copy_files(daysim_folders_csv, existing_model_run,  "daysim", destination)
    copy_files(daysim_coef_folders_csv, existing_model_run,  "daysim\coefficients", destination)
    copy_files(daysim_software_folders_csv, existing_model_run,  "daysim\software", destination)



if __name__ == '__main__':
    #read ledger folder where ledger and files are currently stored
    ledger_folder=r"\\data-svr\Modeling\SACSIM23\Model_Catalog_Tool"
    run_folders_csv = pandas.read_csv(os.path.join(ledger_folder,"run_file_list.csv"))
    input_folders_csv = pandas.read_csv(os.path.join(ledger_folder,"input_file_list.csv"))
    daysim_folders_csv = pandas.read_csv(os.path.join(ledger_folder,"daysim_file_list.csv"))
    daysim_coef_folders_csv = pandas.read_csv(os.path.join(ledger_folder,"daysim_coef_file_list.csv"))
    daysim_software_folders_csv = pandas.read_csv(os.path.join(ledger_folder,"daysim_software_file_list.csv"))

    #read template of file structure
    os.chdir(os.path.dirname(__file__))
    #source = r".\file_output"
    source = os.path.join(os.path.dirname(__file__),"file_output")

    #Indicate the name of model run to be rebuilt
    existing_model_run = r"\\win10-model-5\D\SACSIM23\2035\P1\run_2035_P1BL_pjtscn_itern5\run_2035_P1BL_pjtscn_itern5"

    #Indicate the folder to build the model run in
    new_path = r"\\data-svr\Modeling\SACSIM23\Model_Catalog_Tool\rebuilder_output"

    run_rebuilder(run_folders_csv,input_folders_csv,daysim_folders_csv,daysim_coef_folders_csv,daysim_software_folders_csv,existing_model_run,new_path,source)

    print("Done!")







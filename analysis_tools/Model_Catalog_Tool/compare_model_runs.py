"""
Name: compare_model_runs.py
Purpose: Compare which input files are different between two logged model runs


Author: Ibrahim Itani
Last Updated: March 2024
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""

#Import libraries
import os
import pandas
from ledger import run_ledger
import csv
import pandas
def diff_files(ledger_csv,model_run_1,model_run_2):
    file_names_1 = ledger_csv["File Name"][ledger_csv["Parent Folder"] == model_run_1]
    file_names_2 = ledger_csv["File Name"][ledger_csv["Parent Folder"] == model_run_2]
    file_names_1 = [str(x) for x in file_names_1]
    file_names_2 = [str(x) for x in file_names_2]

    different_1 = []
    different_2 = []
    for file in file_names_1:
        #print(file)
        if file not in file_names_2:
            file_name_no_ext = file.split(".")[0]
            different_1.append(file_name_no_ext)
    for file in file_names_2:
        if file not in file_names_1:
            file_name_no_ext = file.split(".")[0]
            different_2.append(file_name_no_ext)
    return different_1, different_2
def run_compare_model_runs(run_ledger,daysim_coef_ledger,daysim_ledger,daysim_software_ledger,input_ledger,model_run_1,model_run_2):
    runs = [str(x) for x in run_ledger["Parent Folder"]]
    if model_run_1 not in runs:
        error=str(model_run_1)+" is not in the ledger. Please add it to run_list.csv and run the ledger.py script first!"
        raise Exception(error)
    if model_run_2 not in runs :
        error = str(model_run_2) + " is not in the ledger. Please add it to run_list.csv and run the ledger.py script first!"
        raise Exception(error)

    [different_1_run, different_2_run] = diff_files(run_ledger,model_run_1,model_run_2)
    [different_1_daysim_coef, different_2_daysim_coef] = diff_files(daysim_coef_ledger, model_run_1, model_run_2)
    [different_1_daysim, different_2_daysim] = diff_files(daysim_ledger, model_run_1, model_run_2)
    [different_1_daysim_software, different_2_daysim_software] = diff_files(daysim_software_ledger, model_run_1, model_run_2)
    [different_1_input, different_2_input] = diff_files(input_ledger, model_run_1, model_run_2)

    if len(different_1_run)>0:
        print("\nThe first model run has these unique run files: \n" + "\n".join(different_1_run))
    if len(different_2_run) > 0:
        print("\nThe second model run has these unique run files: \n"+ "\n".join(different_2_run))
    if len(different_1_run) == 0 and len(different_2_run) == 0:
        print("\nThe run folders are identical")

    if len(different_1_daysim_coef) > 0:
        print("\nThe first model run has these unique daysim\coefficients files: \n" + "\n".join(different_1_daysim_coef))
    if len(different_2_daysim_coef) > 0:
        print("\nThe second model run has these unique daysim\coefficients files: \n" + "\n".join(different_2_daysim_coef))
    if len(different_1_daysim_coef) == 0 and len(different_2_daysim_coef) == 0:
        print("\nThe daysim\coefficient folders are identical")

    if len(different_1_daysim) > 0:
        print("\nThe first model run has these unique daysim files: \n" + "\n".join(different_1_daysim))
    if len(different_2_daysim) > 0:
        print("\nThe second model run has these unique daysim files: \n" + "\n".join(different_2_daysim))
    if len(different_2_daysim) == 0 and len(different_2_daysim) == 0:
        print("\nThe daysim first level folders are identical")

    if len(different_1_daysim_software) > 0:
        print("\nThe first model run has these unique daysim\software files: \n" + "\n".join(different_1_daysim_software))
    if len(different_2_daysim_software) > 0:
        print("\nThe second model run has these unique daysim\software files: \n" + "\n".join(different_2_daysim_software))
    if len(different_1_daysim_software) == 0 and len(different_2_daysim_software) == 0:
        print("\nThe daysim\software folders are identical")

    if len(different_1_input) > 0:
        print("\nThe first model run has these unique input files: \n" + "\n".join(different_1_input))
    if len(different_2_input) > 0:
        print("\nThe second model run has these unique input files: \n" + "\n".join(different_2_input))
    if len(different_1_input) == 0 and len(different_2_input) == 0:
        print("\nThe input folders are identical")

if __name__ == '__main__':
    ledger_folder = r"\\data-svr\Modeling\SACSIM23\Model_Catalog_Tool"

    run_ledger = pandas.read_csv(os.path.join(ledger_folder, "run_file_list.csv"))
    daysim_coef_ledger = pandas.read_csv(os.path.join(ledger_folder, "daysim_coef_file_list.csv"))
    daysim_ledger = pandas.read_csv(os.path.join(ledger_folder, "daysim_file_list.csv"))
    daysim_software_ledger = pandas.read_csv(os.path.join(ledger_folder, "daysim_software_file_list.csv"))
    input_ledger = pandas.read_csv(os.path.join(ledger_folder, "input_file_list.csv"))

    model_run_1 = r"\\win10-model-3\D\SACSIM23\2035\P1\run_2035_P1BL_pjtscn_itern7\run_2035_P1BL_pjtscn_itern7"
    model_run_2 = r"\\win10-model-5\D\SACSIM23\2035\P1\run_2035_P1BL_pjtscn_itern5\run_2035_P1BL_pjtscn_itern5"


    run_compare_model_runs(run_ledger, daysim_coef_ledger, daysim_ledger, daysim_software_ledger, input_ledger,
                           model_run_1, model_run_2)
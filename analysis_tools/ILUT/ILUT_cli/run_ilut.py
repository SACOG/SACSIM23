"""
Name: run_ilut.py
Purpose: 
    Single script that allows user to both load and analyze/summarize model outputs
    into Integrated Land Use Transportation (ILUT) parcel table

Author: Darren Conly
Last Updated: Jul 2022
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""
    
import importlib
import os
import sys

from arcpy import GetParameterAsText, AddMessage

import bcp_loader
importlib.reload(bcp_loader)

from MakeCombinedILUT import ILUTReport
                
gis_interface = False

def inspect_input_file(in_file_path):
    """Checks to make sure (1) input file exists and (2) it's large
    enough to have data in it (i.e., it's not empty)"""
    file_size_kb = round(os.path.getsize(in_file_path)/1000, 2)

    if not os.path.exists(in_file_path):
        raise Exception(f"{in_file_path} not found. Please confirm file exists")

    if file_size_kb < 5:
        cont_decn = input(f"WARNING: {in_file_path} is only {file_size_kb}KB. Continue (y/n)? ")

        if cont_decn.lower() == 'y':
            pass
        else:
            print("Quitting script...")
            sys.exit()


if __name__ == '__main__':
    
    base_years = [2016, 2020]
    #===============PARAMETERS SET AT EACH RUN========================
    if gis_interface:
        model_run_folder = GetParameterAsText(0)
        scenario_year = int(GetParameterAsText(1))
        scenario_id = int(GetParameterAsText(2))
        lu_scenario = GetParameterAsText(3)
        av_tnc = GetParameterAsText(4) # whether AVs or TNCs are assumed to be operating
        scenario_desc = GetParameterAsText(5) # string description of the scenario
        run_ilut_combine = GetParameterAsText(6) # boolean
        remove_input_tables = GetParameterAsText(7) # boolean; indicate if you want to only keep the resulting "ilut combined" table
        shared_externally = GetParameterAsText(8) # boolean; indicate if the run is shared externally and needs to be saved/archived (e.g. MTIP amendment, MTP run)
        
    else:
        model_run_folder = input("Enter model run folder path: ")
        scenario_year = int(input("Enter scenario year: "))
        scenario_id = int(input("Enter scenario ID number: "))
        lu_scenario = 'BY_latest' if scenario_year in base_years \
            else input("Enter land use scenario ID: ")
        av_tnc = None # will be set later via user prompt in CLI interface
        scenario_desc = None # will be set later via user prompt in CLI interface
        run_ilut_combine = input("Do you want to run ILUT Combine script after loading tables (enter 'true' or 'false')? ")
        remove_input_tables = input("Do you want to remove raw input tables after creating final combined ILUT table (enter 'true' or 'false')? ")
        shared_externally = input("Will this run be shared externally (enter 'true' or 'false')? ") # boolean; indicate if the run is shared externally and needs to be saved/archived (e.g. MTIP amendment, MTP run)

    #=============SELDOM-CHANGED PARAMETERS==========================
    # folder containing query files used to create tables
    os.chdir(os.path.dirname(__file__)) # ensure that you start in same folder as script
    query_dir = os.path.abspath("sql_bcp") # subfolder with sql scripts
    
    sql_server_name = 'SQL-SVR'
    ilut_db_name = 'MTP2024' # 'MTP2020'
    
    # in table names, base year and earlier is usually written as 4-digit year, while for future years its
    # written as "pa<two-digit year"
    # 7/27/2022 - eventually we should get rid of this. All years should be 4-digit year rather than "PA",
        # which meaning "preferred alternative" does not describe most ILUT runs, which are test alts
    
    # legacy naming convention through 2020 MTP: future years would be "paYY" format instead of 
    # 4-digit "YYYY" format. Starting in Blueprint 2024 we change to be "YYYY", but allowing 
    # easy switching here if needed.
    yeartag = scenario_year

    use_pa_yeartag = False 
    if use_pa_yeartag:
        yeartag = f"pa{str(scenario_year)[-2:]}" if scenario_year not in base_years else scenario_year
    
    # indicate which tables you want to load, if not all tables
    load_triptbl = True
    load_tourtbl = True
    load_persontbl = True
    load_persondaytbl = True
    load_hhtbl = True
    load_parceltbl = True
    load_ixxworkerfractbl = True
    load_cveh_taztbl = True
    load_ixxi_taztbl = True

    # convert ESRI "true"/"false" string to python booleans
    tf_dict = {'true':True, 'false':False, 'yes':True, 'no':False}

    run_ilut_combine = tf_dict[run_ilut_combine.lower()]
    remove_input_tables = tf_dict[remove_input_tables.lower()]
    shared_externally = tf_dict[shared_externally.lower()]

    # TAZ table
    taz_tbl = "TAZ21_RAD07"  # "TAZ07_RAD07"

    # master parcel table
    parcel_master_lookup = {"BY_latest": "PARCEL_MASTER",
                            "P1_latest": "PARCEL_MASTER",
                            "P2_latest": "PARCEL_MASTER", 
                            "P3_latest": "PARCEL_MASTER", 
                            "DS_latest": "PARCEL_MASTER_DS"}
    
    parcel_master = parcel_master_lookup[lu_scenario]
    
    # population and envision tomorrow land use tables
    pop_tblname = f"raw_pop{scenario_year}_{lu_scenario}" # population table name string template
    # pop_tblname = f"raw_pop2020_BY_latest_08302023" # population table name string template - TEMPORARILY ADJUSTED 9/6/2023 for agreement with model run person table
    eto_tblname = f"raw_eto{scenario_year}_{lu_scenario}" # envision-tomorrow parcel table name string template

    k_sql_tbl_name = "sql_tbl_name"
    k_input_file = "in_file_name"
    k_file_format = "file_field_delimiter"
    k_sql_qry_file = "create_table_sql_file"
    k_data_start_row = "data_start_row"
    k_load_tbl = "load_table"
    
    
    ilut_tbl_specs = [{k_sql_tbl_name: "raw_parcel", 
                      k_input_file: f"{yeartag}_raw_parcel.txt",
                      k_sql_qry_file: 'create_parcel_table.sql',
                      k_data_start_row: 2,
                      k_load_tbl: load_parceltbl},
                     {k_sql_tbl_name: "raw_hh", 
                      k_input_file: "_household.tsv",
                      k_sql_qry_file: 'create_hh_table.sql',
                      k_data_start_row: 2,
                      k_load_tbl: load_hhtbl},
                     {k_sql_tbl_name: "raw_person", 
                      k_input_file: "_person.tsv",
                      k_sql_qry_file: 'create_person_table.sql',
                      k_data_start_row: 2,
                      k_load_tbl: load_persontbl},
                     {k_sql_tbl_name: "raw_personday", 
                      k_input_file: "_person_day.tsv",
                      k_sql_qry_file: 'create_person_day_table.sql',
                      k_data_start_row: 2,
                      k_load_tbl: load_persondaytbl},
                     {k_sql_tbl_name: "raw_tour", 
                      k_input_file: "_tour.tsv",
                      k_sql_qry_file: 'create_tour_table.sql',
                      k_data_start_row: 2,
                      k_load_tbl: load_tourtbl},
                     {k_sql_tbl_name: "raw_trip", 
                      k_input_file: "_trip_1_1.csv",
                      k_sql_qry_file: 'create_trip_table_wskimvals.sql',
                      k_data_start_row: 2,
                      k_load_tbl: load_triptbl},
                     {k_sql_tbl_name: "raw_cveh", 
                      k_input_file: "cveh_taz.dbf", 
                      k_sql_qry_file: 'create_cveh_taz.sql',
                      k_data_start_row: 2,
                      k_load_tbl: load_cveh_taztbl},
                     {k_sql_tbl_name: "raw_ixxi", 
                      k_input_file: "ixxi_taz.dbf",
                      k_sql_qry_file: 'create_ixxi_taz.sql',
                      k_data_start_row: 2,
                      k_load_tbl: load_ixxi_taztbl},
                     {k_sql_tbl_name: "raw_ixworker", 
                      k_input_file: "worker_ixxifractions.dat",
                      k_sql_qry_file: 'create_ixworker_table.sql',
                      k_data_start_row: 1,
                      k_load_tbl: load_ixxworkerfractbl},
                     ]
    
    #======================RUN SCRIPT=================================
    # create instance of ILUT combiner report; in so doing, ask for additional info required to do
    # the ILUT aggregation once the tables have loaded. By having this here, before the loading,
    # the user can have a "one and done" process, just setting parameters once, hitting "go",
    # and having the full ILUT process happen for them.
    if run_ilut_combine:
        comb_rpt = ILUTReport(model_run_dir=model_run_folder, 
                                dbname=ilut_db_name, 
                                master_parcel_tbl=parcel_master,
                                sc_yr=scenario_year, 
                                sc_code=scenario_id, 
                                land_use_scen=lu_scenario, 
                                envision_tomorrow_tbl=eto_tblname,
                                pop_table=pop_tblname, 
                                taz_rad_tbl=taz_tbl, 
                                av_tnc_type=av_tnc, 
                                sc_desc=scenario_desc, 
                                shared_ext=shared_externally)

        if comb_rpt.shared_externally():
            raise Exception(f"An ILUT table for year {scenario_year} and scenario ID {scenario_id} already exists in SQL Server " \
                "and has been shared externally. Please consult the ilut_scenario_log table to choose a "\
                " different scenario ID.")
    else:
        pass
        AddMessage("Loading model output tables but will NOT run ILUT combination process...\n")
    
    # change workspace to model run folder
    os.chdir(model_run_folder)
    

    # 7/29/22: consider adding "load_tables = True/False" feature, i.e., allow running
    # without reloading tables.
    tbl_loader = bcp_loader.BCP(svr_name=sql_server_name, db_name=ilut_db_name)

    AddMessage("Loading model output files into SQL Server database...")
    for tblspec in ilut_tbl_specs:
        if tblspec[k_load_tbl]:
            sql_tname = f"{tblspec[k_sql_tbl_name]}{scenario_year}_{scenario_id}_{lu_scenario}"
            input_file = os.path.join(model_run_folder, tblspec[k_input_file]) # need full path to enable using UNC file path
            inspect_input_file(input_file) # make sure that the input file exists and warn user if file seems too small (<5kb)

            qry_file = os.path.join(query_dir, tblspec[k_sql_qry_file])
            startrow = tblspec[k_data_start_row]
            
            # populate table creation query file with name of table to create
            with open(qry_file, 'r') as f_sql_in:
                raw_sql = f_sql_in.read()
                formatted_sql = raw_sql.format(sql_tname)
        
            AddMessage(f"\tLoading {input_file}...")
            tbl_loader.create_sql_table_from_file(input_file, formatted_sql, sql_tname,
                                              overwrite=True, data_start_row=startrow)
        else:
            AddMessage(f"Skipping loading of {tblspec[k_sql_tbl_name]} table...")
            continue

    AddMessage("All tables successfully loaded!\n")
    
    if run_ilut_combine:
        AddMessage("Starting ILUT combining/aggregation process...\n")
        comb_rpt.pop_parcel_qa()
        comb_rpt.run_report(delete_input_tables=remove_input_tables)
        comb_rpt.topline_summary(["PT_TOT_RES", "VT_TOT_RES", "VMT_TOT_RES"])
    


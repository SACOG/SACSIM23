# -*- coding: utf-8 -*-
"""
Single script that allows user to both load and analyze/summarize model outputs
into Integrated Land Use Transportation (ILUT) parcel table
"""

import os
import csv

from dbfread import DBF
from arcpy import GetParameterAsText, AddMessage
 
from bcp_loader import BCP # bcp_loader script must be in same folder as this script to import it
from MakeCombinedILUT import ILUTReport
                
gis_interface = False

if __name__ == '__main__':
    
    #===============PARAMETERS SET AT EACH RUN========================
    if gis_interface:
        model_run_folder = GetParameterAsText(0) # r'D:\SACSIM19\MTP2020\Conformity_Runs\run_2035_MTIP_Amd1_Baseline_v1'
        scenario_year = int(GetParameterAsText(1)) # 2035
        scenario_id = int(GetParameterAsText(2)) # 999
        av_tnc = GetParameterAsText(3) # whether AVs or TNCs are assumed to be operating
        scenario_desc = GetParameterAsText(4) # string description of the scenario
        run_ilut_combine = GetParameterAsText(5) # boolean
        remove_input_tables = GetParameterAsText(6) # boolean; indicate if you want to only keep the resulting "ilut combined" table
        shared_externally = GetParameterAsText(7) # boolean; indicate if the run is shared externally and needs to be saved/archived (e.g. MTIP amendment, MTP run)

    else:
        model_run_folder = input("Enter model run folder path: ")# r'D:\SACSIM19\MTP2020\Conformity_Runs\run_2035_MTIP_Amd1_Baseline_v1'
        scenario_year = int(input("Enter scenario year: ")) # 2035
        scenario_id = int(input("Enter scenario ID number: ")) # 999
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
    # writted as "pa<two-digit year"
    base_year = 2016
    yeartag = "pa{}".format(str(scenario_year)[-2:]) if scenario_year > base_year else scenario_year
    
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
    tf_dict = {'true':True, 'false':False}

    run_ilut_combine = tf_dict[run_ilut_combine.lower()]
    remove_input_tables = tf_dict[remove_input_tables.lower()]
    shared_externally = tf_dict[shared_externally.lower()]

    # master parcel table and TAZ table
    taz_tbl = "TAZ21_RAD07"  # "TAZ07_RAD07"
    master_parcel_table = "PARCEL_MASTER"
    
    # population tables
    pop_y1 = 'raw_Pop2016_latest'
    pop_y2 = 'raw_pop2027_latest'
    pop_y3 = 'raw_Pop2035_latest'
    pop_y4 = 'raw_Pop2040_latest'
    
    # envision-tomorrow parcel tables
    env_tmrw_y1 = 'raw_eto2016_latest'
    env_tmrw_y2 = 'raw_eto2027_latest'
    env_tmrw_y3 = 'raw_eto2035_latest'
    env_tmrw_y4 = 'raw_eto2040_latest'
    
    # set envision-tomorrow and population sql tables for given run based on the scenario year
    yr1 = base_year
    yr2 = 2027
    yr3 = 2035
    yr4 = 2040
    
    pop_yr_dict = {yr1:pop_y1, yr2:pop_y2, yr3:pop_y3, yr4:pop_y4}
    env_tmrw_yr_dict = {yr1:env_tmrw_y1, yr2:env_tmrw_y2, yr3:env_tmrw_y3, yr4:env_tmrw_y4}
    
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
        eto_tbl = env_tmrw_yr_dict[scenario_year]
        popn_tbl = pop_yr_dict[scenario_year]
        comb_rpt = ILUTReport(model_run_dir=model_run_folder, dbname=ilut_db_name, sc_yr=scenario_year, 
                              sc_code=scenario_id, envision_tomorrow_tbl=eto_tbl,
                              pop_table=popn_tbl, taz_rad_tbl=taz_tbl, master_pcl_tbl=master_parcel_table,
                              av_tnc_type=av_tnc, sc_desc=scenario_desc, shared_ext=shared_externally)

        if comb_rpt.shared_externally():
            raise Exception(f"An ILUT table for year {scenario_year} and scenario ID {scenario_id} already exists in SQL Server " \
                "and has been shared externally. Please consult the ilut_scenario_log table to choose a "\
                " different scenario ID.")
    else:
        pass
        AddMessage("Loading model output tables but will NOT run ILUT combination process...\n")
    
    # change workspace to model run folder
    os.chdir(model_run_folder)
    
    tbl_loader = BCP(svr_name=sql_server_name, db_name=ilut_db_name)

    AddMessage("Loading model output files into SQL Server database...")
    for tblspec in ilut_tbl_specs:
        if tblspec[k_load_tbl]:
            sql_tname = f"{tblspec[k_sql_tbl_name]}{scenario_year}_{scenario_id}"
            input_file = tblspec[k_input_file]
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
        comb_rpt.run_report(delete_input_tables=remove_input_tables)
    


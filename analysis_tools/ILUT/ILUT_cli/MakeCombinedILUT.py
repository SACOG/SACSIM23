"""
Name: MakeCombinedILUT.py
Purpose: After model output tables have been loaded into SQL server, this script
    runs a series of queries that aggregate model outputs to give detailed population
    and travel data at the parcel level.
        
          
Author: Darren Conly
Last Updated: Jan 2020
Updated by: <name>
Copyright:   (c) SACOG
Python Version: 3.x
"""

import os
import sys
import time
from pathlib import Path

from arcpy import AddMessage
import pyodbc



#===================================FUNCTIONS================================
class ILUTReport():

    def __init__(self, model_run_dir, dbname, master_parcel_tbl=None, envision_tomorrow_tbl=None, pop_table=None, 
                taz_rad_tbl=None, sc_yr=None, sc_code=None, land_use_scen='', av_tnc_type=None, sc_desc=None, 
                shared_ext=False):
        
        # ========parameters that are unlikely to change or are changed rarely======
        self.driver = '{SQL Server}'
        self.server = 'SQL-SVR'
        self.database = dbname
        self.trusted_connection = 'yes'
        self.conxn_info = "DRIVER={0}; SERVER={1}; DATABASE={2}; Trusted_Connection={3}" \
            .format(self.driver, self.server, self.database, self.trusted_connection)
        self.conn = pyodbc.connect(self.conxn_info) 

        self.scen_log_tbl = "ilut_scenario_log" #logs each run made and asks user for scenario description
        
        #sql script directory
        os.chdir(os.path.dirname(__file__)) # ensure that you start in same folder as script
        self.sql_dir = os.path.abspath("sql_ilut_summary")
        
        #Tables that don't come from model-run folder
        self.parcel_master_tbl = self.update_input_tbl(master_parcel_tbl, "Parcel master table")
        self.taz_rad_table = self.update_input_tbl(taz_rad_tbl, "TAZ-RAD table")
        self.envision_tomorrow_tbl = self.update_input_tbl(envision_tomorrow_tbl, \
            "Envision Tomorrow parcel table")
        self.pop_table = self.update_input_tbl(pop_table, "Population table")
        
        # Specify theme table queries to execute
        self.person_sql = "theme_person.sql"
        self.hh_sql_yesAV = "theme_hh_yesAV.sql"
        self.hh_sql_noAV = "theme_hh_noAV.sql"
        self.triptour_sql_noAV = "theme_triptour_VMTConstants.sql" 
        self.triptour_sql_yesAV = "theme_triptour_VMTConstants.sql" #for now, AV/No AV is using the same trip tour script
        self.cvixxi_sql = "theme_cveh_ixxi.sql"
        self.telework_sql = "theme_telework_VMTConstants.sql"
        
        self.mix_density_sql1 = "mix_density_pt1.sql"
        self.mix_density_sql2 = "mix_density_pt2.sql"
        
        self.comb_sql = "ILUT_combine_tables.sql"
        
        
        # Autonomous Vehicle (AV) and TNC (e.g. Uber/Lyft) assumptions used:
        self.no_av = "No AV"
        self.yes_av = "Yes AV"

        self.avmode_dict = {
            self.no_av:[self.triptour_sql_noAV, self.hh_sql_noAV],
            self.yes_av:[self.triptour_sql_yesAV, self.hh_sql_yesAV]
            }

        if av_tnc_type:
            self.av_tnc_type = av_tnc_type
        else: 
            user_tnc_entry = input(f"Enter '1' ('{self.no_av}') or '2' ('{self.yes_av}'): ")
            av_tnc_lookup = {'1':self.no_av, '2':self.yes_av}

            self.av_tnc_type = av_tnc_lookup[user_tnc_entry]

        # model run folder
        self.model_run_dir = model_run_dir
            
        # scenario year
        self.sc_yr = self.get_cond_user_input(sc_yr, "Enter scenario year: ")
            
        # scenario ID code
        self.sc_code = self.get_cond_user_input(sc_code, "Enter scenario number: ")

        # land use scenario id
        self.land_use_scen = land_use_scen

        # 1/0 flag indicator if run is shared externally (e.g. for EIR, MTP, MTIP amendment, etc.)
        self.shared_ext = int(shared_ext) # convert True/False to 1/0 value

        # additional scenario description
        desc_prompt = "Enter scenario description (255 char limit): "
        self.sc_desc = self.get_cond_user_input(sc_desc, desc_prompt)

        self.scenario_extn = f"{self.sc_yr}_{self.sc_code}_{self.land_use_scen}"

        # Tables in model run folder used in ILUT
        self.raw_parcel = "raw_parcel{}".format(self.scenario_extn)
        self.raw_hh = "raw_hh{}".format(self.scenario_extn)
        self.raw_person = "raw_person{}".format(self.scenario_extn)
        self.raw_personday = "raw_personday{}".format(self.scenario_extn)
        self.raw_ixxi = "raw_ixxi{}".format(self.scenario_extn)
        self.raw_cveh = "raw_cveh{}".format(self.scenario_extn)
        self.raw_ixworkerfraxn = "raw_ixworker{}".format(self.scenario_extn)
        self.raw_tour = "raw_tour{}".format(self.scenario_extn)
        self.raw_trip = "raw_trip{}".format(self.scenario_extn)

    def get_cond_user_input(self, in_val, prompt):
        # if in_val is provided in __init__, then use it. Otherwise ask user for input.
        if in_val:
            output = in_val
        else:
            output = input(prompt)
        
        return output
        
    def update_input_tbl(self, tblname, tbl_desc):
        """Checks if table in database, then if not in db, has user manually specify table name."""
        if tblname:
            if self.check_if_table_exists(tblname):
                out_tblname = tblname # if user specified a table, and the table is in the db, then all good and you can move on to check next table
            else: # if the user-specified table isn't found, let them know and give a chance to re-enter manually.
                err_msg = f"Table {tblname} not found in database {self.database}. " \
                                            "Please double check name and spelling."
                raise Exception(err_msg)
        else: # if a table wasn't specified ahead of time, have user specify it.
            out_tblname = self.conditional_table_entry(f"Specify table you are using for {tbl_desc} or press ctrl+c to exit")
        
        return out_tblname
    
    def pop_parcel_qa(self):
        AddMessage("Running QA checks on parcel, person, and population tables...")

        cursor = self.conn.cursor()

        sql_pclcnt_raw = f"""SELECT COUNT(*) FROM {self.raw_parcel}"""
        sql_pclcnt_eto = f"""SELECT COUNT(*) FROM {self.envision_tomorrow_tbl}"""
        sql_pclcnt_master = f"""SELECT COUNT(*) FROM {self.parcel_master_tbl}"""
        sql_pcl_joincheck = f"""SELECT COUNT(*) 
            FROM {self.parcel_master_tbl} pm
                JOIN {self.raw_parcel} raw
                    ON pm.parcelid = raw.parcelid
                JOIN {self.envision_tomorrow_tbl} eto
                    ON raw.parcelid = eto.parcelid
        """

        sql_popcnt_raw = f"""SELECT COUNT(*) FROM {self.raw_person}"""
        sql_popcnt_poptbl = f"""SELECT COUNT(*) FROM {self.pop_table}"""
        sql_popcnt_join = f"""SELECT COUNT(*) as popcheck
            FROM {self.pop_table} pop
                JOIN {self.raw_person} per
                    ON pop.serialno = per.hhno
                        AND pop.pnum = per.pno
            """

        sql_poppcl_check = f"""SELECT COUNT(*)
        FROM {self.pop_table} pop
            JOIN {self.parcel_master_tbl} pm
                ON pop.hhcel = pm.parcelid
        """

        results_parcels = {
            f"Rows in {self.raw_parcel}": sql_pclcnt_raw,
            f"Rows in {self.envision_tomorrow_tbl}": sql_pclcnt_eto,
            f"Rows in {self.parcel_master_tbl}": sql_pclcnt_master,
            f"Rows after joining {self.raw_parcel}, {self.envision_tomorrow_tbl}, and {self.parcel_master_tbl}": sql_pcl_joincheck
        }

        results_pop = {
            f"Rows in {self.raw_person}": sql_popcnt_raw,
            f"Rows in {self.pop_table}": sql_popcnt_poptbl,
            f"Rows after joining {self.pop_table} and {self.raw_person}": sql_popcnt_join,
            f"Rows after joining {self.pop_table} and {self.parcel_master_tbl}": sql_poppcl_check
        }

        for d in [results_parcels, results_pop]:

            for k, sql in d.items():
                cursor.execute(sql)
                result = cursor.fetchall()[0][0]
                d[k] = result

            if max(d.values()) != min(d.values()):
                AddMessage("WARNING: Mismatch in tables. See topline summary of differences:")
                for k, v in d.items():
                    AddMessage(f"\t{k}: {v}")
                AddMessage("\n This mismatch may result in incorrect outputs in final table!\n")

        cursor.close()

    def check_if_table_exists(self, table_name):
        '''Returns true/false value of whether a given table exists in database'''
        cursor = self.conn.cursor()
        tables = [i.table_name for i in cursor.tables()]
        cursor.close()

        return table_name in tables

    def check_table_sizes(self):
        """Check to make sure that there's consistency in table versions.
        E.g., parcels in raw_parcel should match parcels in ETO and in parcel_master"""
        pass

    def shared_externally(self):
        '''
        Checks if the indicated year and scenario ID correspond to an existing run that was
        shared publicly (e.g. MTIP, MTP, EIR run). If it is shared, do not let the user overwrite
        the table.
        '''
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        
        sql = f"""
            SELECT * FROM {self.scen_log_tbl}
            WHERE scenario_year = {self.sc_yr}
                AND scenario_code = {self.sc_code}
                AND table_status = 'created'
            """
        
        cursor.execute(sql)
        results = cursor.fetchall()

        if len(results) > 0:
            fields = [i[0] for i in cursor.description]
            record = dict(zip(fields, results[0]))
            share_flag = record['shared_externally']

            output = True if share_flag == 1 else False
        else:
            output = False

        cursor.close()   

        return output
    
    
    def run_sql(self, sql_file, params_list):
        '''Runs SQL file. params_list contains any formatters used in the SQL 
        file (e.g. to specify which table names to use in the SQL command).'''
        
        AddMessage("Running {}...".format(sql_file))
        self.conn.autocommit = True
        with open(os.path.join(self.sql_dir, sql_file),'r') as in_sql:
            raw_sql = in_sql.read()

            if type(params_list) is dict:
                formatted_sql = raw_sql.format(**params_list)
            else:
                formatted_sql = raw_sql.format(*params_list)
            cursor = self.conn.cursor()
            cursor.execute(formatted_sql)
            cursor.commit()
            cursor.close()
        
    def get_unc_path(self, in_path):
    
        # based on a network drive path, convert the letter to full machine name
        unc_path = str(Path(in_path).resolve())
        
        # if the model run folder is on the machine that this script is getting run on,
        # the full machine name path must be manually built.

        if unc_path[:2] != r"\\":
            import socket
            machine = socket.gethostname()
            drive_letter = os.path.splitdrive(in_path)[0].strip(':')
            folderpath = os.path.splitdrive(in_path)[1]
            unc_path = f"\\\\{machine}\\{drive_letter}{folderpath}"
        
        return unc_path
        
    def log_run(self, av_tnc_flag):
        '''
        Logs information about the ILUT run performed, including scenario year,
        scenario ID, text description and notes of scenario, when the ILUT for the scenario was run,
        and what AV/TNC settings were used in the run.
        '''
        
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        self.username = os.getlogin()
        
        sc_desc_fmt = r'{}'.format(self.sc_desc) #gets rid of pesky unicode escape errors if description has \N, \t, etc.
        default_tbl_status = "created"
        run_folder = self.get_unc_path(self.model_run_dir)
        
        sql = f"""
            INSERT INTO {self.scen_log_tbl} VALUES (
            {self.sc_yr}, {self.sc_code}, '{sc_desc_fmt}', GETDATE(), 
            '{av_tnc_flag}', '{default_tbl_status}', '{run_folder}',
            {self.shared_ext}, '{self.username}', '{self.land_use_scen}')
            """
        
        cursor.execute(sql)
        cursor.commit()
        cursor.close()
        
    def conditional_table_entry(self, input_prompt):
        '''If user needs to specify a table that already exists, this makes sure
        they specify a valid table name'''
        while True:
            tbl_name = input(f"{input_prompt}: ")
            valid_tbl_name = self.check_if_table_exists(tbl_name)
            if valid_tbl_name:
                AddMessage(tbl_name)
                break
            else:
                AddMessage("Table {} does not exist. Please try a different table name." \
                      .format(tbl_name))
                continue
        
        return tbl_name

    def delete_tables(self, tables_to_delete):
        self.conn.autocommit = True

        for table in tables_to_delete:
            cursor = self.conn.cursor()

            sql = f"DROP TABLE {table}"
            cursor.execute(sql)

            cursor.commit()
            cursor.close()
    
    def run_report(self, create_triptour_table=True,
                    create_person_table=True,
                    create_hh_table=True,
                    create_cvixxi_table=True,
                    create_telewk_table=True,
                    create_comb_table=True,
                    delete_input_tables=True):
        
        '''Runs queries to generate parcel-level ILUT table.'''
        
        start_time = time.time()
        cursor = self.conn.cursor()
        
        # these tables are temporary and will be deleted after script finishes.
        triptour_outtbl = "TEMP_ilut_triptour{}".format(self.scenario_extn)
        person_outtbl = "TEMP_ilut_person{}".format(self.scenario_extn)
        hh_outtbl = "TEMP_ilut_hh{}".format(self.scenario_extn)
        cvixxi_outtbl = "TEMP_ilut_ixxicveh{}".format(self.scenario_extn)
        telewk_outtbl = "TEMP_tw_x_pcl{}".format(self.scenario_extn)
        
        # create trip-tour theme table
        if create_triptour_table:
            triptour_sql = self.avmode_dict[self.av_tnc_type][0]
            triptour_params = dict(raw_trip=self.raw_trip, raw_tour=self.raw_tour, raw_hh=self.raw_hh, 
                                    raw_person=self.raw_person, raw_parcel=self.raw_parcel, 
                                    raw_ixworkerfraxn=self.raw_ixworkerfraxn, triptour_outtbl=triptour_outtbl)

            self.run_sql(triptour_sql, triptour_params) 
            
        #Create person theme table
        if create_person_table:
            person_params = dict(pop_table=self.pop_table, raw_person=self.raw_person, 
                                raw_parcel=self.raw_parcel, person_outtbl=person_outtbl)
            self.run_sql(self.person_sql, person_params) 
        
        #create hh theme table
        if create_hh_table:
            hh_sql = self.avmode_dict[self.av_tnc_type][1]
            hh_params = dict(pop_table=self.pop_table, raw_hh=self.raw_hh, 
                            raw_parcel=self.raw_parcel, hh_outtbl=hh_outtbl)
            self.run_sql(hh_sql,hh_params)
            
        # create comm veh ixxi table
        if create_cvixxi_table:
            cvixxi_params = dict(raw_parcel=self.raw_parcel, raw_cveh=self.raw_cveh, 
                                taz_rad_table=self.taz_rad_table, 
                                raw_hh=self.raw_hh, raw_ixxi=self.raw_ixxi,
                                cvixxi_outtbl=cvixxi_outtbl)
            self.run_sql(self.cvixxi_sql, cvixxi_params)

        # create telework data table
        if create_telewk_table:
            telework_params = dict(raw_trip=self.raw_trip, raw_personday=self.raw_personday,
                                    raw_person=self.raw_person, raw_hh=self.raw_hh,
                                    telewk_outtbl=telewk_outtbl)
            self.run_sql(self.telework_sql, telework_params)        

        tables_for_combining = [triptour_outtbl, person_outtbl, hh_outtbl, cvixxi_outtbl, telewk_outtbl]
        tables_existing = [cursor.tables(table=t).fetchone()[2] for t in tables_for_combining \
                           if cursor.tables(table=t).fetchone() is not None]

        
        if create_comb_table:
            if len(tables_existing) == len(tables_for_combining): #check that all ilut tables exist before creating combo table
                col_str_yr = str(self.sc_yr)[-2:] #for columns in ETO table with year suffix in header name
                self.comb_outtbl = f"ilut_combined{self.scenario_extn}"
                self.comb_outtbl = self.comb_outtbl.replace('_latest', '') # clean up name of final output table

                comb_params = dict(parcel_master=self.parcel_master_tbl, envision_tomorrow_tbl=self.envision_tomorrow_tbl, 
                                raw_parcel=self.raw_parcel, hh_outtbl=hh_outtbl, person_outtbl=person_outtbl, 
                                triptour_outtbl=triptour_outtbl, cvixxi_outtbl=cvixxi_outtbl, 
                                comb_outtbl=self.comb_outtbl, col_str_yr=col_str_yr, telewk_outtbl=telewk_outtbl)
                
                self.run_sql(self.mix_density_sql1,[self.raw_parcel]) #calculate mixed-density column on parcel file
                self.run_sql(self.mix_density_sql2,[self.raw_parcel]) #calculate mixed-density column on parcel file
                self.run_sql(self.comb_sql, comb_params) #run script to combine all theme tables
                
                # av_tnc_desc = self.avmode_dict[self.av_tnc_type][0]
                self.log_run(self.av_tnc_type)
            else:
                AddMessage("Not all input ILUT tables exist. Make sure all theme ILUT tables exist then re-run.")
                sys.exit()

            if delete_input_tables:
                input_tables = [self.raw_parcel, self.raw_hh, self.raw_person, self.raw_personday, self.raw_ixxi, 
                                self.raw_cveh, self.raw_ixworkerfraxn, self.raw_tour, self.raw_trip]
                self.delete_tables(input_tables)
        
        cursor.close()
        elapsed_time = round((time.time() - start_time)/60,1)
        AddMessage("Success! Elapsed time: {} minutes".format(elapsed_time))

    def topline_summary(self, list_topline_fields, sql_agg_op='SUM'):
        try:
            field_clauses = ', '.join([f"{sql_agg_op}({i}) as {sql_agg_op}_{i}" \
                                        for i in list_topline_fields])
            sql_topline = f"""
                SELECT {field_clauses}
                FROM {self.comb_outtbl}
                """
            
            self.conn.autocommit = True
            cursor = self.conn.cursor()
            cursor.execute(sql_topline)
            results = cursor.fetchall()

            AddMessage("\n-----TOPLINE SUMMARY---------")
            for i, fname in enumerate(list_topline_fields):
                value = results[0][i]
                if value >= 1000: # make comma-separated if >=1,000, otherwise give 2 decimal places
                    value = int(round(value, 0))
                    value_f = format(value, ',d')
                else:
                    value_f = round(value, 2) 
                AddMessage(f"{sql_agg_op}_{fname}: {value_f}")

            cursor.close()
        except NameError:
            AddMessage(f"You did not create a combined output ILUT table. You must create one to generate a topline summary.")

#=========================SCRIPT ENTRY POINT===================================


if __name__ == '__main__':

    model_run_folder = r'D:\SACSIM19\MTP2020\2016_UpdatedAug2020\run_2016_baseline_AO13_V7_NetUpdate08202020'
    scenario_year = 2016
    scenario_id = 9998

    run_ilut_combine = 'true'
    remove_input_tables = 'true'
    shared_externally = 'false'

    comb_rpt = ILUTReport(model_run_dir=model_run_folder, dbname='MTP2024', sc_yr=scenario_year, 
                            sc_code=scenario_id, envision_tomorrow_tbl='raw_eto2016_latest',
                            pop_table='raw_pop2016_latest', taz_rad_tbl='TAZ21_RAD07',
                            av_tnc_type='No AV', sc_desc='test', shared_ext=False)

    comb_rpt.run_report()
    comb_rpt.topline_summary(["PT_TOT_RES", "VT_TOT_RES", "VMT_TOT_RES"])





# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 15:41:46 2018

@author: dconly

Purposes:
    Clean out old ILUT tables, including intermediate tables, raw model output
    tables.
    
"""

import os 
import pyodbc

    
def delete_old_tables(**kwargs):

    #=============================top_parameters

    db_conninfo = f"DRIVER={kwargs['db_driver']}; SERVER={kwargs['server']};" \
        f"DATABASE={kwargs['db_database']}; Trusted_Connection={kwargs['db_trusted_connection']}"

    pref_ilut_comb = 'ilut_combined' # prefix for ilut combined table

    table_status_delall = "deleted all tables"
    table_status_keepcomb = "deleted raw/theme tables, kept combined table"

    # prefixes for all of the tables you'll delete. Table naming convention is <prefix><year>_<id>
    table_prefixes = ['raw_person','raw_hh','raw_parcel','raw_trip','raw_tour',
                'raw_ixxi','raw_cveh','raw_ixworker','ilut_triptour',
                'ilut_person','ilut_hh','ilut_ixxicveh', pref_ilut_comb]

    #========================== subfunctions within function

    def run_sql(sql_file, params_list):
        conn = pyodbc.connect(db_conninfo)
        conn.autocommit = True
        with open(sql_file,'r') as in_sql:
            raw_sql = in_sql.read()
            formatted_sql = raw_sql.format(*params_list)
            #print(formatted_sql) #uncomment to see query
            cursor = conn.cursor()
            cursor.execute(formatted_sql)
            # =============================================================================
                    # use this to get rows of data if needed
            #         rows = cursor.fetchall()
            #         for row in rows:
            #             print(row)
            # =============================================================================
            cursor.commit()
            cursor.close()
        conn.close()


    def shared_externally():
        '''
        Checks if the indicated year and scenario ID correspond to an existing run that was
        shared publicly (e.g. MTIP, MTP, EIR run). If it is shared, do not let the user overwrite
        the table.
        '''
        
        conn = pyodbc.connect(db_conninfo)
        conn.autocommit = True
        cursor = conn.cursor()
        
        sql = f"""
            SELECT * FROM {kwargs['scenario_log_tbl']}
            WHERE {kwargs['logtbl_yearfld']} = {kwargs['scenario_year']}  
                AND {kwargs['logtbl_id_field']} = {kwargs['scenario_id']}  
                AND {kwargs['logtbl_statfld']} = 'created'
                AND {kwargs['logtbl_sharefld']} = 1
            """
    
        cursor.execute(sql)
        results = cursor.fetchall()

        if len(results) > 0:
            output = True # if share_flag == 1 else False
        else:
            output = False

        cursor.close()
        conn.close()       

        return output

    def update_table_status(status_value):
        conn = pyodbc.connect(db_conninfo)
        conn.autocommit = True
        cursor = conn.cursor()

        
        sql = f"UPDATE {kwargs['scenario_log_tbl']} SET {kwargs['logtbl_statfld']} = '{status_value}' " \
            f"WHERE {kwargs['logtbl_yearfld']} = {kwargs['scenario_year']} " \
            f"AND {kwargs['logtbl_id_field']} = {kwargs['scenario_id']}" 
        
        cursor.execute(sql)
        
        cursor.commit()
        cursor.close()
        conn.close()

    #========================== rest of function

    is_shared_externally = shared_externally()

    if is_shared_externally:
        msg = f"ILUT for year {kwargs['scenario_year']} with ID {kwargs['scenario_id']} is an externally-shared run " \
            "and cannot be removed with this script. Removal can only happen manually in SQL Server."
        print(msg)
        pass

    else:
        #option to include combined output tables.

        if kwargs['drop_combined_yn'].lower() == 'y':
            tables = [f"{prefix}{kwargs['scenario_year']}_{kwargs['scenario_id']}" for prefix in table_prefixes]
            update_table_status(table_status_delall)
        elif kwargs['drop_combined_yn'].lower() == 'n':
            tbl_prefixes_nocomb = ['na' if prefix == pref_ilut_comb \
                                else prefix for prefix in table_prefixes]

            tables = [f"{prefix}{kwargs['scenario_year']}_{kwargs['scenario_id']}" for prefix in tbl_prefixes_nocomb]
            update_table_status(table_status_keepcomb)
        else:
            quit()
    
        run_sql(kwargs['drop_tbl_sql'], tables)
        print("Deleted tables for year {}, scenario {}".format(kwargs['scenario_year'], kwargs['scenario_id']))
    
    
#============================MAIN SCRIPT=======================================


if __name__ == '__main__':
    #==========NORMAL INPUT PARAMETERS=====================
    database = 'MTP2024'

    year = 2016 # year you want to delete select tables from 

    # list of scenario IDs from within year that you want to delete
    scenario_ids = [9995]

    #============PARAMETERS NOT CHANGED VERY OFTEN===========
    
    #sql script path
    os.chdir(os.path.dirname(__file__))

    drop_comb = input(f"Deleting year {year}, scenarios {scenario_ids}. Also drop combined output tables too (y/n)? ")
    
    for scen_id in scenario_ids:
        delete_table_argdict = dict(
                scenario_year = year,
                scenario_id = scen_id,
                drop_combined_yn = drop_comb,
                db_database = database,
                db_driver = '{SQL Server}',
                server = 'SQL-SVR',
                db_trusted_connection = 'yes',
                scenario_log_tbl = "ilut_scenario_log",
                drop_tbl_sql = 'delete_old_tablesSQL.sql',
                logtbl_statfld = 'table_status',
                logtbl_yearfld = 'scenario_year',
                logtbl_id_field = 'scenario_code',
                logtbl_sharefld = 'shared_externally'
                )      


        delete_old_tables(**delete_table_argdict) 
        
    print("--"*20)
    print("Finished. Note that raw population and Envision Tomorrow parcel files will require manual removal.")


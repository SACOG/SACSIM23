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



def run_sql(sql_file,params_list):
    conn = pyodbc.connect(conxn_info)
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

def shared_externally(scenario_year, scenario_code):
    '''
    Checks if the indicated year and scenario ID correspond to an existing run that was
    shared publicly (e.g. MTIP, MTP, EIR run). If it is shared, do not let the user overwrite
    the table.
    '''
    
    conn = pyodbc.connect(conxn_info)
    conn.autocommit = True
    cursor = conn.cursor()
    
    sql = f"""
        SELECT * FROM {scen_log_tbl}
        WHERE scenario_year = {scenario_year}
            AND scenario_code = {scenario_code}
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
    conn.close()       

    return output
    
def update_table_status(log_table, status_column, status_value, year, sc_id):
    conn = pyodbc.connect(conxn_info)
    conn.autocommit = True
    cursor = conn.cursor()
        
    year_column = 'scenario_year'
    sc_id_col = 'scenario_code'
    
    sql = "UPDATE {} SET {} = '{}' WHERE {} = {} AND {} = {}" \
            .format(log_table, status_column, status_value, year_column, year, sc_id_col, sc_id)
    
    cursor.execute(sql)
    
    cursor.commit()
    cursor.close()
    conn.close()
    
def delete_old_tables(sc_yr,sc_id,drop_comb):

    is_shared_externally = shared_externally(sc_yr, sc_id)

    if is_shared_externally:
        msg = f"ILUT for year {sc_yr} with ID {sc_id} is an externally-shared run and cannot be removed with this script. Removal" \
            " can only happen manually in SQL Server."
        print(msg)
        pass

    else:
        #option to include combined output tables.
        table_prefixes = ['raw_person','raw_hh','raw_parcel','raw_trip','raw_tour',
                        'raw_ixxi','raw_cveh','raw_ixworker','ilut_triptour',
                        'ilut_person','ilut_hh','ilut_ixxicveh','ilut_combined']

        if drop_comb.lower() == 'y':
            tables = ["{}{}_{}".format(prefix,sc_yr,sc_id) for prefix in table_prefixes]
            update_table_status(scen_log_tbl, 'table_status', table_status_delall, sc_yr, sc_id)
        elif drop_comb.lower() == 'n':
            tbl_prefixes_nocomb = ['na' if prefix == 'ilut_combined' \
                                else prefix for prefix in table_prefixes]
            tables = ["{}{}_{}".format(prefix,sc_yr,sc_id) \
                    for prefix in tbl_prefixes_nocomb]
            update_table_status(scen_log_tbl, 'table_status', table_status_keepcomb, sc_yr, sc_id)
        else:
            quit()
    
        run_sql(drop_table_sql,tables)
        print("Deleted tables for year {}, scenario {}".format(sc_yr, sc_id))
    
    
#============================MAIN SCRIPT=======================================


if __name__ == '__main__':
    # Database connection
    driver = '{SQL Server}'
    server = 'SQL-SVR'
    database = 'MTP2024'
    trusted_connection = 'yes'
    conxn_info = "DRIVER={0}; SERVER={1}; DATABASE={2}; Trusted_Connection={3}".format(driver, server, database, trusted_connection)
    
    scen_log_tbl = "ilut_scenario_log" #logs each run made and asks user for scenario description
    table_status_delall = "deleted all tables"
    table_status_keepcomb = "deleted raw/theme tables, kept combined table"
    
    #sql script directory
    os.chdir(os.path.dirname(__file__))
    
    drop_table_sql = 'delete_old_tablesSQL.sql'
    
    year = 2035
    scenario_ids = [9993]
    
    drop_comb = input("Deleting year {}, scenarios {}. Also drop combined output tables too (y/n)? ".format(year,scenario_ids))
    
    
    for sc_id in scenario_ids:
        delete_old_tables(year,sc_id,drop_comb) 
        
    print("--"*20)
    print("Finished. Note that raw population and Envision Tomorrow parcel files will require manual removal.")


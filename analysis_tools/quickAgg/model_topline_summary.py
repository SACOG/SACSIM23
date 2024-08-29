"""
Name:model_topline_summary.py
Purpose: Get topline, region-totals summary of some of the most common metrics we like to pull
    from SACOG's Integrated Land Use-Transportation (ILUT) process. ILUT is great, but takes 10-20mins to set up and run,
    and has dependencies like SQL Server. this aims to be an alternative to make the process faster and easier.

    OUTPUTS
        o	Total trips by mode
    o	Trip share by mode
    o	Total person trips
    o	Residential VMT
    o	VMT/capita
    o	Population

        
          
Author: Darren Conly
Last Updated: July 2023
Updated by: KS
Copyright:   (c) SACOG
Python Version: 3.x
"""

import os
from pathlib import Path
import csv
import datetime

import arcpy
import pandas as pd
from dbfread import DBF

from pandas_memory_optimization import memory_optimization
from get_unc_path import build_unc_path

def trace_error():
    import sys, traceback, inspect
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # script name + line number
    line = tbinfo.split(", ")[1]
    filename = inspect.getfile(inspect.currentframe())
    # Get Python syntax error
    synerror = traceback.format_exc().splitlines()[-1]
    return f"{line}, {filename}, {synerror}" #return line number, name of file with error line, and type of error

class modelRunSummary:
    def __init__(self, model_run_dir, road_vmt_dbf=None, scenario_desc=""):
        
        self.model_run_dir = model_run_dir
        self.road_vmt_dbf = road_vmt_dbf
        self.scenario_year = self.get_year_from_basenet()
        self.scenario_desc = scenario_desc

        # trip table attributes
        self.in_trip_file = '_trip_1_1.csv' #trip table name
        self.in_trip_tsv = '_trip.tsv'
        self.trnsummary = 'line_summary.dbf'
        self.c_pno = 'pno'
        self.c_hhno = 'hhno'
        self.c_mode = 'mode'
        self.c_dorp = 'dorp'
        self.c_distau = 'distau'
        self.c_distcong = 'distcong'

        self.tripcols = [self.c_hhno, self.c_mode, self.c_dorp] #specify columns you want to import. This will save memory.
        self.vmt_cols = [self.c_distau, self.c_distcong]
        
        if os.path.exists(os.path.join(self.model_run_dir, self.in_trip_file)):
            self.tripcols = self.tripcols + self.vmt_cols
            self.df_trip = self.load_table(self.in_trip_file, self.tripcols)
        else:
            arcpy.AddMessage(f"WARNING: {self.in_trip_file} not found. Loading from {self.in_trip_tsv} instead, which will" \
                  " lack resident VMT data.")
            self.df_trip = self.load_table(self.in_trip_tsv, self.tripcols, delim_char='\t')
            for f in self.vmt_cols:
                self.df_trip[f] = 0 # adding vmt fields even if loading from TSV, but filling in as zero
        

        # hh table attributes
        self.in_hh_file = '_household.tsv'
        self.c_hhsize = 'hhsize'
        self.hhcols = [self.c_hhno, self.c_hhsize]

        # pop table attributes
        self.in_person_file = '_person.tsv'

    def load_table(self, in_table, use_cols, delim_char=','):
        tbl_path = os.path.join(self.model_run_dir, in_table)
        arcpy.AddMessage(f"reading {tbl_path} into dataframe...")

        try: # if pandas version supports it, reduce load time by ~40% using pyarrow engine
            df = pd.read_csv(tbl_path, usecols=use_cols, delimiter=delim_char, engine='pyarrow')
        except:
            print("Unable to load with pyarrow engine; consider update to pandas > 1.4.0 to reduce load time by ~40%.")
            df = pd.read_csv(tbl_path, usecols=use_cols, delimiter=delim_char)

        memory_optimization(df)
        return df
    
    def dbf2df(self, dbf_path, fields_to_load):
        # maybe faster, and more memory-efficient, way to load big DBF into pandas dataframe
        dbf_obj = DBF(dbf_path)

        data_rows = []
        for row in dbf_obj:
            row2append = {}
            for fname, fval in row.items():
                if fname in fields_to_load: row2append[fname] = fval
            data_rows.append(row2append)
        
        df_out = pd.DataFrame(data_rows)

        return df_out

    def get_year_from_basenet(self):
        # get scenario year based on latest version of base network inside folder
        try:
            fp = Path(self.model_run_dir)
            basenets = {f.stat().st_ctime: f  for f in fp.glob('*_base.net')} # list of all PRNs in folder
            last_ctime = max(basenets.keys()) # time of last created PRN
            last_prn = basenets[last_ctime]
            sc_year = last_prn.name[:4]
        except:
            arcpy.AddWarning("Warning: could not infer scenario year. Confirm that model run folder path is correct.")
            sc_year = 'UNKNOWN'
        
        return sc_year
    
    def get_total_pop(self, header_row_cnt=1):
        
        person_file_path = os.path.join(self.model_run_dir, self.in_person_file)
        arcpy.AddMessage(f"counting pop from rows in {person_file_path}...")

        tot_pop = 0
        with open(person_file_path, 'r') as f:
            rows = f.readlines()
            for row in rows:
                tot_pop += 1
        
        return tot_pop - header_row_cnt

    def calc_res_vmt_paxcnt(self, vmt_col):
        """
        'Normal' way of calculating total residential VMT: assumes VMT is halved for HOV2 trips,
        and a bit more than 70% reduced for HOV3+ trips.
        """

        mgrp = self.df_trip.groupby('mode')
        sumxmode = mgrp.sum()[vmt_col] #get sum of vmt grouped by mode
        sov_vmt =  sumxmode[3]  #sov vmt, or row where mode = 3, or sov
        hov2_vmt = sumxmode[4] * 0.5 #vmt/person-trip is half for 2-person carpool
        hov3plus_vmt = sumxmode[5] * 0.3 #averaging that 3+ carpool has 0.3 times the per-person vmt
        total_vmt = sov_vmt + hov2_vmt + hov3plus_vmt
        return total_vmt


    def calc_res_vmt_dorp(self, vmt_col):
        """
        'DORP' (driver or passenger) way of calculating total residential VMT: 
        Only count the VMT if the DORP flag = 1, indicating the trip maker is the driver.
        In theory this is a better way of estimating actual vehicle trips.

        NOTE - As of 1/4/2022, this method is not being used.
        """
        total_vmt = self.df_trip.loc[self.df_trip[self.c_dorp] == 1][vmt_col] \
                    .sum()

        return total_vmt

    def calc_hh_pop(self):
        hh_file_path = os.path.join(self.model_run_dir, self.in_hh_file)
        arcpy.AddMessage(f"summing hh sizes from rows in {hh_file_path}...")

        tot_hh_pop = 0
        with open(hh_file_path, 'r') as f:
            d_reader = csv.DictReader(f, delimiter='\t')
            for d in d_reader:
                hhsize = int(d[self.c_hhsize])
                tot_hh_pop += hhsize
        
        return tot_hh_pop

    def get_trips_x_mode(self, mode_val):
        mode_trips = self.df_trip.loc[self.df_trip[self.c_mode] == mode_val].shape[0]
        return mode_trips

    def keep_field_as_dummy(df, fname, source_file, source_fields):
        """If a field specified for dataframe is not in the source file (reflist
        is list of fields in the source file), then make it a 'dummy' field with value zero
        and add a warning."""
        if fname not in source_fields:
            arcpy.AddMessage(f"Warning, field {fname} not in {source_file}. Adding as zero-value field...")
            df[fname] = 0

    def get_trn_data(self):
        self.trnsummary_path = os.path.join(self.model_run_dir, self.trnsummary)
        self.fld_revhr = 'REVENUHRS'

        trnfields = [self.fld_revhr]

        df = self.dbf2df(self.trnsummary_path, fields_to_load=trnfields)

        resultdict = {}
        for fn in trnfields:
            resultdict[f"TRN_{fn}"] = df[fn].sum()

        return resultdict

    def get_road_vmt(self):
        """Get total roadway VMT and CVMT
        Args:
            road_vmt_dbf (DBF): DBF file of model links with daynet data
        """
        arcpy.AddMessage(f"reading in roadway VMT data from {self.road_vmt_dbf}...")
        self.fld_day_vmt = 'DAYVMT'
        self.f_capclass = 'CAPCLASS'
        self.fld_day_cvmt = 'DAYCVMT'
        self.fld_lanemi = 'LANEMI'
        self.fld_a = 'A'
        self.fld_b = 'B'
        link_attr_fields = [self.fld_b, self.fld_a, self.f_capclass]
        initial_val_fields = [self.fld_day_vmt, self.fld_day_cvmt, self.fld_lanemi] # initial set of daynet values to try and summarize
        w_gateway_val_fields = [self.fld_day_vmt, self.fld_day_cvmt]
        
        # put in default vals for result_dict--at least want the fields to show up in output table
        result_dict = {}
        for f in w_gateway_val_fields:
            result_dict[f"{f}_tot"] = -1
            result_dict[f"{f}_gtwy"] = -1
            result_dict[f"{f}_nongtwy"] = -1

        if len(self.road_vmt_dbf) > 1:
            # if there is a DBF for daynet data, read it.
            self.vmt_dbf_path = os.path.join(self.model_run_dir, self.road_vmt_dbf) 
            dbf_obj = DBF(self.vmt_dbf_path) # set up dbf object
            dbf_fields = dbf_obj.field_names # get field names of raw DBF file
            final_val_fields = [] # identify which fields you actually need to load
            for fname in initial_val_fields: 
                if fname in dbf_fields: final_val_fields.append(fname)

            # load data from appropriate fields
            all_fields_to_load = link_attr_fields + final_val_fields
            df = self.dbf2df(self.vmt_dbf_path, fields_to_load=all_fields_to_load)
            df_gateways = df.loc[(df[self.fld_a] <= 30) | (df[self.fld_b] <= 30)].copy()

            # then for fields actually in daynet, update with correct sum values
            for f in w_gateway_val_fields:
                result_dict[f"{f}_tot"] = df[f].sum()
                result_dict[f"{f}_gtwy"] = df_gateways[f].sum()
                result_dict[f"{f}_nongtwy"] = result_dict[f"{f}_tot"] - result_dict[f"{f}_gtwy"]

            # compute lane-miles for real roads
            exclude_capcs = [7, 61, 62, 63, 99] # exclude bike paths, centroid connectors, LRT links, PNR connectors, and disabled links
            car_lanemi = df.loc[~df[self.f_capclass].isin(exclude_capcs)][self.fld_lanemi].sum()
            result_dict['lanemi_motveh'] = car_lanemi

        return result_dict

    def get_topline(self):
        disclaimer_msg = "Numbers reported in this summary may vary from those reported bySACOG published documents." \
            "\nEnd user assumes all risk associated with reporting numbers generated in this report."

        tot_pop = self.get_total_pop()
        tot_hh_pop = self.calc_hh_pop()
        tot_restrips = self.df_trip.shape[0]
        tot_vmt_fracmethod = self.calc_res_vmt_paxcnt(self.c_distau)
        vmt_cap_frac = tot_vmt_fracmethod / tot_pop
        
        transit_data = self.get_trn_data()
        roadway_data = self.get_road_vmt()

        modes = {1: "walk", 2:"bike", 3:"sov", 4:"hov2", 5: "hov3", 6: "transit",
                 8: "schoolbus", 9: "tnc"} # note - mode=9 is actually "other" per daysim docs, but is considered TNC
        modenames = [f"{n}_trips" for n in modes.values()]

        trips_x_mode = [self.get_trips_x_mode(mode_id) for mode_id in modes.keys()]

        dict_trips_x_mode = dict(zip(modenames, trips_x_mode))
        
        model_run_uncpath = build_unc_path(self.model_run_dir)

        out_dict = {"DISCLAIMER": disclaimer_msg,
            "model_run_folder": model_run_uncpath,
            "scenario_year": self.scenario_year, 
            "scenario_desc": self.scenario_desc,
            "tot_pop": tot_pop,
            "tot_hh_pop": tot_hh_pop, 
            "tot_vmt_ii": tot_vmt_fracmethod,
            "tot_restrips": tot_restrips, 
            "tot_resvmt_percap": vmt_cap_frac}

        for d in [transit_data, roadway_data, dict_trips_x_mode]:
            out_dict.update(d)
        
        df = pd.DataFrame.from_dict(out_dict, orient='index')

        csv_out = os.path.join(self.model_run_dir, f"{self.scenario_year}_toplinesummary.csv")
        df.to_csv(csv_out)

        return (df, csv_out)






if __name__ == '__main__':
    #=======================USER-DEFINED INPUT PARAMETERS=========================

    # IF RUNNING TOOL FROM PYTHON INTERPRETER, COMMENT THESE VALUES OUT
    in_dir_root = arcpy.GetParameterAsText(0)  
    roadway_data_dbf = arcpy.GetParameterAsText(1)  
    sc_desc = arcpy.GetParameterAsText(2)  


    # UNCOMMENT AND UPDATE THESE VALUES TO RUN FROM PYTHON INTERPRETER
    # in_dir_root = r'\\Win10-model-3\d\SACSIM23\PEP_Testing\Baseline\run_StocktonBRTFullRun'  
    # roadway_data_dbf = r'\\Win10-model-3\d\SACSIM23\PEP_Testing\Baseline\run_StocktonBRTFullRun\2035daynet_pep.dbf' 
    # sc_desc = 'test'

    #=========================WRITE OUT TO CSV===========================
    date_suffix = str(datetime.date.today().strftime('%Y%m%d'))

    # model_run_dir, road_vmt_dbf=None, scenario_year=None, scenario_desc="")
    sumobj = modelRunSummary(in_dir_root, road_vmt_dbf=roadway_data_dbf, scenario_desc=sc_desc)
    result = sumobj.get_topline()
    df = result[0]
    print(df)

    arcpy.SetParameterAsText(3, result[1]) # COMMENT OUT IF RUNNING FROM INTERPRETER
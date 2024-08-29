#--------------------------------
# Name: Project_Screening_main.py
# Purpose: Select Projects for 2025 Blueprint Pathways.
#       After project screening build input and output model network comparisons for GIS
#           at link level and project level
#
#
# Author: Kyle Shipley & Eason Zhang
# Created: 4/18/2023
# Update: 5/2/2023
# Copyright:   (c) SACOG
# ArcGIS Version:   Pro
# Python Version:   3.7
#--------------------------------
import os
import pandas as pd
import numpy as np
from dbfread import DBF
#import geopandas as gpd
import arcpy
from arcgis.features import GeoAccessor, GeoSeriesAccessor
arcpy.env.overwriteOutput = True


def L_Calcs(net):
    # calculate link level model metrics
    net['A_B'] = net['A'].astype(str) + '_' + net['B'].astype(str)
    net['SACTRAK'].str.strip()
    net['PHASE2'].str.strip()

    net['pjt_id'] = net['SACTRAK'].astype(str) + '__' + net['PHASE2'].astype(str)
    net['pjt_id'] = net['pjt_id'].replace({'__nan': '__'}, regex=True)

    net['A_B'] = net['A_B'].str.strip(' ')
    net['pjt_id'] = net['pjt_id'].str.strip(' ')
    # import pdb; pdb.set_trace()

    net['V4h'] = net['H07V'] + net['H08V'] + net['H16V'] + net['H17V']
    net['VMT4h'] = net['H07VMT'] + net['H08VMT'] + net['H16VMT'] + net['H17VMT']
    net['CVMT4h'] = net['H07CVMT'] + net['H08CVMT'] + net['H16CVMT'] + net['H17CVMT']
    net['VC4h'] = 0.00
    net['VC4h'] = np.where(net['V4h'] != 0,
                           ((net['H07VC'] * net['H07V']) + (net['H08VC'] * net['H08V']) + (net['H16VC'] * net['H16V'])
                            + (net['H17VC'] * net['H17V'])) / net['V4h'], 0)

    net['VC4h_2'] = net['VC4h']
    net['CSPD4h'] = 0.00
    net['CSPD4h'] = np.where(net['V4h'] != 0, (
                (net['H07SPDGRP'] * net['H07V']) + (net['H08SPDGRP'] * net['H08V']) + (net['H16SPDGRP'] * net['H16V'])
                + (net['H17SPDGRP'] * net['H17V'])) / net['V4h'], 0)
    net['CSPD4h_2'] = net['CSPD4h']

    ## Addition variable based on delta_byfy.s script ##
    net['congmapvc'] = np.where(net['A3VC'] >= net['P3VC'], net['A3VC'], net['P3VC'])
    net['cvmt_p'] = np.where(net['DAYVMT'] > 0, net['DAYCVMT'] / net['DAYVMT'], 0)
    # roadway utilization
    # 1=underutilized
    # 2=well utilized
    # 3=overutilized
    net['rd_util'] = 0
    util_conditions = [
        (net['CAPCLASS'].isin([5, 24]) & (net['congmapvc'] < 0.85)),  # utilization = 2
        (net['CAPCLASS'].isin([5, 24]) & (net['congmapvc'] >= 0.85)),  # utilization = 3
        (net['CAPCLASS'].isin([2, 3, 4, 12, 22]) & (net['congmapvc'] < 0.85)),  # utilization = 1
        (net['CAPCLASS'].isin([2, 3, 4, 12, 22]) & (net['congmapvc'] >= 0.85) & (net['congmapvc'] <= 1.1)),
        # utilization = 2
        (net['CAPCLASS'].isin([2, 3, 4, 12, 22]) & (net['congmapvc'] > 1.1)),  # utilization = 3
        (net['CAPCLASS'].isin([1, 6, 16, 26]) & (net['congmapvc'] < 0.9)),  # utilization = 1
        (net['CAPCLASS'].isin([1, 6, 16, 26]) & (net['congmapvc'] >= 0.9) & (net['congmapvc'] <= 1.05)),
        # utilization = 2
        (net['CAPCLASS'].isin([1, 6, 16, 26]) & (net['congmapvc'] > 1.05)),  # utilization = 3
        (net['CAPCLASS'].isin([8, 9, 51, 56]) & (net['congmapvc'] < 0.5)),  # utilization = 1
        (net['CAPCLASS'].isin([8, 9, 51, 56]) & (net['congmapvc'] >= 0.5) & (net['congmapvc'] <= 0.85)),
        # utilization = 2
        (net['CAPCLASS'].isin([8, 9, 51, 56]) & (net['congmapvc'] > 0.85))  # utilization = 3
    ]
    util_values = [2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3]
    net['rd_util'] = np.select(util_conditions, util_values, default=0)

    # validation (crude)
    # The net doesn't have c20dyd yet, comment out for now
    # net['valrat'] = 0
    # net['valrat'] = np.where(net['C20DYD']>0, net['DYV']/net['C20DYD'], 0)

    # congestion levels
    # 1=low
    # 2=mod
    # 3=hi
    net['conglev'] = 1
    cong_conditions = [
        ((net['A3VC'] >= 0.90) & (net['A3VC'] < 1.05)) | ((net['P3VC'] >= 0.90) & (net['P3VC'] < 1.05)),  # cong_lev = 2
        (net['A3VC'] >= 1.05) | (net['P3VC'] >= 1.05)  # cong_lev = 3
    ]
    cong_values = [2, 3]
    net['conglev'] = np.select(cong_conditions, cong_values, default=1)
    return net

#EZ version agg_Calcs, to get the original version, see _Copy script
def agg_Calcs(df):
    # Convert 'LANES' and 'SPEED' columns to numeric data types
    df[['LANES', 'SPEED']] = df[['LANES', 'SPEED']].apply(pd.to_numeric, errors='coerce')

    # Calculate weighted values for each column of interest
    def weighted_values(column_name, weight_column):
        data_times_weight = df[column_name] * df[weight_column]
        weight_where_notnull = df[weight_column] * pd.notnull(df[column_name])
        return data_times_weight, weight_where_notnull

    columns_to_weight = ['LANES', 'SPEED', 'CSPD4h', 'CSPD4h', 'VC4h', 'VC4h',
                         'A3VC', 'P3VC', 'EVVC', 'A3VC', 'P3VC', 'EVVC',
                         'A3S', 'P3S', 'EVS']
    weight_columns = ['DISTANCE', 'DISTANCE', 'DISTANCE', 'VMT4h', 'DISTANCE', 'VMT4h',
                      'A3VMT', 'P3VMT', 'EVVMT', 'DISTANCE', 'DISTANCE', 'DISTANCE',
                      'A3VMT', 'P3VMT', 'EVVMT']
    weighted_col_names = ['LANE_D', 'SPD_D', 'CSPD4h_D', 'CSPD4h_VMT', 'VC4h_D', 'VC4h_VMT',
                          'A3VC_D', 'P3VC_D', 'EVVC_D', 'A3VC_VMT', 'P3VC_VMT', 'EVVC_VMT',
                          'A3S_VMT', 'P3S_VMT', 'EVS_VMT']

    for col, weight_col, weighted_col_name in zip(columns_to_weight, weight_columns, weighted_col_names):
        df[f'{weighted_col_name}_data_times_weight'], df[f'{weighted_col_name}_weight_where_notnull'] = weighted_values(col, weight_col)

    # Group by 'pjt_id' and aggregate sum of relevant columns
    agg_columns =(['DISTANCE', 'LANEMI', 'DAYVMT', 'DAYCVMT', 'VMT4h', 'CVMT4h']
                  + ["A3VMT","MDVMT","P3VMT","EVVMT"]
                  + ["A3CVMT","MDCVMT","P3CVMT","EVCVMT"]
                  + [f'{col}_data_times_weight' for col in weighted_col_names]
                  + [f'{col}_weight_where_notnull' for col in weighted_col_names])
    agg_funcs = {col: 'sum' for col in agg_columns}
    df_pjt_stats = df.groupby('pjt_id', as_index=False).agg(agg_funcs)

    # Calculate weighted averages
    for weighted_col_name in weighted_col_names:
        df_pjt_stats[weighted_col_name] = df_pjt_stats[f'{weighted_col_name}_data_times_weight'] / df_pjt_stats[f'{weighted_col_name}_weight_where_notnull']
        del df_pjt_stats[f'{weighted_col_name}_data_times_weight'], df_pjt_stats[f'{weighted_col_name}_weight_where_notnull']

    return df_pjt_stats

def byfy(by_input, fy_input):
    fields_fy = ['A_B', 'NAME', 'DISTANCE', 'SCREEN', 'RAD', 'HWYSEG', 'TRAV_DIR', 'FWYID', 'COUNTID', 'SACTRAK',
                 'PHASE2',
                 'LANES', 'CAPCLASS', 'SPEED', 'BIKE', 'LANEMI', 'A3V', 'MDV', 'P3V', 'A3S', 'P3S', 'EVS', 'DYV',
                 'DAYVMT', 'DAYCVMT', 'A3VC', 'P3VC',
                 'pjt_id', 'V4h', 'VMT4h', 'CVMT4h', 'VC4h', 'CSPD4h', 'congmapvc', 'cvmt_p', 'rd_util', 'conglev']
    # 'C20DYD' excluded temporally
    fields_by = ['A_B', 'LANES', 'CAPCLASS', 'SPEED', 'BIKE', 'LANEMI', 'A3V', 'MDV', 'P3V', 'A3S', 'P3S', 'EVS', 'DYV',
                 'DAYVMT', 'DAYCVMT', 'A3VC', 'P3VC',
                 'V4h', 'VMT4h', 'CVMT4h', 'VC4h', 'CSPD4h', 'congmapvc', 'cvmt_p', 'rd_util', 'conglev']
    by = by_input[fields_by].rename(columns={c: 'BY' + c for c in fields_by if c != 'A_B'})
    fy = fy_input[fields_fy].rename(columns={c: 'FY' + c for c in fields_fy if c not in ['A_B', 'NAME', 'DISTANCE',
                                                                                         'SCREEN', 'RAD', 'HWYSEG',
                                                                                         'TRAV_DIR', 'FWYID', 'COUNTID',
                                                                                         'SACTRAK', 'PHASE2',
                                                                                         'pjt_id']})
    df = pd.merge(by, fy, on='A_B')
    # compute key delta measures based on delta_byfy.s
    df['fybydyv_p'] = 0
    df['fybydyv_p'] = np.where(df['BYDYV'] > 0, df['FYDYV'] / df['BYDYV'], 0)
    df['fybylanes'] = 0
    df['fybylanes'] = np.where(df['BYLANES'] > 0, df['FYLANES'] / df['BYLANES'], 0)
    df['newroad'] = 0
    df.loc[(df['FYCAPCLASS'] < 99) & (df['BYCAPCLASS'] == 99), 'newroad'] = 1  # new roadways
    df.loc[(df['FYCAPCLASS'] == 99) & (df['BYCAPCLASS'] < 99), 'newroad'] = 2  # deleted roadways
    df['delta_lanes'] = df['FYLANES'] - df['BYLANES']
    df['delta_day'] = df['FYDYV'] - df['BYDYV']
    df['delta_dvmt'] = df['FYDAYVMT'] - df['BYDAYVMT']
    df['delta_dcvmt'] = df['FYDAYCVMT'] - df['BYDAYCVMT']

    df['proj'] = 0
    df.loc[(df['SACTRAK'] != ' ') | (df['PHASE2'] != ' '), 'proj'] = 1
    df['proj_IN'] = 0
    df.loc[(df['delta_lanes'] > 0) | (df['FYCAPCLASS'] != df['BYCAPCLASS']), 'proj_IN'] = 1

    ##phasing values
        # 1=no congestion in by or fy & no project
        # 2: by low, fy mod--no proj
        # 3: by low, fy hi--no proj
        # 4: by mod, fy low--no proj
        # 5: by mod, fy mod--no proj
        # 6: by mod, fy hi--no proj
        # 7: by hi, fy low--no proj
        # 8: by hi, fy mod--no proj
        # 9: by hi, fy hi--no proj
        # 11: by low, fy low--proj
        # 10: same as 11, but byfych>1.25
        # 12: by low, fy mod--proj
        # 13: by low, fy hi--proj
        # 14: by mod, fy low--proj
        # 15: by mod, fy mod--proj
        # 16: by mod, fy hi--proj
        # 17: by hi, fy low--proj
        # 18: by hi, fy mod--proj
        # 19: by hi, fy hi--proj
    df['phasing'] = 0
    df.loc[(df['BYconglev'] == 1) & (df['FYconglev'] == 1), 'phasing'] = 1
    df.loc[(df['BYconglev'] == 1) & (df['FYconglev'] == 2), 'phasing'] = 2
    df.loc[(df['BYconglev'] == 1) & (df['FYconglev'] == 3), 'phasing'] = 3
    df.loc[(df['BYconglev'] == 2) & (df['FYconglev'] == 1), 'phasing'] = 4
    df.loc[(df['BYconglev'] == 2) & (df['FYconglev'] == 2), 'phasing'] = 5
    df.loc[(df['BYconglev'] == 2) & (df['FYconglev'] == 3), 'phasing'] = 6
    df.loc[(df['BYconglev'] == 3) & (df['FYconglev'] == 1), 'phasing'] = 7
    df.loc[(df['BYconglev'] == 3) & (df['FYconglev'] == 2), 'phasing'] = 8
    df.loc[(df['BYconglev'] == 3) & (df['FYconglev'] == 3), 'phasing'] = 9

    df.loc[(df['proj_IN'] == 1) & (df['newroad'] == 0), 'phasing'] += 10
    df.loc[(df['proj_IN'] == 1) & (df['newroad'] == 1), 'phasing'] += 20

    df.loc[
        (df['phasing'] == 11) & (df['fybydyv_p'] > 1.3) & ((df['FYA3VC'] > 0.5) | (df['FYP3VC'] > 0.5)), 'phasing'] = 10

    df.loc[(df['phasing'] > 20) & ((df['FYA3VC'] > 0.7) | (df['FYP3VC'] > 0.7)), 'phasing'] = 20
    return df


def CalcTransMetrics(BY,Scn,csvout=None,outname=None):

    BY = L_Calcs(BY)
    Scn = L_Calcs(Scn)
    BY_agg_df = agg_Calcs(BY)
    SCN_agg_df = agg_Calcs(Scn)

    difdf = pd.DataFrame()
    #print(BY_agg_df.head())

    list_of_all_columns = list(BY_agg_df.columns)
    #print(list_of_all_columns)
    for column in list_of_all_columns:
        if column == 'pjt_id':
            difdf['pjt_id'] = SCN_agg_df[column]
        else:
            difcol = "dif" + column
            pctcol = "ptc" + column

            BY_agg_df[column] = pd.to_numeric(BY_agg_df[column], errors='coerce').fillna(0).astype(float)
            SCN_agg_df[column] = pd.to_numeric(SCN_agg_df[column], errors='coerce').fillna(0).astype(float)

            difdf[difcol] = SCN_agg_df[column] - BY_agg_df[column]
            difdf[pctcol] = difdf[difcol]/(BY_agg_df[column])

    BY_rename = BY_agg_df.rename(columns={c: 'BY'+c for c in BY_agg_df.columns if c != 'pjt_id'})
    SN_rename = SCN_agg_df.rename(columns={c: 'SN'+c for c in SCN_agg_df.columns if c != 'pjt_id'})

    pjt_all_df = pd.merge(pd.merge(BY_rename,SN_rename,on='pjt_id'),difdf,on='pjt_id')
    return pjt_all_df

### Start ###
# Main Script
if __name__ == '__main__':

    # STEP 1.1 Scenario Inputs
    fields_keep = ["A", "B", "DISTANCE", "RAD", "SACTRAK", "PHASE2", "TOLLID", "USECLASS", "CAPCLASS", "LANES", "SPEED",
                   "CSPD_1", "COUNTY", "GAI", "LANEMI", "BIKE", "NAME", "SCREEN", "HWYSEG", "TRAV_DIR", "FWYID", "COUNTID",
                   # period vmt
                   "H07VMT", "H08VMT", "H09VMT", "MD5VMT", "H15VMT", "H16VMT", "H17VMT", "EV2VMT", "N11VMT", "A3VMT",
                   "MDVMT", "P3VMT", "EVVMT", "DAYVMT",
                   # period volume
                   "H07V", "H08V", "H09V", "MD5V", "H15V", "H16V", "H17V", "EV2V", "N11V", "A3V", "MDV", "P3V", "EVV", "DYV",
                   # period spd
                   "H07S", "H08S", "H09S", "MD5S", "H15S", "H16S", "H17S", "EV2S", "N11S", "A3S", "P3S", "EVS",
                   # period spd grp
                   "H07SPDGRP", "H08SPDGRP", "H09SPDGRP", "MD5SPDGRP", "H15SPDGRP", "H16SPDGRP", "H17SPDGRP", "EV2SPDGRP", "N11SPDGRP",
                   # period CVMT
                   "H07CVMT", "H08CVMT", "H09CVMT", "MD5CVMT", "H15CVMT", "H16CVMT", "H17CVMT", "EV2CVMT", "N11CVMT",
                   "A3CVMT", "MDCVMT", "P3CVMT", "EVCVMT", "DAYCVMT",
                   # period VC
                   "H07VC", "H08VC", "H09VC", "MD5VC", "H15VC", "H16VC", "H17VC", "EV2VC", "N11VC", "A3VC", "P3VC", "EVVC"]

    ## Define Input Files ##
    BY = r'\\win10-model-3\D\SACSIM23\2020\run_2020_DS_no_speed_adj\run_2020_DS_no_speed_adj\2020daynet_prjscn_temp.dbf'
    FY = r'\\win11-model-1\d$\SACSIM23\2035\DS\run_2035_DS_DSv03network_screenedtrnv01\run_2035_DS_DSv03network_screenedtrnv01\2035daynet_prjscn.dbf'

    # define out files
    BYFY_out_name = 'BYFY_35P2_35P3'
    outpathgdb = r'Q:\SACSIM23\Project_screen\networkCompare\pathwaycompare.gdb'
    link_output_fc_name = os.path.join(outpathgdb,BYFY_out_name + "_link")
    pjt_output_fc_name = os.path.join(outpathgdb,BYFY_out_name + "_pjt")

    if not arcpy.Exists(outpathgdb):
        print("Create " + os.path.basename(outpathgdb))
        ppath = os.path.dirname(outpathgdb)
        gdbname = os.path.basename(outpathgdb)
        arcpy.CreateFileGDB_management(ppath, gdbname)

    # load esri feature class into SEDF
    # Link level
    GIS_MNin = r'Q:\SACSIM23\Project_screen\Project_Screening_tests.gdb\Master_network_02282023'
    cols_to_use_link_lvl = ['SHAPE', 'A', 'B', 'pjt_id', 'COMTYPE_BY']
    net_df = pd.DataFrame.spatial.from_featureclass(GIS_MNin, usecols=cols_to_use_link_lvl)
    net_df = net_df[cols_to_use_link_lvl]
    net_df['A_B'] = net_df['A'].astype(str) + '_' + net_df['B'].astype(str)
    # Pjt level
    GIS_MN_pjt = r'Q:\SACSIM23\Project_screen\Project_Screening_tests.gdb\Scn_MN_prj_dis_comtyp'
    cols_to_use_pjt_lvl = ['SHAPE', 'SACTRAK', 'PHASE2', 'pjt_id', 'pri_lu']
    net_pjt_df = pd.DataFrame.spatial.from_featureclass(GIS_MN_pjt, usecols=cols_to_use_pjt_lvl)
    net_pjt_df = net_pjt_df[cols_to_use_pjt_lvl]
    ## Define Input Files End ##

    # Process data
    BY_dbfobj = DBF(BY)
    BY_df_raw = pd.DataFrame(iter(BY_dbfobj))
    BY_df = L_Calcs(BY_df_raw[fields_keep])

    FY_dbfobj = DBF(FY)
    FY_df_raw = pd.DataFrame(iter(FY_dbfobj))
    FY_df = L_Calcs(FY_df_raw[fields_keep])

    delta_fyby_df = byfy(BY_df, FY_df)

    # Link level comparsion and export to FC
    delta_fyby_df = byfy(BY_df, FY_df)
    link_sedf = pd.merge(net_df, delta_fyby_df, on='A_B', how='left')
    #link_output_fc_name = r'Q:\SACSIM23\Project_screen\Project_Screening_tests.gdb\TEST_Link_fromSEDF'
    link_sedf.spatial.to_featureclass(link_output_fc_name,
                                      sanitize_columns=False)  # if sanitize_columns is True, then all field names will be set to lower case with underscores

    # Pjt level comparsion and export to FC
    delta_pjt_df = CalcTransMetrics(BY_df, FY_df)
    pjt_sedf = pd.merge(net_pjt_df, delta_pjt_df, on='pjt_id', how='left')
    #pjt_output_fc_name = r'Q:\SACSIM23\Project_screen\Project_Screening_tests.gdb\TEST_Pjt_fromSEDF'
    pjt_sedf.spatial.to_featureclass(pjt_output_fc_name,
                                     sanitize_columns=False)  # if sanitize_columns is True, then all field names will be set to lower case with underscores

# -*- coding: utf-8 -*-
#--------------------------------
# Name:agg_parcel_to_hexgeom.py
# Purpose: intersect parcel and hexagon polygon files to allow area-weighted
#          aggregations of parcel data onto hex polygons
#           
# Author: Darren Conly
# Last Updated: 5/24/2019
# Updated by: <name>
# Copyright:   (c) SACOG
# Python Version: 3.x
#--------------------------------

import time
import datetime as dt
import arcpy

arcpy.env.overwriteOutput = True

#==============================

#divide parcel polygons at hex boundaries
def do_intersect(fc_parcels, fc_hexs, fc_intersect_base, fld_pcl_id, fld_pcltotarea): #clean up, fc_intersect doesn't need to be argument; is temp layer
    
    #add field with parcel's total area
    print("Intersecting {} with {}...".format(fc_parcels, fc_hexs))
    if fld_pcltotarea not in [f.name for f in arcpy.ListFields(fc_parcels)]:
        arcpy.AddField_management(fc_parcels, fld_pcltotarea, "FLOAT")
    
    area_pcl = "SHAPE@AREA"
    fields = [fld_pcltotarea, area_pcl]
    
    
    with arcpy.da.UpdateCursor(fc_parcels, fields) as cur:
        for row in cur:
            row[fields.index(fld_pcltotarea)] = row[fields.index(area_pcl)]
            cur.updateRow(row)
    
    #do intersection
    in_features = [fc_parcels, fc_hexs]
    arcpy.Intersect_analysis(in_features, fc_intersect_base)
    
#join ILUT data to polygons formed by intersecting parcel polys with hexes
def join_ilut_to_intersect(fc_intersect_base, tbl_ilutdata, tempfc_intsct_w_ilut, fld_pcl_id):
    print("Joining {} ILUT data to hex-parcel intersect layer...".format(tbl_ilutdata))
    
    arcpy.env.qualifiedFieldNames = False

    fl_intersect = "fl_intersect"
    arcpy.MakeFeatureLayer_management(fc_intersect_base, fl_intersect)
    
    
    #join ILUT or other parcel-level data to parcel pieces in intersect layer
    arcpy.AddJoin_management(fl_intersect, fld_pcl_id, tbl_ilutdata, fld_pcl_id)
    
    #copy features to temp output fc WITHOUT full field qualifiers
    arcpy.CopyFeatures_management(fl_intersect, tempfc_intsct_w_ilut)
    
    #arcpy.Delete_management(tempfc_intersect)
    
def calc_wtd_val(fc_intersect_w_propn, fld_pcltotarea, fldin, fldoutprefix, sufx_proplflds):
    
    #add field which'll be populated with the % of parcel that's in hex, as well as weighted value
    fld_pclareapct = "pctpclarea"
    fld_wtd_val = "{}{}".format(fldoutprefix, sufx_proplflds) #get area weighted value for each parcel piece (value = pop, emp, whatever)
    
    if fld_pclareapct not in [f.name for f in arcpy.ListFields(fc_intersect_w_propn)]:
        arcpy.AddField_management(fc_intersect_w_propn, fld_pclareapct, "FLOAT")
    
    if fld_wtd_val not in [f.name for f in arcpy.ListFields(fc_intersect_w_propn)]:
        arcpy.AddField_management(fc_intersect_w_propn, fld_wtd_val, "FLOAT")
    
    fl_intersectjoin = "fl_intersectjoin"
    arcpy.MakeFeatureLayer_management(fc_intersect_w_propn, fl_intersectjoin)
    
    fields = [f.name for f in arcpy.ListFields(fl_intersectjoin)]
    fields.append("SHAPE@AREA")
    
    with arcpy.da.UpdateCursor(fl_intersectjoin, fields) as cur:
        for row in cur:
            #for parcel pieces in a hex, get that piece's share of the total area of the parcel to which the piece belongs
            area_piece = row[fields.index("SHAPE@AREA")]
            area_totalpcl = row[fields.index(fld_pcltotarea)]
            pclshare = area_piece / area_totalpcl
            row[fields.index(fld_pclareapct)] = pclshare
            
            #get area-proportional value (of pop, emp, etc) for each polygon piece
            pcl_val = row[fields.index(fldin)]
            if pcl_val is None:
                share_val = 0
            else:
                share_val = pcl_val * pclshare #e.g. if piece's area is half of parcel's and we want pop, then (pop for entire parcel) * 0.5
            row[fields.index(fld_wtd_val)] = share_val
            
            cur.updateRow(row)
    
#dissolve by hex ID; getting sum, avg, etc. of the values weighted by parcel piece area
def dissolve_x_hex(fc_intersect_w_propn, dict_fld_pcl_vals, fc_hexs, fld_hexid,
                   fc_hexoutput, sufx_proplflds):
    print("aggregating to hex geometry...")
    
    arcpy.env.qualifiedFieldNames = False #ensures original table names do not append to joined fields after exporting joined layers.
    
    time_id = str(dt.datetime.now().strftime('%Y%m%d%H%M'))
    tbl_tempstats = "TEMP_stats_x_hex{}".format(time_id)
    
    #summarize by hex ID, outputting table
    #e.g. [['POP_propl':'SUM'],['VAL2_propl':'SUM']...]
    stats_tbl = [["{}{}".format(v[0], sufx_proplflds),v[1]] for k, v in dict_fld_pcl_vals.items()]
    arcpy.Statistics_analysis(fc_intersect_w_propn, tbl_tempstats, stats_tbl, fld_hexid)
    
    fl_hexs = "fl_hexs"
    arcpy.MakeFeatureLayer_management(fc_hexs, fl_hexs)
    
    #join summary x hex ID table to hex feature class.
    arcpy.AddJoin_management(fl_hexs, fld_hexid, tbl_tempstats, fld_hexid)
    arcpy.CopyFeatures_management(fl_hexs, fc_hexoutput)
    
    arcpy.Delete_management(tbl_tempstats)
    arcpy.Delete_management(fc_intersect_w_propn)
    
def get_buffered_aggs(fc_ilut_parcels, fc_hexs, fld_hexid, fld_pcl_id, val_fields):
    
    buff_dist_ft = 1320
    
    fl_hexs = "fl_hexs"
    fl_parcels = "fl_parcels"
    
    arcpy.MakeFeatureLayer_management(fc_hexs, fl_hexs)
    arcpy.MakeFeatureLayer_management(fc_ilut_parcels, fl_parcels)
    
    list_hex_ids = []
    
    #make list of all hex ids
    with arcpy.da.SearchCursor(fl_hexs,fld_hexid) as cur:
        for row in cur:
            hexid = row[0]
            list_hex_ids.append(hexid)
            
    dict_ids_values = {}     
    #go through each id and update the value in the hex FC
    for hexid in list_hex_ids: #might be able to calculate multiple value fields at a time?
        #get hex for current iteration
        sql_gethexid = "{} = '{}'".format(fld_hexid, hexid)
        #print(sql_gethexid)
        arcpy.SelectLayerByAttribute_management(fl_hexs,"NEW SELECTION",sql_gethexid)
        
        #get all parcels with centroid within specified distance of hex
        arcpy.SelectLayerByLocation_management(fl_parcels,"HAVE THEIR CENTER IN", 
                                               fl_hexs, buff_dist_ft, "NEW SELECTION")
        
        aggval = 0
        with arcpy.da.SearchCursor(fl_parcels, val_fields) as cur:
            for row in cur:
                for field in val_fields:
                    out_val = row[0]
                    aggval += out_val
    
    return dict_ids_values
        
        

def do_work(fc_parcels, fc_hexs, fld_hexid, tbl_ilutdata, fld_pcl_id, dict_fld_pcl_vals, fc_hexoutput,
            make_new_pcl_intersect_layer = True):
    
    time_id = str(dt.datetime.now().strftime('%Y%m%d%H%M'))
    fld_pcltotarea = "pcltotarea" #clean this up. This variable is declared twice
    
    
    
#    #make layer that's intersect of parcels and hexs
#    fc_intersect_base = "PclHexIntersect_base"
#    if make_new_pcl_intersect_layer:
#        do_intersect(fc_parcels, fc_hexs, fc_intersect_base, fld_pcl_id, fld_pcltotarea)
#    
#    #join ILUT data to the intersect layer
#    tempfc_intsct_w_ilut = "TEMPintsct_w_ILUT{}".format(time_id)
#    join_ilut_to_intersect(fc_intersect_base, tbl_ilutdata, tempfc_intsct_w_ilut, fld_pcl_id)
#    
#    sufx_proplflds = "_propl" #suffix to give to field names to indicate they're proportional to share of parcel
#    
#    #calculate area-weighted ILUT values for hexes
#    fc_intsct_out = "PclHexIntsct_wValPropns{}".format(time_id)
#    arcpy.CopyFeatures_management(tempfc_intsct_w_ilut, fc_intsct_out)
#    
#    for fldin, fldoutprefix in dict_fld_pcl_vals.items():
#        print("calculating {} field...".format(fldoutprefix))
#        fldoutprefix = fldoutprefix[0]
#        calc_wtd_val(fc_intsct_out, fld_pcltotarea, fldin, fldoutprefix, sufx_proplflds)
#    
#    dissolve_x_hex(fc_intsct_out, dict_fld_pcl_vals, fc_hexs, fld_hexid, fc_hexoutput, sufx_proplflds)
    
#    arcpy.Delete_management(tempfc_intsct_w_ilut)
    

if __name__ == '__main__':
    arcpy.env.workspace = r'Q:\SACSIM19\Integration Data Summary\ILUT GIS\ILUT GIS.gdb'

    fc_ilut_parcels = 'ilut_combined2040_16_mixd'
    
    fc_hexs = 'hex_base_sample'
    
    fld_hexid = 'GRID_ID'
    
    fld_pcl_id = 'PARCELID'
    
    fld_pcl_id = 'parcelid'
    
    val_fields = ['HH_hh', 'EMPTOT']
                         
    test_dict = get_buffered_aggs(fc_ilut_parcels, fc_hexs, fld_hexid, fld_pcl_id, val_fields)		 

    
    
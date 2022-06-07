"""
Name: line_node_to_tranline.py
Purpose: Takes in list of transit nodes and table of line-level transit attributes,
    combines to create LIN transit file for CUBE model runs


Author: Darren Conly
Last Updated: Apr 2022
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""

import os
import re
import csv


import pandas as pd

def break_list(in_list, max_pc_len, make_str=True, indent=''):
    # takes 1 long in_list and breaks it into sublists of maximum length max_pc_len
    # specifying make_str returns each sublist as a comma-separated string instead
    # of a list object.

    # indent specifies what amount of spacing (tab, or '\t' is typical)


    out_rows = [in_list[i:i + max_pc_len] for i, v \
                    in enumerate(in_list) if i % max_pc_len == 0]

    if make_str:
        out_row_strs = []

        for i, out_row in enumerate(out_rows):
            if out_row == out_rows[-1]:
                out_row = f"{indent}{', '.join(out_row)}\n"
            else:
                out_row = f"{indent}{', '.join(out_row)},\n"

            out_row_strs.append(out_row)
        
        final_rows = out_row_strs
    else:
        final_rows = out_rows

    return final_rows

def get_node_rows(route_name, node_df): 

    line_node_list = []
    node_mult_dict = {"Y": 1, "N": -1}
    max_line_items = 8 # no more than 8 items in one line, to help make more navigable

    line_data = node_df.loc[node_df["LINE NAME"] == route_name].to_dict(orient='records')
    # import pdb; pdb.set_trace()

    for i, row in enumerate(line_data):
        if i == 0: tf_prev = '0'

        tf_new = row["TF"]

        # get node ids
        if row["LINE NAME"] == route_name:
            stop_tag_val = node_mult_dict[row["STOP"]]
            node_id_signed = int(row["N"]) * stop_tag_val # make N negative if non-stop node
            node_item = f"N={node_id_signed}"

            # if there's an in-line change in time factor, indicate it in the node list
            if tf_prev != tf_new:
                tf_item = f"TF={tf_new}"
                if i == 0:
                    line_node_list.extend([node_item, tf_item]) # line node lists must start with a node, not a time factor
                else:
                    line_node_list.extend([tf_item, node_item])
                # line_node_list.append(tf_item)
                # line_node_list.append(node_item)
                tf_prev = tf_new
            else:
                line_node_list.append(node_item)

    node_row_strs = break_list(line_node_list, max_line_items, make_str=True, indent='\t')

    return node_row_strs

    

def make_trnfile(working_dir, sc_year, line_txt, node_txt):
    output_lin = os.path.join(working_dir, f"{sc_year}_tranline.lin")

    k_linename = "LINE NAME"
    k_timefac = 'TF'

    out_list = []
    with open(line_txt, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            out_list.append(row)
    node_data_df = pd.read_csv(node_txt, dtype={k_timefac: 'str'})

    with open(output_lin,'w') as f_out:
        total_lines = len(out_list)
        for i, line_info_dict in enumerate(out_list):
            out_row = []
            for k, v in line_info_dict.items():
                if k == k_linename:
                    v = f"\"{v}\"" 
                out_attr = f'{k}={v}'
                out_row.append(out_attr)

            # import pdb; pdb.set_trace()

            line_attr_rows = break_list(out_row, max_pc_len=8, make_str=True, indent='')
            line_attr_rows[-1] = line_attr_rows[-1].replace('\n',',\n') # must insert comma to separate line-level features from node list
            for attr_row in line_attr_rows:
                f_out.write(attr_row)

            rtname = line_info_dict[k_linename]

            node_rows = get_node_rows(rtname, node_data_df)

            for row in node_rows:
                f_out.write(row)
            
            if i % 50 == 0: print(f"{i} of {total_lines} routes compiled...")

    return output_lin
            

if __name__ == '__main__':

    working_dir = input('Enter output folder path: ')
    in_lines = input('Enter file path to LINE attribute table: ')
    in_nodes = input('Enter file path to NODE attribute table: ')

    # # hard-coded for testing
    # working_dir = r'D:\SACSIM23\faresystem_change\LIN_updating\transit_linenode_V1'
    # in_lines = r"D:\SACSIM23\faresystem_change\LIN_updating\transit_linenode_V1\2016_tranline_lines2.txt"
    # in_nodes = r"D:\SACSIM23\faresystem_change\LIN_updating\transit_linenode_V1\2016_tranline_original_nodes.txt"


    output_lin_path = make_trnfile(working_dir=working_dir, sc_year=2016, line_txt=in_lines, node_txt=in_nodes)
    print(f"Success! Output LIN file in {output_lin_path}")


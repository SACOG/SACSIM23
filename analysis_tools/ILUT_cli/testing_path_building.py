# -*- coding: utf-8 -*-
"""
Created on Tue Feb  9 10:31:04 2021

@author: dconly
"""

import os
from pathlib import Path

def get_unc_path(in_path):
    
    # based on a network drive path, convert the letter to full machine name
    unc_path = str(Path(in_path).resolve())
    
    # if the model run folder is on the machine that this script is getting run on,
    # the full machine name path must be manually built.

    # import pdb; pdb.set_trace()
    if unc_path == in_path:
        import socket
        machine = socket.gethostname()
        drive_letter = os.path.splitdrive(in_path)[0].strip(':')
        folderpath = os.path.splitdrive(in_path)[1]
        unc_path = f"\\\\{machine}\\{drive_letter}{folderpath}"
    
    return unc_path

test_path = r'\\win10-model-4\D\SACSIM23\2016_new_DaySim_updated_Preliminary_deliverable\run_2016_fixDelCurv_newDaysim'

test_result = get_unc_path(test_path)
print(test_result)
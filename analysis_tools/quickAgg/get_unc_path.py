

def build_unc_path(in_path):
    # function to convert any file path into a UNC path (e.g., even if it's a mapped letter drive)
    # allows resulting string to be more consistent and copy/pastable for linking to model run folder
    
    # based on a network drive path, convert the letter to full machine name
    from pathlib import Path
    import socket
    import os
    
    unc_path = str(Path(in_path).resolve())
    script_machine = socket.gethostname() # name of machine that script's running on
    
    model_run_drive = Path(unc_path).drive
    
    if script_machine not in model_run_drive: # if model run is on different machine from script run, just use UNC path straight up
        out_path = unc_path
    else: # what to do if on this script is run on same machine as model run folder
        drive_letter = model_run_drive.strip(':') # get drive letter
        folderpath = os.path.splitdrive(in_path)[1] # get the parts of the folder path following drive
        out_path = f"\\\\{script_machine}\\{drive_letter}{folderpath}" # manually build a UNC path, even if on local machine
    
    return out_path

if __name__ == '__main__':
    testdir = r'T:\SACSIM23'
    result = build_unc_path(testdir)

    print(result)
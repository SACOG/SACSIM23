"""
Name: format_for_bcp.py
Purpose: Make minor tweaks to input files so that they meet BCP loader's
    finicky formatting requirements.

    Example fixes this script can do:
     - add newline character at end of last line so that BCP will read in last line of file.
     - some CSVs have weird leading comma at start of first line. This script removes that comma.
     - structured as a class to have more cleanup/QA methods added as specific use cases require.


Author: Darren Conly
Last Updated: Nov 2022
Updated by: 
Copyright:   (c) SACOG
Python Version: 3.x
"""

    

import pathlib
# import csv

class fileChecker:
    def __init__(self, in_file, delim_char=',', overwrite=False):
        self.in_file = pathlib.Path(in_file)
        with open(self.in_file, 'r') as f_in:
            self.f_rows = f_in.readlines()

        self.delim_char = delim_char
        self.overwrite = overwrite
        self.file_changed = False

        

    def check_eol_char(self, eol_char='\n'):
        """Ensures that last row of file ends with newline (\n) character"""

        self.eol_char = eol_char
        last_row = self.f_rows[-1]
        
        # if last row does not have a newline character, add one
        if last_row[-1:] != eol_char:
            last_row2 = f"{last_row}{eol_char}"

            self.f_rows[-1] = last_row2
            self.file_changed = True
    
    def check_row_len(self):
        header_row = self.f_rows[0].split(self.delim_char)
        data_row = self.f_rows[1].split(self.delim_char)

        len_header = len(header_row)
        len_datarow = len(data_row)

        if len_header != len_datarow:
            exc_msg = f"""ERROR: {self.in_file} header specifies {len_header} fields,
            but data rows only have {len_datarow} elements in them.
            Please check file for errors."""
            raise Exception(exc_msg)


    def leading_comma_warn(self):
        # if header row first character is the delimiter character, warn user
        # do not allow a leading comma, because all headers must have names
        first_row = self.f_rows[0]
        if first_row[0] == self.delim_char:
            input_msg = f"""
            WARNING: {self.in_file} header row is the following:
            {first_row}
            Note that its leading character is the delimiter character.
            This can result in empty field names, which are bad practice.
            Please enter a field name or just hit Enter if you want a default field name assigned:  
            """
            leading_fname = input(input_msg)
            leading_fname2 = 'FIELD0' if leading_fname == '' else leading_fname
            self.f_rows[0] = f"{leading_fname2}{first_row}"
            self.check_row_len()
            self.file_changed = True

    def export_to_file(self):
        # export updated set of rows to new file (may want to update to overwrite existing file?)
        fext = self.in_file.suffix
        fname = self.in_file.stem
        output_dir = self.in_file.parent
        out_path = self.in_file if self.overwrite else self.in_file.joinpath(output_dir, f"{fname}_fmt4bcp{fext}")
        with open(out_path, 'w') as f_out:
            for row in self.f_rows:
                f_out.write(row)


if __name__ == '__main__':
    # file_in = r"Q:\SACSIM23\Parcel\2020_baseline_v3\to_SACSIM23\Post_Allocation_results\eto.csv"
    file_in = r"C:\Users\dconly\GitRepos\SQL-tools\bulk_loader\test_inputs\test_inputs.csv"
    overwrite_val = False
    obj = fileChecker(file_in, overwrite=overwrite_val)

    import pdb; pdb.set_trace()

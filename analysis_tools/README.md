# SACSIM19 Integrated Land Use Transportation (ILUT) Summary Tool

## Contents
* [Version Info](#Version-Info)
* [What is the ILUT Tool?]()
* [Using the ILUT Tool](#Using-the-ILUT-Tool)
* [Software and Package Requirements](#Software-and-Package-Requirements)



## Version Info

Last update: July 2022

## What is the ILUT Tool?
The integrated land-use transportation (ILUT) tool takes in raw model input and output files and generates an output table that provides wide array of transportation and land use information for each parcel in the SACOG region. Among the dozens of variables, the resulting ILUT table provides the following information for each parcel::
* Travel behavior (VMT, mode split, etc.)
* Demographic and job data (total population, count of workers, school population, total jobs, etc.)
* Land use characteristics (total dwelling units, type of land use, etc.)

The ILUT table also contains fields like TAZ, census tract, county, and other characteristics that allow more aggregate "roll-ups" of the data to these and other geographies.

## Using the ILUT Tool

### Running as a toolbox in ArcGIS Pro

Instructions for running the ILUT process from an ArcGIS Pro interface are documented in the [README for the ILUT GIS Toolbox](https://github.com/SACOG/SACSIM23-internal/blob/main/ilut_tools/ILUT_GISTool/README.md).

The advantage to using the toolbox is that it contains all dependencies in a single folder, so as long as you have ArcGIS Pro installed, you *should* be able to run the tool without creating any custom python environments. You should be able to just load the tool into an ArcGIS Pro project and run it.

### Running from a standalone script

The upshot to running the ILUT process from a standalone script is that it does not require ArcGIS Pro to be installed. The drawback is that it is less friendly to users not versed in python or working with python environments.

1.  Go to the 'ILUT' folder
2.  Open ilut.py script in interpreter (e.g. IDLE, PyCharm)
3.  Run the script, entering parameters as prompted



## Software and Package Requirements:

### Python packages

The ILUT Summary Tool requires the following packages be installed

-pyodbc

-dbfread

At SACOG, we recommend installing these packages using Conda. For more information on how to do this, please refer to our [Conda reference](https://github.com/SACOG/SACOG-Intro/blob/main/using-envs/sacog-Python-Env-Reference.md#setting-up-your-python-environment)

### Other Software Requirements

**Microsoft SQL Server**

The ILUT tool is designed to work with Microsoft SQL Server. If you have
a different RDBMS (e.g. Postgres, MySQL, etc.) , you will need to update
the query syntax accordingly.


**SQL Server Bulk Copy Program (BCP)**

The ILUT tool relies on SQL Server's Bulk Copy Program (BCP) to quickly
and seamlessly load model output tables into SQL Server. Before running
the ILUT tool, you must [download the BCP utility from
Microsoft](https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver15).

*Note -- if this link does not work, simply search for "SQL Server BCP
utility"*



## Troubleshooting

* *Problem* - `PermissionError: [Errno 13] Permission denied` may arise when trying to run the ILUT, and will often say that the script doesn't have permission to open a package (e.g. numpy)
  * *Diagnosing and Workaround* - If you are trying to run the script in the default ArcGIS Pro environment, try making a clone of it, switching to the clone, then running the script in the clone. More information on environments and cloning is available [here](https://github.com/SACOG/SACOG-Intro/blob/b81aa5b47b740d52a381b1e7b33675a0c20bfd12/using-envs/sacog-Python-Env-Reference.md).

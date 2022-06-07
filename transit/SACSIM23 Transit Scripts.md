# SACSIM23 Transit Scripts

## Check Fit Between Highway and Transit Networks

If there is a mismatch between the SACSIM highway network and the node list for a transit line in the transit network, then the model will break. 

Splitting a link is the most common cause of a network mismatch. E.g., a transit line traverses a link A->B in the model network. But then a user will split the link in the model network so that link A->B becomes A->C->B, and that change will not be reflected in the transit network.

To ensure the model will not break, it is a good idea to run [CheckFit_PTLineHwyNet.py](https://github.com/SACOG/SACSIM23-internal/blob/b1eb3efa2881a7a240818d4bf9a2b441fcbfeaef/transit/CheckFit_PTLineHwyNet.py). The script will compare the nodes in the transit file to those in the highway file and produce a CSV flagging the following issues:

* CHECK_DIR, where the transit line is going against the direction indicated by the link (this does not affect model runs but is something to be aware of)
* DISABLED_LINK, indicating the transit line is traveling on a link that is not a real road. Again this does not affect the model run but is generally something that should not occur (i.e., we want our transit to run on real roads)
* LINK_MISSING - this is the *most important* flag to check for. If any links are missing, it means there is likely a mismatch and either the highway network or transit node list must be updated before running the model.

## Export Transit Links to Node Lists or GIS Files

Explanation to be added here at a later date. In the meantime, you can use the [trantxt2linknode_gis.py](https://github.com/SACOG/SACSIM23-internal/blob/b1eb3efa2881a7a240818d4bf9a2b441fcbfeaef/transit/trantxt2linknode_gis.py) script.

## Generate GTFS Line Summary

### Overview

The [gtfs_linesummary.py](https://github.com/SACOG/SACSIM23-internal/blob/b2f68fea8ba44aeadecf640cf84017322ad325b5/transit/gtfs/line_summary/gtfs_linesummary.py) script calculates key transit service characteristics aggregated to SACSIM model time periods. Specifically, **for each time period**, the script calculates:

* Average headway (includes shortened or partial trips)
* "First" headway, defined as headway between the first and second runs of the time period. This is to flag instances of commuter routes whose 5am-9am AM average headway may be very long, but it has departures spaced close together, tailored to the needs of a specific commuter market.
* Number of trips (includes shortened or partial trips)
* Departure time of the first trip from the first stop (includes shortened or partial trips)
* Departure time of the second trip from the first stop (includes shortened or partial trips)
* Vehicle service hours (includes shortened or partial trips)
* Vehicle service miles (includes shortened or partial trips)
* Average first-to-last-stop trip duration (only for trips traveling full route length)
* Average first-to-last-stop trip speed (includes shortened or partial trips)

These outputs are intended to help modelers set base year headways and operating characteristics for transit lines in the model.

### Run setup

1. Create a folder containing unzipped folders for each operator's GTFS data.
2. In the same folder you created in step 1, add the [gtfs_input_spec.csv](https://github.com/SACOG/SACSIM23-internal/blob/b2f68fea8ba44aeadecf640cf84017322ad325b5/transit/gtfs/line_summary/gtfs_input_spec.csv) file and update it as needed. Ideally, you should be able to just indicate the raw TXT files that come with each operator's GTFS data, but in some cases, you may need to make a modified version of the TXT file and specify the name of that modified version in the spec CSV.
3. Update the user input parameters in the gtfs_linesummary.py script

4. Run gtfs_linesummary.py script.




/*
Sets value for column EMPHBB_SCNYR in table.
As of 2/1/2019, this value is not estimated for future years, so we just carry it through
to future years from 2016 base year.


*/

USE MTP2020

SELECT
	etof.*,
	etob.EMPHBB_SCNYR
INTO raw_eto2040_2
FROM raw_eto2040 etof
	LEFT JOIN raw_etopcl2016_base etob
		ON etof.PARCELID = etob.PARCELID

SELECT * 
FROM raw_eto2040_2
WHERE EMPHBB_SCNYR IS NULL
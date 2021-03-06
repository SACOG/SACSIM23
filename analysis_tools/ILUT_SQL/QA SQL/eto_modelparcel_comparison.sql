--to check for why MIXINDEX is null in combined
select TOP 100
	eto.*,
	pcl.hh_2
FROM raw_etopcl2016_base eto
	LEFT JOIN raw_parcel2016_2 pcl
		ON eto.PARCELID = pcl.parcelid
WHERE pcl.parcelid IS NULL
	AND (eto.HH_P > 0 OR Persons_P > 0)
	AND eto.EMPTOT_P > 0


--check why IXXI/CVEH values null in combined table
--happens on parcels where there are no people or jobs
select TOP 100
	eto.*,
	com.IX_VT_RES AS IX_VT_RES_raw,
	CASE WHEN com.IX_VT_RES IS NULL THEN 0 ELSE com.IX_VT_RES END
FROM raw_etopcl2016_base eto
	LEFT JOIN ilut_combined2016_2 com
		ON eto.PARCELID = com.parcelid
WHERE com.PARCELID IS NOT NULL
	AND com.IX_VT_RES IS NULL
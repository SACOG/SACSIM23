/*
Name: theme_telework_VMTConstants.sql
Purpose: 
	Create parcel-level telework data fields, including:
		# of workers working from home (already have this field as WAH) 
		# of full telework workers (not WAH) 
		# of workers who did not do full telework (not WAH, traveled to work location at least once during day for any amount of time) 
		VMT made by workers working at home 
		VMT from workers doing full telework 
		VMT from workings who went into office at all 
	
           
Author: Darren Conly
Last Updated: Jun 2022
Updated by: <name>
Copyright:   (c) SACOG
SQL Flavor: SQL Server
*/

SET NOCOUNT ON

IF OBJECT_ID('{4}', 'U') IS NOT NULL 
DROP TABLE {4}; --output table name. Delete if it exists

-- get VMT for each trip
SELECT
	trip.hhno,
	trip.pno,
	CASE 
		WHEN trip.mode = 3 THEN trip.distau
		WHEN trip.mode = 4 THEN trip.distau*0.5
		WHEN trip.mode = 5 THEN trip.distau*0.3
		WHEN trip.mode = 9 THEN trip.distau*0.6
		ELSE 0
	END AS vmt
INTO #trip_temp
FROM {0} trip --raw trip table


--Get table of person attributes, compute telework flag
SELECT
	p.hhno,
	p.pno,
	hh.hhparcel,
	CASE 
		WHEN p.pwtyp = 1
			AND p.pwpcl != hh.hhparcel --primary work location not at home
			AND pd.wkathome = 0 --did not telework that day
		THEN 1 --person is a worker with primary work place away from home and did not telework that day
		WHEN p.pwtyp = 1
			AND p.pwpcl != hh.hhparcel --primary work location not at home
			AND pd.wkathome = 4 --teleworked that day
			AND pd.wktours > 0 --made 1 or more work-related tours that day
		THEN 2 --person teleworked part of the day but still commuted to work at some point
		WHEN p.pwtyp = 1 -- pwtyp: 0 = not worker, 1 = full-time, 2 = part-time
			AND p.pwpcl != hh.hhparcel --primary work location not at home
			AND pd.wkathome = 4 --teleworked that day
			AND pd.wktours = 0 --made no work-related tours that day
		THEN 3 --person teleworked the entire day and did not make any work-related tours
		WHEN p.pwtyp = 1
			AND p.pwpcl = hh.hhparcel --primary workplace is at home (home-based business)
		THEN 4 --person works at a home-based business
		WHEN p.pwtyp = 0 --person is not a worker
		THEN 0
		ELSE -1 --person is a part-time worker
	END AS telework_ind
	INTO #person_tw
FROM {1} pd --raw person_day table
	JOIN {2} p --raw person table
		ON p.id = pd.person_id
	JOIN {3} hh --raw hh table
		ON pd.hhno = hh.hhno


-- get each person's VMT, along with their telework flag
SELECT 
	p.hhno,
	p.pno,
	p.hhparcel,
	telework_ind,
	SUM(CASE WHEN t.vmt IS NULL THEN 0 ELSE t.vmt END) AS person_vmt
INTO #tw_vmt
FROM #person_tw p
	LEFT JOIN #trip_temp t
		ON p.hhno = t.hhno
		AND p.pno = t.pno
GROUP BY p.hhparcel, p.hhno, p.pno, telework_ind

SELECT
	hhparcel,
	SUM(CASE WHEN telework_ind = 1 THEN 1 ELSE 0 END) AS WKR_NO_TELWK,
	SUM(CASE WHEN telework_ind = 2 THEN 1 ELSE 0 END) AS WKR_TELWK_PART,
	SUM(CASE WHEN telework_ind = 3 THEN 1 ELSE 0 END) AS WKR_TELWK_FULL,
	SUM(CASE WHEN telework_ind = 1 THEN person_vmt ELSE 0 END) AS VMT_WKR_NOTELWK,
	SUM(CASE WHEN telework_ind = 2 THEN person_vmt ELSE 0 END) AS VMT_TELWKR_PART,
	SUM(CASE WHEN telework_ind = 3 THEN person_vmt ELSE 0 END) AS VMT_TELWKR_FULL,
	SUM(CASE WHEN telework_ind = 4 THEN person_vmt ELSE 0 END) AS VMT_WAHWKR
INTO {4} --temporary table with telework data
FROM #tw_vmt
GROUP BY hhparcel


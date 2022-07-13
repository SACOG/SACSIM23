/*
Name: telework_summary_testing.sql
Purpose: 
	Create person-level tag indicating if workers teleworked all day,
	teleworked part of the day, or did not telework.

	Compare per-capita VMT between people who are not workers, workers
	who did not telework, workers who teleworked part of the day,
	and workers who teleworked the entire day.
           
Author: Darren Conly
Last Updated: <date>
Updated by: <name>
Copyright:   (c) SACOG
SQL Flavor: SQL Server
*/

USE MTP2024

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
FROM raw_trip2016_8008 trip --raw_trip2016_8008


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
			AND pd.wktours > 0 --made at least 1 work-related tour
		THEN 2 --person teleworked part of the day but still commuted to work at some point
		WHEN p.pwtyp = 1 -- pwtyp: 0 = not worker, 1 = full-time, 2 = part-time
			AND p.pwpcl != hh.hhparcel --primary work location not at home
			AND pd.wkathome = 4 --teleworked that day
			AND pd.wktours = 0 --made no work-related tours
		THEN 3 --person teleworked the entire day and did not commute to work
		WHEN p.pwtyp = 1
			AND p.pwpcl = hh.hhparcel --primary workplace is at home (home-based business)
		THEN 4 --person works at a home-based business
		WHEN p.pwtyp = 0 --person is not a worker
		THEN 0
		ELSE -1 --person is a part-time worker
	END AS telework_ind
	INTO #person_tw
FROM raw_personday_2016_8008 pd
	JOIN raw_person2016_8008 p
		ON p.id = pd.person_id
	JOIN raw_hh2016_8008 hh
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

--SELECT TOP 1000 * FROM #tw_vmt
--SELECT COUNT (*) FROM #tw_vmt

SELECT
	hhparcel,
	SUM(CASE WHEN telework_ind = 1 THEN 1 ELSE 0 END) AS WKR_NO_TELWK,
	SUM(CASE WHEN telework_ind = 2 THEN 1 ELSE 0 END) AS WKR_TELWK_PART,
	SUM(CASE WHEN telework_ind = 3 THEN 1 ELSE 0 END) AS WKR_TELWK_FULL,
	SUM(CASE WHEN telework_ind = 4 THEN 1 ELSE 0 END) AS WKR_WAH,
	SUM(CASE WHEN telework_ind = 1 THEN person_vmt ELSE 0 END) AS VMT_WKR_NOTELWK,
	SUM(CASE WHEN telework_ind = 2 THEN person_vmt ELSE 0 END) AS VMT_TELWKR_PART,
	SUM(CASE WHEN telework_ind = 3 THEN person_vmt ELSE 0 END) AS VMT_TELWKR_FULL
INTO #TEMP_tw_x_pcl
FROM #tw_vmt
GROUP BY hhparcel


--SELECT TOP 100 * FROM #TEMP_tw_x_pcl
--WHERE hhparcel = 67102752

SELECT * FROM #tw_vmt
WHERE hhno = 518742 and pno = 1

SELECT hhno, pno, distau, mode FROM raw_trip2016_8008
WHERE hhno = 518742 and pno = 1


--summarize VMT per capita by telework type
SELECT
	telework_ind,
	SUM(person_vmt) AS vmt,
	COUNT(telework_ind) AS person_cnt,
	SUM(person_vmt) / COUNT(person_vmt) AS vmt_per_cap
FROM #tw_vmt
GROUP BY telework_ind


SELECT
	SUM(CASE WHEN telework_ind IN (2, 3) THEN person_vmt ELSE 0 END)
	/ SUM(CASE WHEN telework_ind IN (2, 3) THEN 1 ELSE 0 END)
	AS vmt_per_cap_any_tw
FROM #tw_vmt

--SELECT
--	SUM(WKR_NO_TELWK) AS WKR_NO_TELWK,
--	SUM(WKR_TELWK_PART) AS WKR_TELWK_PART,
--	SUM(WKR_TELWK_FULL) AS WKR_TELWK_FULL,
--	SUM(WKR_WAH) AS WKR_WAH,
--	SUM(VMT_WKR_NOTELWK) AS VMT_WKR_NOTELWK,
--	SUM(VMT_TELWKR_PART) AS VMT_TELWKR_PART,
--	SUM(VMT_TELWKR_FULL) AS VMT_TELWKR_FULL
--FROM #TEMP_tw_x_pcl

--SELECT COUNT(*) FROM raw_person2016_8008 WHERE PWTYP = 1



/*
DROP TABLE #trip_temp
DROP TABLE #person_tw
DROP TABLE #tw_vmt
DROP TABLE #TEMP_tw_x_pcl
*/
--select distinct wkathome from raw_personday_2016_8008


/*
select * from #trip_temp
where hhno = 772100 and pno = 2

SELECT * FROM #person_tw 
WHERE hhno = 772100 and pno = 2

SELECT * FROM raw_personday_2016_8008 
WHERE hhno = 772100 and pno = 2

SELECT COUNT(*) FROM raw_personday_2016_8008
SELECT COUNT(*) FROM raw_person2016_8008

SELECT TOP 10 * FROM raw_hh2016_8008
*/


/*
Name: theme_triptour_VMTConstants.sql
Purpose: Summarize trip and tour data at parcel level--DOES NOT INCLUDE IXXI OR CVEH. THESE ARE IN SEPARATE SCRIPT
	Script works for any scenario with AVs DISABLED
	Input tables: model output trip file w/skim data, model output tour table, 
			hh table, person table

	FOR VMT - this script uses VMT constants to estimate VMT from person-trips.
		E.g., SOV = trip vmt * 1, HOV2 = tripvmt * 0.5, etc.

		HOWEVER, script uses driver-or-passenger (DORP) for TNC VMT. See below

		In theory DORP is better basis for VMT (if person making trip is driver, then count its VMT, otherwise no).
		But for consistency with prior plans, we are still using above-described constants for non-VMT auto-trips.
           
Author: Darren Conly
Last Updated: Nov 2022
Updated by: <name>
Copyright:   (c) SACOG
SQL Flavor: SQL Server
*/

--=======make raw trip table with tour, parcel, data attached================
SET NOCOUNT ON

IF OBJECT_ID('{triptour_outtbl}', 'U') IS NOT NULL 
DROP TABLE {triptour_outtbl}; --output triptour table name

--set per-mile GHG production factors (grams of GHGs)
DECLARE @emissions0to5 FLOAT SET @emissions0to5 = 1251.4
DECLARE @emissions5to10 FLOAT SET @emissions5to10 = 937.0
DECLARE @emissions10to15 FLOAT SET @emissions10to15 = 732.1
DECLARE @emissions15to20 FLOAT SET @emissions15to20 = 595.8
DECLARE @emissions20to25 FLOAT SET @emissions20to25 = 504.5
DECLARE @emissions25to30 FLOAT SET @emissions25to30 = 444.1
DECLARE @emissions30to35 FLOAT SET @emissions30to35 = 406.1
DECLARE @emissions35to40 FLOAT SET @emissions35to40 = 385.4
DECLARE @emissions40to45 FLOAT SET @emissions40to45 = 379.4
DECLARE @emissions45to50 FLOAT SET @emissions45to50 = 387.0
DECLARE @emissions50to55 FLOAT SET @emissions50to55 = 408.5
DECLARE @emissions55to60 FLOAT SET @emissions55to60 = 446.0
DECLARE @emissionsOver60 FLOAT SET @emissionsOver60 = 503.3

--TNC adjustment factors

--multiply distance by this to capture TNC deadhead distance--needs further research; fixed single value not ideal
	--Example: value of 1.40 assumes that any TNC trip is 40% more VMT because of deadheading
DECLARE @TNCDeadHeadFactor FLOAT SET @TNCDeadHeadFactor = 1.40 
DECLARE @TNCNonAV1px FLOAT SET @TNCNonAV1px = 11 --TNC trip in non-AV with 1 passenger
DECLARE @TNCAV1px FLOAT SET @TNCAV1px = 21 --TNC trip in AV with 1 passenger
DECLARE @TNCNonAV2px FLOAT SET @TNCNonAV2px = 12
DECLARE @TNCAV2px FLOAT SET @TNCAV2px = 22
DECLARE @TNCNonAV3px FLOAT SET @TNCNonAV3px = 13
DECLARE @TNCAV3px FLOAT SET @TNCAV3px = 23

CREATE TABLE #trip_temp (
	parcelid INT,
	hhno INT,
	hrestype INT,
	person_id INT,
	p_uworkpcl INT,
	tour_id INT,
	pdpurp INT,
	tmodetp INT,
	parent INT,
	trip_id INT,
	trip_mode INT,
	trip_pathtype INT,
	trip_travtime_hrs FLOAT,
	trip_timeau_hrs FLOAT,
	trip_travdist FLOAT,
	trip_distau FLOAT,
	trip_distcong FLOAT,
	trip_speed FLOAT,
	trip_dorp INT --1 = driver, normal veh; 3 = "main" passenger in AV
	)


INSERT INTO #trip_temp
	SELECT
		hh.hhparcel,
		hh.hhno,
		hh.hrestype,
		p.id,
		p.pwpcl,
		tour.id,
		tour.pdpurp,
		tour.tmodetp,
		tour.parent,
		trip.id,
		trip.mode,
		trip.pathtype,
		trip.travtime/60 AS trip_travtime_hrs, --NOTE: in some cases the total travel time is less than the in-auto TT--how?
		trip.timeau/60 AS trip_timeau_hrs, --do not sum this. it double counts. for unique veh trips sum the travtimeau_hrs in the #vehicle_trips_temp table
		trip.travdist,
		trip.distau,
		trip.distcong,
		CASE WHEN trip.mode IN (3,4,5) AND trip.timeau> 0 AND trip.distau > 0 THEN trip.distau/(trip.timeau/60) --auto speed
			WHEN trip.mode NOT IN (3,4,5) AND trip.travtime > 0 AND trip.travdist > 0 THEN trip.travdist/(trip.travtime/60) --non-auto speed
			ELSE 20 --default speed if travtime or travdist = 0
		END AS trip_speed,
		trip.dorp
	FROM {raw_trip} trip --raw trip table
		JOIN {raw_tour} tour --raw tour table
			ON trip.tour_id = tour.id
		JOIN {raw_person} p --raw person table
			ON tour.person_id = p.id
		JOIN {raw_hh} hh --raw hh table
			ON p.hhno = hh.hhno

--==get unique VMT-per-vehicle trip stats (i.e. not double counting due to carpool trips)
CREATE TABLE #vehicle_trips_temp (
	trip_id BIGINT,
	tour_id INT,
	travtimeau_hrs FLOAT,
	distau2 FLOAT,
	distcong2 FLOAT
	)

INSERT INTO #vehicle_trips_temp
	SELECT
		trip.trip_id,
		trip.tour_id,
		CASE 
			WHEN trip.trip_mode = 3 THEN trip.trip_timeau_hrs --drive alone
			WHEN trip.trip_mode = 4 THEN trip.trip_timeau_hrs*0.5 --HOV2
			WHEN trip.trip_mode = 5 THEN trip.trip_timeau_hrs*0.3 --HOV3+
			--WHEN trip.trip_mode = 9 THEN trip.trip_timeau_hrs*0.6 --TNC
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV1px, @TNCAV1px) --SHOULD TNC *AUTO* TRAVEL TIME INCLUDE DEAD HEAD TRAVEL TIME?
				THEN trip.trip_timeau_hrs*@TNCDeadHeadFactor * 1 -- TNC with single passenger
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV2px, @TNCAV2px)
				THEN trip.trip_timeau_hrs*@TNCDeadHeadFactor * 0.5 -- TNC with 2 passengers
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV3px, @TNCAV3px)
				THEN trip.trip_timeau_hrs*@TNCDeadHeadFactor * 0.25 -- TNC with 3+ passengers
			ELSE 0 
		END AS travtimeau_hrs,
		CASE 
			WHEN trip.trip_mode = 3 THEN trip.trip_distau
			WHEN trip.trip_mode = 4 THEN trip.trip_distau*0.5
			WHEN trip.trip_mode = 5 THEN trip.trip_distau*0.3
			--WHEN trip.trip_mode = 9 THEN trip.trip_timeau_hrs*0.6 --TNC
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV1px, @TNCAV1px)
				THEN trip.trip_distau*@TNCDeadHeadFactor * 1 -- TNC with single passenger
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV2px, @TNCAV2px)
				THEN trip.trip_distau*@TNCDeadHeadFactor * 0.5 -- TNC with 2 passengers
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV3px, @TNCAV3px)
				THEN trip.trip_distau*@TNCDeadHeadFactor * 0.25 -- TNC with 3+ passengers
			ELSE 0 
		END AS distau2,
		CASE 
			WHEN trip.trip_mode = 3 THEN trip.trip_distcong
			WHEN trip.trip_mode = 4 THEN trip.trip_distcong*0.5
			WHEN trip.trip_mode = 5 THEN trip.trip_distcong*0.3
			--WHEN trip.trip_mode = 9 THEN trip.trip_timeau_hrs*0.6 --TNC
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV1px, @TNCAV1px)
				THEN trip.trip_distcong*@TNCDeadHeadFactor * 1 -- TNC with single passenger
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV2px, @TNCAV2px)
				THEN trip.trip_distcong*@TNCDeadHeadFactor * 0.5 -- TNC with 2 passengers
			WHEN trip.trip_mode = 9 AND trip.trip_dorp IN (@TNCNonAV3px, @TNCAV3px)
				THEN trip.trip_distcong*@TNCDeadHeadFactor * 0.25 -- TNC with 3+ passengers
			ELSE 0 
		END AS distcong2
	FROM #trip_temp trip

--============aggregate tour numbers requiring counting unique tours=========================
CREATE TABLE #tour_agg_temp (
	parcelid INT,
	PTO_TOT_RES INT,
	PTO_WRK_RES INT,
	PTOURSOV INT,
	PTOURHOV INT,
	PTOURTRN INT,
	PTOURBIK INT,
	PTOURWLK INT,
	PTOURSCB INT,
	PTOURTNC INT,
	WTOURSOV INT,
	WTOURHOV INT,
	WTOURTRN INT,
	WTOURBIK INT,
	WTOURWLK INT,
	WTOURTNC INT
	)

INSERT INTO #tour_agg_temp
	SELECT
		hh.hhparcel,
		COUNT(tour.id) AS PTO_TOT_RES,
		SUM(CASE WHEN pdpurp = 1 OR parent > 0 THEN 1 ELSE 0 END) AS PTO_WRK_RES, --work tours include subtours (parent > 0)
		SUM(CASE WHEN tmodetp = 3 THEN 1 ELSE 0 END) AS PTOURSOV,
		SUM(CASE WHEN tmodetp IN (4,5) THEN 1 ELSE 0 END) AS PTOURHOV,
		SUM(CASE WHEN tmodetp = 6 THEN 1 ELSE 0 END) AS PTOURTRN,
		SUM(CASE WHEN tmodetp = 2 THEN 1 ELSE 0 END) AS PTOURBIK,
		SUM(CASE WHEN tmodetp = 1 THEN 1 ELSE 0 END) AS PTOURWLK,
		SUM(CASE WHEN tmodetp = 8 THEN 1 ELSE 0 END) AS PTOURSCB,
		SUM(CASE WHEN tmodetp = 9 THEN 1 ELSE 0 END) AS PTOURTNC,
		SUM(CASE WHEN tmodetp = 3 AND (pdpurp = 1 OR parent > 0) THEN 1 ELSE 0 END) AS WTOURSOV,
		SUM(CASE WHEN tmodetp IN (4,5) AND (pdpurp = 1 OR parent > 0) THEN 1 ELSE 0 END) AS WTOURHOV,
		SUM(CASE WHEN tmodetp = 6 AND (pdpurp = 1 OR parent > 0) THEN 1 ELSE 0 END) AS WTOURTRN,
		SUM(CASE WHEN tmodetp = 2 AND (pdpurp = 1 OR parent > 0) THEN 1 ELSE 0 END) AS WTOURBIK,
		SUM(CASE WHEN tmodetp = 1 AND (pdpurp = 1 OR parent > 0) THEN 1 ELSE 0 END) AS WTOURWLK,
		SUM(CASE WHEN tmodetp = 9 AND (pdpurp = 1 OR parent > 0) THEN 1 ELSE 0 END) AS WTOURTNC
	FROM {raw_tour} tour --raw tour table
		JOIN {raw_hh} hh --raw hh table
			ON tour.hhno = hh.hhno
	GROUP BY hh.hhparceL

--==============get data summarizing VMT, etc at the work destination============

--get sums of VMT, veh trips, etc. at each TOPCL (primary tour origin pcls for work tour and work-related subtours)
SELECT
	tour.topcl, --tour destination parcel
	SUM(vt.distau2) AS VMT_wrk_topcl,
	SUM(vt.distcong2) AS CVMT_wrk_topcl,
	SUM(CASE 
			WHEN trip.trip_mode IN (3,4,5,9) AND trip.trip_dorp = 1
			THEN 1 ELSE 0 END)
		AS VT_wrk_topcl,
	COUNT(trip.parcelid) AS PT_wrk_topcl,
	SUM(CASE WHEN trip.trip_mode = 3 THEN 1 ELSE 0 END) AS SOV_wrk_topcl,
	SUM(CASE WHEN trip.trip_mode IN (4,5) THEN 1 ELSE 0 END) AS HOV_wrk_topcl,
	SUM(CASE WHEN trip.trip_mode = 6 THEN 1 ELSE 0 END) AS TRN_wrk_topcl,
	SUM(CASE WHEN trip.trip_mode = 2 THEN 1 ELSE 0 END) AS BIK_wrk_topcl,
	SUM(CASE WHEN trip.trip_mode = 1 THEN 1 ELSE 0 END) AS WLK_wrk_topcl,
	SUM(CASE WHEN trip.trip_mode = 9 THEN 1 ELSE 0 END) AS TNC_wrk_topcl
INTO #workenddata_topcl
FROM {raw_tour} tour --raw tour table 
	JOIN #trip_temp trip
		ON tour.id = trip.tour_id
	JOIN #vehicle_trips_temp vt
		ON trip.trip_id = vt.trip_id
WHERE tour.parent > 0 -- want VMT from work-based subtours, regardless of subtour purpose
GROUP BY tour.topcl

--get sums of VMT, veh trips, etc. at each TDPCL (primary tour destination pcls for work tour and work-related subtours)
SELECT
	tour.tdpcl, --tour destination parcel
	SUM(vt.distau2) AS VMT_wrk_tdpcl,
	SUM(vt.distcong2) AS CVMT_wrk_tdpcl,
	SUM(CASE 
			WHEN trip.trip_mode IN (3,4,5,9) AND trip.trip_dorp = 1
			THEN 1 ELSE 0 END)
		AS VT_wrk_tdpcl,
	COUNT(trip.parcelid) AS PT_wrk_tdpcl,
	SUM(CASE WHEN trip.trip_mode = 3 THEN 1 ELSE 0 END) AS SOV_wrk_tdpcl,
	SUM(CASE WHEN trip.trip_mode IN (4,5) THEN 1 ELSE 0 END) AS HOV_wrk_tdpcl,
	SUM(CASE WHEN trip.trip_mode = 6 THEN 1 ELSE 0 END) AS TRN_wrk_tdpcl,
	SUM(CASE WHEN trip.trip_mode = 2 THEN 1 ELSE 0 END) AS BIK_wrk_tdpcl,
	SUM(CASE WHEN trip.trip_mode = 1 THEN 1 ELSE 0 END) AS WLK_wrk_tdpcl,
	SUM(CASE WHEN trip.trip_mode = 9 THEN 1 ELSE 0 END) AS TNC_wrk_tdpcl
INTO #workenddata_tdpcl
FROM {raw_tour} tour --raw tour table 
	JOIN #trip_temp trip
		ON tour.id = trip.tour_id
	JOIN #vehicle_trips_temp vt
		ON trip.trip_id = vt.trip_id
WHERE tour.pdpurp = 1 --only want VMT for work tours ending at TDPCL
	AND tour.parent = 0 --want to exclude trips that are counted in subtour query
GROUP BY tour.tdpcl

--get total work VMT at each work tour end by summing VMT at each TDPCL and TOPCL
CREATE TABLE #workenddata (
	parcelid INT,
	VMT_wrk_tourend FLOAT,
	CVMT_wrk_tourend FLOAT,
	VT_wrk_tourend INT,
	PT_wrk_tourend INT,
	SOV_wrk_tourend INT,
	HOV_wrk_tourend INT,
	TRN_wrk_tourend INT,
	BIK_wrk_tourend INT,
	WLK_wrk_tourend INT,
	TNC_wrk_tourend INT
	)

INSERT INTO #workenddata
	SELECT
		pcl.parcelid,
		CASE WHEN o.VMT_wrk_topcl + d.VMT_wrk_tdpcl IS NULL THEN 0 
			ELSE o.VMT_wrk_topcl + d.VMT_wrk_tdpcl END,
		CASE WHEN o.CVMT_wrk_topcl + d.CVMT_wrk_tdpcl IS NULL THEN 0
			ELSE o.CVMT_wrk_topcl + d.CVMT_wrk_tdpcl END,
		CASE WHEN o.VT_wrk_topcl + d.VT_wrk_tdpcl IS NULL THEN 0
			ELSE o.VT_wrk_topcl + d.VT_wrk_tdpcl END,
		CASE WHEN o.PT_wrk_topcl + d.PT_wrk_tdpcl IS NULL THEN 0
			ELSE o.PT_wrk_topcl + d.PT_wrk_tdpcl END,
		CASE WHEN o.SOV_wrk_topcl + d.SOV_wrk_tdpcl IS NULL THEN 0
			ELSE o.SOV_wrk_topcl + d.SOV_wrk_tdpcl END,
		CASE WHEN o.HOV_wrk_topcl + d.HOV_wrk_tdpcl IS NULL THEN 0
			ELSE o.HOV_wrk_topcl + d.HOV_wrk_tdpcl END,
		CASE WHEN o.TRN_wrk_topcl + d.TRN_wrk_tdpcl IS NULL THEN 0
			ELSE o.TRN_wrk_topcl + d.TRN_wrk_tdpcl END,
		CASE WHEN o.BIK_wrk_topcl + d.BIK_wrk_tdpcl IS NULL THEN 0
			ELSE o.BIK_wrk_topcl + d.BIK_wrk_tdpcl END,
		CASE WHEN o.WLK_wrk_topcl + d.WLK_wrk_tdpcl IS NULL THEN 0
			ELSE o.WLK_wrk_topcl + d.WLK_wrk_tdpcl END,
		CASE WHEN o.TNC_wrk_topcl + d.TNC_wrk_tdpcl IS NULL THEN 0
			ELSE o.TNC_wrk_topcl + d.TNC_wrk_tdpcl END
	FROM {raw_parcel} pcl --raw scenario parcel table
		LEFT JOIN #workenddata_topcl o
			ON o.topcl = pcl.parcelid
		LEFT JOIN #workenddata_tdpcl d
			ON d.tdpcl = pcl.parcelid

--===========run the query and insert output into "triptour" theme table===========
SELECT
	pcl.parcelid,
	SUM(CASE WHEN trip.trip_id IS NULL THEN 0 ELSE 1 END) AS PT_TOT_RES,
	MAX(tour.PTO_TOT_RES) AS PTO_TOT_RES, --total tours made by residents, using MAX so we don't have to do GROUP BY for all of these.
	SUM(CASE WHEN trip.trip_mode IN (3,4,5,9) AND trip.trip_dorp = 1 THEN 1 ELSE 0 END) AS VT_TOT_RES, --veh trips
	SUM(CASE WHEN trip.pdpurp = 1 OR trip.parent > 0 THEN 1 ELSE 0 END) AS PT_WRK_RES, --person trips on work tours; include all subtours (parent tour > 0)
	MAX(tour.PTO_WRK_RES) AS PTO_WRK_RES, --total work tours made by residents
	SUM(CASE WHEN (trip.trip_mode IN (3,4,5,9) AND trip.trip_dorp = 1) AND (trip.pdpurp = 1 OR trip.parent > 0) 
		THEN 1 ELSE 0 END) AS VT_WRK_RES,--veh trips, work purpose or work subtour
	SUM(CASE WHEN trip.trip_mode = 3 THEN 1 ELSE 0 END) AS SOV_TOT_RES,
	SUM(CASE WHEN trip.trip_mode IN (4,5) THEN 1 ELSE 0 END) AS HOV_TOT_RES,
	SUM(CASE WHEN trip.trip_mode = 6 THEN 1 ELSE 0 END) AS TRN_TOT_RES,
	SUM(CASE WHEN trip.trip_pathtype = 3 THEN 1 ELSE 0 END) AS TRN_LBUS_RES,
	SUM(CASE WHEN trip.trip_pathtype = 4 THEN 1 ELSE 0 END) AS TRN_LRT_RES,
	SUM(CASE WHEN trip.trip_pathtype = 5 THEN 1 ELSE 0 END) AS TRN_EBUS_RES,
	SUM(CASE WHEN trip.trip_mode = 2 THEN 1 ELSE 0 END) AS BIK_TOT_RES,
	SUM(CASE WHEN trip.trip_mode = 1 THEN 1 ELSE 0 END) AS WLK_TOT_RES,
	SUM(CASE WHEN trip.trip_mode = 8 THEN 1 ELSE 0 END) AS SCB_TOT_RES,
	SUM(CASE WHEN trip.trip_mode = 9 THEN 1 ELSE 0 END) AS TNC_TOT_RES,
	SUM(CASE WHEN trip.trip_mode = 3 AND (trip.pdpurp = 1 OR trip.parent > 0) THEN 1 ELSE 0 END) AS SOV_WRK_RES,
	SUM(CASE WHEN trip.trip_mode IN (4,5) AND (trip.pdpurp = 1 OR trip.parent > 0) THEN 1 ELSE 0 END) AS HOV_WRK_RES,
	SUM(CASE WHEN trip.trip_mode = 6 AND (trip.pdpurp = 1 OR trip.parent > 0) THEN 1 ELSE 0 END) AS TRN_WRK_RES,
	SUM(CASE WHEN trip.trip_mode = 2 AND (trip.pdpurp = 1 OR trip.parent > 0) THEN 1 ELSE 0 END) AS BIK_WRK_RES,
	SUM(CASE WHEN trip.trip_mode = 1 AND (trip.pdpurp = 1 OR trip.parent > 0) THEN 1 ELSE 0 END) AS WLK_WRK_RES,
	SUM(CASE WHEN trip.trip_mode = 9 AND (trip.pdpurp = 1 OR trip.parent > 0) THEN 1 ELSE 0 END) AS TNC_WRK_RES,
	MAX(tour.PTOURSOV) AS PTOURSOV,
	MAX(tour.PTOURHOV) AS PTOURHOV,
	MAX(tour.PTOURTRN) AS PTOURTRN,
	MAX(tour.PTOURBIK) AS PTOURBIK,
	MAX(tour.PTOURWLK) AS PTOURWLK,
	MAX(tour.PTOURSCB) AS PTOURSCB,
	MAX(tour.PTOURTNC) AS PTOURTNC,
	MAX(tour.WTOURSOV) AS WTOURSOV,
	MAX(tour.WTOURHOV) AS WTOURHOV,
	MAX(tour.WTOURTRN) AS WTOURTRN,
	MAX(tour.WTOURBIK) AS WTOURBIK,
	MAX(tour.WTOURWLK) AS WTOURWLK,
	MAX(tour.WTOURTNC) AS WTOURTNC,
	SUM(vt.distau2)	AS II_VMT_RES,
	SUM(CASE WHEN trip.pdpurp = 1 OR trip.parent > 0 THEN vt.distau2 ELSE 0 END) AS VMT_WRK_RES,
	SUM(vt.distcong2) AS II_CVMT_RES,
	SUM(CASE WHEN trip.pdpurp = 1 OR trip.parent > 0 THEN vt.distcong2 ELSE 0 END) AS CVMT_WRK_RES,
	SUM(trip.trip_travtime_hrs)/60 AS PHR_TOT_RES, --total person hours of travel time
	SUM(vt.travtimeau_hrs) AS VHR_TOT_RES, --veh hours, without double counting due to carpooling
	SUM(CASE WHEN trip.pdpurp = 1 OR trip.parent > 0 THEN trip.trip_travtime_hrs ELSE 0 END) AS PHR_WRK_RES,
	SUM(CASE WHEN trip.pdpurp = 1 OR trip.parent > 0 THEN vt.travtimeau_hrs ELSE 0 END) AS VHR_WRK_RES,
	SUM(CASE
			WHEN trip_speed > 0 AND trip_speed <= 5 THEN vt.distau2*@emissions0to5
			WHEN trip_speed > 5 AND trip_speed <= 10 THEN vt.distau2*@emissions5to10
			WHEN trip_speed > 10 AND trip_speed <= 15 THEN vt.distau2*@emissions10to15
			WHEN trip_speed > 15 AND trip_speed <= 20 THEN vt.distau2*@emissions15to20
			WHEN trip_speed > 20 AND trip_speed <= 25 THEN vt.distau2*@emissions20to25
			WHEN trip_speed > 25 AND trip_speed <= 30 THEN vt.distau2*@emissions25to30
			WHEN trip_speed > 30 AND trip_speed <= 35 THEN vt.distau2*@emissions30to35
			WHEN trip_speed > 35 AND trip_speed <= 40 THEN vt.distau2*@emissions35to40
			WHEN trip_speed > 40 AND trip_speed <= 45 THEN vt.distau2*@emissions40to45
			WHEN trip_speed > 45 AND trip_speed <= 50 THEN vt.distau2*@emissions45to50
			WHEN trip_speed > 50 AND trip_speed <= 55 THEN vt.distau2*@emissions50to55
			WHEN trip_speed > 55 AND trip_speed <= 60 THEN vt.distau2*@emissions55to60
			WHEN trip_speed > 60 THEN vt.distau2*@emissionsOver60
		ELSE 0 END) AS GMI_TOT_RES,
	pcl.emptot_p*ewf.extWkrfraxn AS JOB_ExWorker,
	MAX(work.VMT_wrk_tourend) AS VMT_wrk_tourend,
	MAX(work.CVMT_wrk_tourend) AS CVMT_wrk_tourend,
	MAX(work.VT_wrk_tourend) AS VT_wrk_tourend,
	MAX(work.PT_wrk_tourend) AS PT_wrk_tourend,
	MAX(work.SOV_wrk_tourend) AS SOV_wrk_tourend,
	MAX(work.HOV_wrk_tourend) AS HOV_wrk_tourend,
	MAX(work.TRN_wrk_tourend) AS TRN_wrk_tourend,
	MAX(work.BIK_wrk_tourend) AS BIK_wrk_tourend,
	MAX(work.WLK_wrk_tourend) AS WLK_wrk_tourend,
	MAX(work.TNC_wrk_tourend) AS TNC_wrk_tourend
INTO {triptour_outtbl} --output triptour table name
FROM {raw_parcel} pcl --raw scenario parcel table
	LEFT JOIN #trip_temp trip
		ON pcl.parcelid = trip.parcelid
	LEFT JOIN #tour_agg_temp tour
		ON pcl.parcelid = tour.parcelid
	LEFT JOIN #vehicle_trips_temp vt
		ON vt.trip_id = trip.trip_id
	LEFT JOIN #workenddata work
		ON pcl.parcelid = work.parcelid
	LEFT JOIN {raw_ixworkerfraxn} ewf --ixworkerfraction table
		ON pcl.taz_p = ewf.TAZ
GROUP BY 
	pcl.parcelid,
	pcl.emptot_p*ewf.extWkrfraxn

--===============delete temporary tables=========================
DROP TABLE #trip_temp
DROP TABLE #vehicle_trips_temp
DROP TABLE #tour_agg_temp
DROP TABLE #workenddata
DROP TABLE #workenddata_topcl
DROP TABLE #workenddata_tdpcl


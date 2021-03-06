USE MTP2020

DECLARE @parcelid INT SET @parcelid = 61106967

SELECT
	tp.mode,
	SUM(tp.distau) AS sum_distau,
	SUM(tp.distcong) AS sum_distcong,
	COUNT(tdpcl) AS trip_cnt,
	tr.tdpcl
FROM raw_trip_testscen tp
	JOIN raw_tour_testscen tr
		ON tp.tour_id = tr.id
WHERE tr.tdpcl = @parcelid
	AND (tr.pdpurp = 1 OR tr.parent > 0)
GROUP BY tp.mode, tr.tdpcl


SELECT
	*
FROM ilut_triptourdata
WHERE parcelid = @parcelid
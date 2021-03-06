/*
Purpose: get aggregate values of ILUT variables at desired aggregation level (whole region, RAD, TAZ, block group, census tract, county, etc)
Comment/uncomment and edit the GROUP BY statement at the bottom as needed.

Following fields are weighted by population: mix index, mix-density index, half-mile employment, half-mile hhs, transit stop distances,
*/

USE MTP2024

SELECT
	SUM(WKR_NO_TELWK) AS WKR_NO_TELWK,
	SUM(WKR_TELWK_PART) AS WKR_TELWK_PART,
	SUM(WKR_TELWK_FULL) AS WKR_TELWK_FULL,
	SUM(VMT_WKR_NOTELWK) AS VMT_WKR_NOTELWK,
	SUM(VMT_TELWKR_PART) AS VMT_TELWKR_PART,
	SUM(VMT_TELWKR_FULL) AS VMT_TELWKR_FULL

FROM 
	ilut_combined2016_80082
	--group by county
	--order by county
	--group by JURIS
	--order by JURIS
    --GROUP BY RAD07_new
    --order by RAD07_new
	--group by comtype_bo
	--order by comtype_bo
    --group by PJOBC_NAME
    --group by tpa36_16
	--order by tpa36_16

  
--  select 
   
--	SUM(EMPEDU) AS EMPEDU,
--	SUM(EMPFOOD) AS EMPFOOD,
--	SUM(EMPGOV) AS EMPGOV,
--	SUM(EMPIND) AS EMPIND,
--	SUM(EMPMED) AS EMPMED,
--	SUM(EMPOFC) AS EMPOFC,
--	SUM(EMPOTH) AS EMPOTH,
--	SUM(EMPRET) AS EMPRET,
--	SUM(EMPSVC) AS EMPSVC,
--	SUM(HOMEEMP) AS HOMEEMP,
--	SUM(EMPTOT) AS EMPTOt
--from mtpuser.ilut_combined2035_100


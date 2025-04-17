--Creating Tables for HCAHPS --


-- Note, before you run this step, you should create a schema called Hopsital Data --
CREATE TABLE IF NOT EXISTS "postgres"."Hospital_Data".hospital_beds
(
     provider_ccn integer
    ,hospital_name character varying(255)
    ,fiscal_year_begin_date character varying(10)
    ,fiscal_year_end_date character varying(10)
    ,number_of_beds integer
);

CREATE TABLE IF NOT EXISTS "postgres"."Hospital_Data".HCAHPS_data
(
    facility_id character varying(10),
    facility_name character varying(255),
    address character varying(255),
    city character varying(50),
    state character varying(2),
    zip_code character varying(10),
    county_or_parish character varying(50),
    telephone_number character varying(20),
    hcahps_measure_id character varying(255),
    hcahps_question character varying(255),
    hcahps_answer_description character varying(255),
    hcahps_answer_percent integer,
    num_completed_surveys integer,
    survey_response_rate_percent integer,
    start_date character varying(10),
    end_date character varying(10)
);



-- Create table with clean data

CREATE TABLE "postgres"."Hospital_Data".Tableau_File as 

WITH hospital_beds_prep AS
(
SELECT LPAD(CAST(provider_ccn AS TEXT),6,'0') AS provider_ccn,
	   hospital_name,
	   TO_DATE(fiscal_year_begin_date,'MM/DD/YYYY') AS fiscal_year_begin_date,
	   TO_DATE(fiscal_year_end_date,'MM/DD/YYYY') AS fiscal_year_end_date,
	   number_of_beds,
	   ROW_NUMBER() OVER(PARTITION BY provider_ccn ORDER BY TO_DATE(fiscal_year_end_date,'MM/DD/YYYY') DESC) AS nth_row
FROM "postgres"."Hospital_Data".hospital_beds
)

SELECT LPAD(CAST(facility_id AS TEXT),6,'0') AS provider_ccn,
	   TO_DATE(start_date,'MM/DD/YYYY') AS start_date_converted,
	   TO_DATE(end_date,'MM/DD/YYYY') AS end_date_converted,
	   hcahps.*,
	   beds.number_of_beds,
	   beds.fiscal_year_begin_date AS beds_start_report_period,
	   beds.fiscal_year_end_date AS beds_end_report_period
FROM "postgres"."Hospital_Data".hcahps_data AS hcahps
LEFT JOIN hospital_beds_prep AS beds
	ON LPAD(CAST(facility_id AS TEXT),6,'0') = beds.provider_ccn
AND beds.nth_row = 1

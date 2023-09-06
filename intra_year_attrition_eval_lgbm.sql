-- intra-year attrition prediction evaluation
-- yes, this is all hard coded dates and years. i know it's not great, but considering the fact that
-- these dates are generally unreliable and can't be called without it sometimes just being wrong,
-- we're just gonna do it this way. 
WITH
enrolled AS (
	select 
	distinct fd.sa_scholar_id,
        CASE
            WHEN scholar_grade = 'K' THEN '0'
            ELSE scholar_grade
        END
	from sacs.fact_daily_scholar_status fd
	inner join sacs.dim_scholar_other_info ds
	on fd.sa_scholar_id = ds.sa_scholar_id 
	where ispreregistered = 'No' 
    and date_key = GETDATE()::DATE::VARCHAR
    and (last_sa_day is null or last_sa_day > GETDATE()::DATE::VARCHAR)
),
-- hard coding start and end dates because as of writing this, there is no table with correct dates
fdss AS (
    SELECT date_key,
        fdss.sa_scholar_id,
        enrolled.scholar_grade,
        school_cd,
        ell_status,
        sped_status,
        frpl_status,
        CASE
	        WHEN enrolled.scholar_grade = '0' THEN CAST('8-14-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '1' THEN CAST('8-16-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '2' THEN CAST('8-16-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '3' THEN CAST('8-14-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '4' THEN CAST('8-14-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '5' THEN CAST('8-14-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '6' THEN CAST('8-16-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '7' THEN CAST('8-16-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '8' THEN CAST('8-16-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '9' THEN CAST('8-7-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '10' THEN CAST('8-7-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '11' THEN CAST('8-7-2023' AS datetime)
	        WHEN enrolled.scholar_grade = '12' THEN CAST('8-7-2023' AS datetime)
	    END AS start_date,
	    CASE
	        WHEN enrolled.scholar_grade = '0' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '1' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '2' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '3' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '4' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '5' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '6' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '7' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '8' THEN CAST('6-21-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '9' THEN CAST('6-7-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '10' THEN CAST('6-7-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '11' THEN CAST('6-7-2024' AS datetime)
	        WHEN enrolled.scholar_grade = '12' THEN CAST('6-7-2024' AS datetime)
	    END AS end_date,
        GETDATE() AS run_date, -- should be GETDATE()-365 to get the date of the same time last year, otherwise replace with explicit dates for testing
        school_yr,
        attendance_status,
        excused
    FROM sacs.fact_daily_scholar_status fdss
    LEFT JOIN enrolled
    ON fdss.sa_scholar_id = enrolled.sa_scholar_id
    WHERE attendance_status != 'N/A'
    	AND date_key >= start_date
    	AND date_key <= end_date
    	AND date_key <= run_date
    	AND fdss.sa_scholar_id IN (SELECT sa_scholar_id FROM enrolled)
),
scholars AS (
    SELECT sa_scholar_id,
        scholar_grade,
    	start_date,
    	end_date,
        run_date,
        SUM(
            CASE
                WHEN attendance_status IN ('P') THEN 1
                ELSE 0
            END
        ) AS present_days,
        SUM(
            CASE
                WHEN attendance_status IN ('T') THEN 1
                ELSE 0
            END
        ) AS tardy_days,
        SUM(
            CASE
                WHEN attendance_status IN ('T', 'P') THEN 1
                ELSE 0
            END
        ) AS total_for_tardy,
        SUM(
            CASE
                WHEN attendance_status IN ('A')
                AND excused = 'False' THEN 1
                ELSE 0
            END
        ) AS absent_days,
        SUM(
            CASE
                WHEN attendance_status IN ('A')
                and excused = 'True' THEN 1
                ELSE 0
            END
        ) AS excused_absent_days,
        (
            tardy_days + present_days + excused_absent_days + absent_days
        ) AS total_days,
        CASE
            WHEN total_for_tardy > 0 THEN ROUND(tardy_days::FLOAT / total_for_tardy::FLOAT, 4)
            ELSE 0
        END AS tardy_percent,
        CASE
            WHEN total_days > 0 THEN ROUND(absent_days::FLOAT / total_days::FLOAT, 4)
            ELSE 0
        END AS absent_percent
    FROM fdss
    GROUP BY 1,
        2,
        3,
        4,
        5
    ORDER BY 1
),
fdss_dd_dup AS (
    SELECT sa_scholar_id,
        school_cd,
        ell_status,
        sped_status,
        frpl_status,
        date_key,
        run_date,
        ROW_NUMBER() OVER (
            PARTITION BY sa_scholar_id
            ORDER BY date_key DESC
        ) AS dupcnt_fdss
    FROM fdss
    WHERE date_key <= run_date::DATE
),
fdss_dd AS (
    SELECT *
    FROM fdss_dd_dup
    WHERE dupcnt_fdss = 1
),
fpna_dup AS (
    SELECT ioa_student_id AS sa_scholar_id,
        last_day_at_success_academy AS last_sa_day,
        ROW_NUMBER() OVER (
            PARTITION BY ioa_student_id
            ORDER BY last_day_at_success_academy DESC
        ) AS dupcnt_fpna
    FROM prod_scholar_enrollment_and_attrition.prod_scholar_enrollment_and_attrition_scholar_attrition_detailed_data_fpna
    WHERE school_year = '2023-2024'
),
fpna AS (
    SELECT *
    FROM fpna_dup
    WHERE dupcnt_fpna = 1
),
ser_dup AS (
    SELECT sa_scholar_id,
        school_yr AS school_year,
        new_v_returning,
        ROW_NUMBER() OVER (
            PARTITION BY sa_scholar_id
            ORDER BY date_key DESC
        ) AS dupcnt_ser
    FROM prod_scholar_enrollment_and_attrition.scholar_enrollment_raw psea
    WHERE school_year = '2023-2024'
),
ser AS (
    SELECT *
    FROM ser_dup
    WHERE dupcnt_ser = 1
),
-- scholar addresses
cd_dup AS (
    SELECT sa_scholar_id,
        address,
        ROW_NUMBER() OVER (
            PARTITION BY sa_scholar_id
            ORDER BY address
        ) AS dupcnt_cd
    FROM prod_scholar_contact.prod_scholar_contact_details
    WHERE sa_scholar_id IN (
            SELECT sa_scholar_id
            FROM scholars
        )
),
cd AS (
    SELECT sa_scholar_id,
        address
    FROM cd_dup
    WHERE dupcnt_cd = 1
),
ct_alpha AS (
    SELECT sa_scholar_id,
        CASE
            WHEN transit_time = '' THEN NULL
            ELSE transit_time
        END AS transit_time,
        CASE
            WHEN walking_time = '' THEN NULL
            ELSE walking_time
        END AS walking_time
    FROM raw_data_science.raw_scholar_app_commute_time_seconds
),
ct AS (
    SELECT sa_scholar_id,
        CASE
            WHEN transit_time IS NOT NULL THEN transit_time::FLOAT
            ELSE walking_time::FLOAT
        END AS commute_time
    FROM ct_alpha
),
dsoi AS (
    SELECT sa_scholar_id,
        gender
    FROM sacs.dim_scholar_other_info
    WHERE sa_scholar_id IN (
            SELECT sa_scholar_id
            FROM scholars
        )
),
-- number of incidents recorded during time period, restricted to time duration of interest
fs1 AS (
	SELECT
		idnumber AS sa_scholar_id,
		COUNT(idnumber) AS count_rep
	FROM sacs.fact_suspension
	WHERE incidenttype_nm LIKE 'REPRIMAND%'
	AND incident_dt <= GETDATE()
	AND incident_dt >= '8-7-2023'
	GROUP BY 1
),
-- number of suspensions recorded during time period, restricted to time duration of interest
fs2 AS (
	SELECT
		idnumber AS sa_scholar_id,
		COUNT(idnumber) AS count_sus
	FROM sacs.fact_suspension
	WHERE incidenttype_nm LIKE 'SUSPENSION%'
	AND incident_dt <= GETDATE()
	AND incident_dt >= '8-8-2023'
	GROUP BY 1
),
final AS (
    SELECT scholars.sa_scholar_id,
        last_sa_day,
        CASE
            WHEN new_v_returning = 'New' OR scholar_grade = '0' THEN True
            WHEN new_v_returning = 'Returning' AND scholar_grade != '0' THEN False
            WHEN (
                new_v_returning IS NULL
                AND scholar_grade = '1'
            ) THEN True
            WHEN (
                new_v_returning IS NULL
                AND scholar_grade = '2'
            ) THEN True
            WHEN (
                new_v_returning IS NULL
                AND scholar_grade = '3'
            ) THEN True
            WHEN (
                new_v_returning IS NULL
                AND scholar_grade = '4'
            ) THEN True
            ELSE False
        END AS new_scholar,
        scholars.start_date,
        scholars.end_date,
        scholars.run_date,
        CASE
            WHEN (scholar_grade = '12' AND last_sa_day+1 < end_date) THEN True
            WHEN (scholar_grade != '12' AND last_sa_day < end_date) THEN True
            -- Seniors have this weird problem where their last_sa_day is the day before end_date
            ELSE False
        END AS attrited,
        CASE
            WHEN gender = 'Female' THEN True
            ELSE False
        END AS gender_female,
        ell_status,
        sped_status,
        frpl_status,
        tardy_percent,
        absent_percent,
        total_days,
        CASE 
			WHEN count_rep IS NULL 
				THEN 0 
			ELSE count_rep 
		END AS total_rep,
		CASE 
			WHEN count_sus IS NULL 
				THEN 0 
			ELSE count_sus 
		END AS total_sus,
        scholar_grade,
        school_cd AS school_name,
        address,
        CASE
            WHEN commute_time <= 599 THEN 1
            WHEN commute_time > 600
            AND commute_time <= 1199 THEN 2
            WHEN commute_time > 1200
            AND commute_time <= 1799 THEN 3
            WHEN commute_time > 1800
            AND commute_time <= 2399 THEN 4
            WHEN commute_time > 2400
            AND commute_time <= 2999 THEN 5
            WHEN commute_time > 3000
            AND commute_time <= 3599 THEN 6
            ELSE 7
        END AS commute,
        ROW_NUMBER() OVER (
            PARTITION BY scholars.sa_scholar_id
            ORDER BY start_date
        ) AS dupcnt_final
    FROM scholars
        LEFT JOIN fpna ON scholars.sa_scholar_id = fpna.sa_scholar_id
        LEFT JOIN cd ON scholars.sa_scholar_id = cd.sa_scholar_id
        LEFT JOIN dsoi ON scholars.sa_scholar_id = dsoi.sa_scholar_id
        LEFT JOIN ct ON scholars.sa_scholar_id = ct.sa_scholar_id
        LEFT JOIN fdss_dd ON scholars.sa_scholar_id = fdss_dd.sa_scholar_id
        LEFT JOIN ser ON scholars.sa_scholar_id = ser.sa_scholar_id
        LEFT JOIN fs1 ON scholars.sa_scholar_id = fs1.sa_scholar_id
        LEFT JOIN fs2 ON scholars.sa_scholar_id = fs2.sa_scholar_id
    WHERE (last_sa_day IS NULL) OR (last_sa_day >= start_date + 7)
    -- the 7 is to account for the weekend. the definition of intra year attrition requires the scholar 
    -- did not attrit in the first 5 days of their grades school year
    ORDER BY 1
)
SELECT *
FROM final WHERE dupcnt_final = 1
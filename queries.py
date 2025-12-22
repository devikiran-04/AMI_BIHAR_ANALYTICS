# --------------------------------------------------
# MDMS STAGE COUNTS
# --------------------------------------------------
MDMS_STAGE_COUNTS = """
SELECT DATE(meter_time) AS dt, hes_vendor_name, 'LP' AS profile,
       COUNT(DISTINCT meter_number) AS cnt
FROM mdms.mdms_csv_lp
WHERE status='Success'
  AND status_message='Successfully Completed'
  AND meter_time BETWEEN %(start)s AND %(end)s
GROUP BY DATE(meter_time), hes_vendor_name

UNION ALL

SELECT DATE(meter_time), hes_vendor_name, 'ED',
       COUNT(DISTINCT meter_number)
FROM mdms.mdms_csv_ed
WHERE status='Success'
  AND status_message='Successfully Completed'
  AND meter_time BETWEEN %(start)s AND %(end)s
GROUP BY DATE(meter_time), hes_vendor_name
ORDER BY dt, hes_vendor_name;
"""

# --------------------------------------------------
# MDMS BLP VENDOR
# --------------------------------------------------
MDMS_BLP_VENDOR = """
WITH c AS (
  SELECT m.mtr_number mno, v.vendor_name, hi.lp_date, hi.lp_time,
         hi.response_time rt, 1 tbl
  FROM cdb.meter_master m
  JOIN mdms.mdm_loadprofile_data hi ON m.mtr_number=hi.mtr_number
  JOIN cdb.hes_vendor_m v ON m.mtr_entry_type=v.id
  WHERE hi.lp_date=%(dt)s AND hi.record_status=1
  UNION ALL
  SELECT m.mtr_number, v.vendor_name, hi.lp_date, hi.lp_time,
         hi.create_date, 2
  FROM cdb.meter_master m
  JOIN mdms.mdm_loadprofile_data_invalid hi ON m.mtr_number=hi.mtr_number
  JOIN cdb.hes_vendor_m v ON m.mtr_entry_type=v.id
  WHERE hi.lp_date=%(dt)s AND hi.record_status=1
),
r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY mno, lp_time ORDER BY tbl, rt) rnk
  FROM c
)
SELECT lp_date, vendor_name,
       COUNT(DISTINCT mno) meters,
       COUNT(*) FILTER (WHERE tbl=1) valid_blocks,
       COUNT(*) FILTER (WHERE tbl=2) invalid_blocks
FROM r WHERE rnk=1
GROUP BY lp_date, vendor_name
ORDER BY vendor_name;
"""

# --------------------------------------------------
# MDMS DLP VENDOR
# --------------------------------------------------
MDMS_DLP_VENDOR = """
WITH c AS (
  SELECT m.mtr_number mno, v.vendor_name, hi.bill_date,
         hi.response_time rt, 1 tbl
  FROM cdb.meter_master m
  JOIN mdms.mdm_energy_data hi ON m.mtr_number=hi.mtr_number
  JOIN cdb.hes_vendor_m v ON m.mtr_entry_type=v.id
  WHERE hi.bill_date=%(dt)s AND hi.record_status=1
  UNION ALL
  SELECT m.mtr_number, v.vendor_name, hi.bill_date,
         hi.create_date, 2
  FROM cdb.meter_master m
  JOIN mdms.mdm_energy_data_invalid hi ON m.mtr_number=hi.meter_number
  JOIN cdb.hes_vendor_m v ON m.mtr_entry_type=v.id
  WHERE hi.bill_date=%(dt)s AND hi.record_status=1
),
r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY mno ORDER BY tbl, rt) rnk
  FROM c
)
SELECT bill_date, vendor_name,
       COUNT(DISTINCT mno) total_reads,
       COUNT(*) FILTER (WHERE tbl=1) valid_reads,
       COUNT(*) FILTER (WHERE tbl=2) invalid_reads
FROM r WHERE rnk=1
GROUP BY bill_date, vendor_name
ORDER BY vendor_name;
"""

# --------------------------------------------------
# HES PROFILE COUNTS
# --------------------------------------------------
HES_PROFILE_COUNTS = """
SELECT %(dt)s::date dt,
(SELECT COUNT(DISTINCT meter_number) FROM fep.fep_csv_ed
 WHERE meter_time BETWEEN %(start)s AND %(end)s AND status='Success') ed,
(SELECT COUNT(DISTINCT meter_number) FROM fep.fep_csv_lp
 WHERE meter_time BETWEEN %(start)s AND %(end)s AND status='Success') lp,
(SELECT COUNT(DISTINCT meter_number) FROM fep.fep_csv_instant
 WHERE meter_time BETWEEN %(start)s AND %(end)s AND status='Success') inst,
(SELECT COUNT(DISTINCT meter_number) FROM fep.fep_csv_eob_ed
 WHERE meter_time BETWEEN %(start)s AND %(end)s AND status='Success') eob;
"""

# --------------------------------------------------
# DB SIZE
# --------------------------------------------------
DB_SIZE = "SELECT pg_size_pretty(pg_database_size('nbpdcl_db')) AS db_size;"

# --------------------------------------------------
# SPM BILLING
# --------------------------------------------------
SPM_BILLING = """
WITH d AS (
  SELECT (enc.bill_date-INTERVAL '1 day')::date dt,
         COUNT(DISTINCT enc.mtr_number) ed_cnt,
         COUNT(DISTINCT b.meter_number) billing_cnt
  FROM spm.prepaid_consdetails_t a
  JOIN spm.mdm_energy_data_clone enc ON a.meter_number=enc.mtr_number
  LEFT JOIN spm.spm_bill_details_t b
         ON b.meter_number=a.meter_number
        AND (b.blissue_dt+INTERVAL '1 day')=enc.bill_date
  WHERE enc.bill_date::date BETWEEN %(from_date)s AND %(to_date)s
  GROUP BY dt
),
p AS (
  SELECT create_date::date dt,
         COUNT(DISTINCT meter_number) total_prepaid,
         COUNT(DISTINCT meter_number)
         FILTER (WHERE billing_status='BS') bs
  FROM spm.prepaid_consdetails_t
  GROUP BY dt
)
SELECT d.dt, p.total_prepaid, p.bs bill_stopped,
       (p.total_prepaid-p.bs) live_meters,
       d.ed_cnt, d.billing_cnt,
       (d.ed_cnt-d.billing_cnt) un_billed_cnt
FROM d JOIN p USING(dt)
ORDER BY dt;
"""

# --------------------------------------------------
# KETTLE JOBS
# --------------------------------------------------
KETTLE_LP_PENDING = "SELECT * FROM mdms.mdms_vee_kettle_job_integ_lp WHERE record_status=1"
KETTLE_ED_PENDING = "SELECT * FROM mdms.mdms_vee_kettle_job_integ_ed WHERE record_status=1"
KETTLE_EOB_PENDING = "SELECT * FROM mdms.mdms_vee_kettle_job_integ_eob WHERE record_status=1"

# --------------------------------------------------
# NFMS QUERIES (YYYYMMDD INT)
# --------------------------------------------------
nfms_LP = """
WITH trans AS (
    SELECT 
        lp_date,
        COUNT(DISTINCT mtr_number) AS trans_mtr_cnt
    FROM mdms.mdm_loadprofile_data
    WHERE lp_date BETWEEN %(from_date)s AND %(to_date)s
      AND mtr_number IN (
          SELECT meter_number
          FROM mdms.mdm_network_hierarchy
          WHERE record_status = 1
            AND element_type = 'F'
            AND meter_number IS NOT NULL
      )
      -- ensures full day LP (48 blocks)
      AND lp_time IS NOT NULL
    GROUP BY lp_date
),
nfms AS (
    SELECT 
        lp_date,
        COUNT(DISTINCT meter_number) AS nfms_mtr_cnt
    FROM nfms.block_loadprofile_data
    WHERE lp_date BETWEEN %(from_date)s AND %(to_date)s
    GROUP BY lp_date
),
audit AS (
    SELECT 
        TO_CHAR(meter_time,'YYYYMMDD')::int AS lp_date,
        COUNT(DISTINCT feeder_code) AS aud_fdr_cnt
    FROM nfms.audit_log
    WHERE profile_name = 'BlockLoad'
      AND TO_CHAR(meter_time,'YYYYMMDD')::int 
          BETWEEN %(from_date)s AND %(to_date)s
    GROUP BY TO_CHAR(meter_time,'YYYYMMDD')::int
)
SELECT 
    trans.lp_date,
    trans.trans_mtr_cnt,
    nfms.nfms_mtr_cnt,
    audit.aud_fdr_cnt
FROM trans
LEFT JOIN nfms  ON trans.lp_date = nfms.lp_date
LEFT JOIN audit ON trans.lp_date = audit.lp_date
ORDER BY trans.lp_date DESC;
"""

nfms_ED = """
WITH trans AS (
    SELECT 
        bill_date,
        COUNT(DISTINCT mtr_number) AS trans_mtr_cnt
    FROM mdms.mdm_energy_data
    WHERE d_type = 1
      AND bill_date BETWEEN %(from_date)s AND %(to_date)s
      AND mtr_number IN (
          SELECT meter_number
          FROM mdms.mdm_network_hierarchy
          WHERE record_status = 1
            AND element_type = 'F'
            AND meter_number IS NOT NULL
      )
    GROUP BY bill_date
),
nfms AS (
    SELECT 
        lp_date,
        COUNT(DISTINCT meter_number) AS nfms_mtr_cnt
    FROM nfms.daily_loadprofile_data
    WHERE lp_date BETWEEN %(from_date)s AND %(to_date)s
    GROUP BY lp_date
),
audit AS (
    SELECT 
        TO_CHAR(meter_time,'YYYYMMDD')::int AS lp_date,
        COUNT(DISTINCT feeder_code) AS aud_fdr_cnt
    FROM nfms.audit_log
    WHERE profile_name = 'DailyProfile'
      AND TO_CHAR(meter_time,'YYYYMMDD')::int 
          BETWEEN %(from_date)s AND %(to_date)s
    GROUP BY TO_CHAR(meter_time,'YYYYMMDD')::int
)
SELECT 
    trans.bill_date,
    trans.trans_mtr_cnt,
    nfms.nfms_mtr_cnt,
    audit.aud_fdr_cnt
FROM trans
LEFT JOIN nfms  ON trans.bill_date = nfms.lp_date
LEFT JOIN audit ON trans.bill_date = audit.lp_date
ORDER BY trans.bill_date DESC;
"""

nfms_EVENTS = """
WITH trans AS (
    SELECT 
        event_date AS lp_date,
        COUNT(DISTINCT meter_number) AS trans_mtr_cnt
    FROM mdms.mdm_event_details
    WHERE event_date BETWEEN %(from_date)s AND %(to_date)s
      AND meter_number IN (
          SELECT meter_number
          FROM mdms.mdm_network_hierarchy
          WHERE record_status = 1
            AND element_type = 'F'
            AND meter_number IS NOT NULL
      )
    GROUP BY event_date
),
nfms AS (
    SELECT 
        TO_CHAR(eventdatetime,'YYYYMMDD')::int AS lp_date,
        COUNT(DISTINCT meter_number) AS nfms_mtr_cnt
    FROM nfms.events_data
    WHERE TO_CHAR(eventdatetime,'YYYYMMDD')::int
          BETWEEN %(from_date)s AND %(to_date)s
    GROUP BY TO_CHAR(eventdatetime,'YYYYMMDD')::int
),
audit AS (
    SELECT 
        TO_CHAR(meter_time,'YYYYMMDD')::int AS lp_date,
        COUNT(DISTINCT feeder_code) AS aud_fdr_cnt
    FROM nfms.audit_log
    WHERE profile_name = 'DeviceEvent'
      AND TO_CHAR(meter_time,'YYYYMMDD')::int
          BETWEEN %(from_date)s AND %(to_date)s
    GROUP BY TO_CHAR(meter_time,'YYYYMMDD')::int
)
SELECT 
    trans.lp_date,
    trans.trans_mtr_cnt,
    nfms.nfms_mtr_cnt,
    audit.aud_fdr_cnt
FROM trans
LEFT JOIN nfms  ON trans.lp_date = nfms.lp_date
LEFT JOIN audit ON trans.lp_date = audit.lp_date
ORDER BY trans.lp_date DESC;
"""

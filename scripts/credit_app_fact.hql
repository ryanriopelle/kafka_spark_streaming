----------------------------------------------------------------------------------------------------
-- Script: credit_app_fact.hql
--
-- Description
--
-- Build fact table for Kyvos cube. This script brings credit application data.
-- The cube will be used for EAGLE Credit reporting dashboards
--
-- MODIFICATION HISTORY
-- -------------------------------------------------------------------------------------------------
-- Paritosh Rohilla   Initial Revision                                             05/09/2018
-- Paritosh Rohilla   Changed dedup logic for all tables based on Marie's reco.    06/05/2018
--                    Fixed partitioning logic to now pull all app nums for affected                  
--                    partitions determined by the driver logic.                                      
----------------------------------------------------------------------------------------------------

set tez.am.resource.memory.mb=6144;
set hive.exec.parallel=true;
set hive.exec.orc.split.strategy=BI;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=40000;
set hive.exec.dynamic.partition.mode=non-strict;
set hive.fetch.task.conversion=none;
set hive.optimize.sort.dynamic.partition=true;
set hive.tez.auto.reducer.parallelism = true;
set hive.tez.container.size=6144;
set hive.tez.java.opts=-Xmx4912m;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;

USE fin_dssseagle_${ENV}_tbls;

-- Get min & max timestamp ranges
DROP TABLE IF EXISTS fin_sseagle_${ENV}_tbls.tmp_credit_app_snapshot_min;

CREATE TABLE fin_sseagle_${ENV}_tbls.tmp_credit_app_snapshot_min AS 
SELECT file_commit_ts_max AS file_commit_ts 
  FROM (SELECT t2.*
          FROM (SELECT t1.*,
                       ROW_NUMBER() OVER(PARTITION BY file_commit_ts_min, file_commit_ts_max, create_ts
                                             ORDER BY create_ts DESC) rnum
                  FROM credit_app_fact_snapshot t1) t2
        WHERE rnum = 1) s;

DROP TABLE IF EXISTS fin_sseagle_${ENV}_tbls.tmp_credit_app_snapshot_max;

CREATE TABLE fin_sseagle_${ENV}_tbls.tmp_credit_app_snapshot_max AS 
SELECT CAST(MAX(FROM_UNIXTIME(UNIX_TIMESTAMP(file_commit_timestamp, 'yyyyMMddHHmmss'))) AS TIMESTAMP) AS file_commit_ts
  FROM fin_eagle_${ENV}_tbls.credit_application;

-- Create the fact table for the first run or when new cols. are added
-- DROP TABLE IF EXISTS credit_app_fact;

CREATE TABLE IF NOT EXISTS credit_app_fact (
       cca_app_num string,
       cca_type string,
       cca_status string,
       cca_previous string,
       cca_city string,
       cca_state string,
       cca_zip string,
       cca_market string,
       cca_order_type string,
       cca_current_user string,
       cca_user_name string,
       cca_timestamp timestamp,
       cca_num_of_phones double,
       cca_agent_code string,
       cca_region_ind string,
       cca_existing_mobile string,
       cca_conversion_mobile string,
       cca_cust_id_no string,
       cca_location_code string,
       cca_language_ind string,
       cca_status_desc string,
       ccd_seq_num string,
       ccd_user_name string,
       ccd_timestamp timestamp,
       ccd_reason string,
       ccd_credit_class_s string,
       ccd_deposit_s string,
       ccd_credit_class_i string,
       ccd_deposit_i string,
       ccd_status string,
       ccd_bureau string,
       ccd_reason_desc string,
       num_lines_apprvd bigint,
       deposit_lvl string,
       ccd_credit_class_desc string,
       dpp_down_pmt_reqd string,
       spend_limit string,
       bta_limit string,
       textapplicationdate timestamp,
       zip5 string,
       source_system string,
       region_ind string,
       create_timestamp timestamp,
       ip_address string,
       email_address string,
       upgrade_indicator string,
       connexus_ind_key string,
       connexus_address_key string,
       connexus_household_key string,
       connexus_confidence_code string,
       wave_pre_qual string,
       efx_id string,
       efx_global_id string,
       efx_domestic_id string,
       efx_parent_id string,
       efx_confidence_code string,
       efx_legal_entity_num string,
       efx_legal_entity_name string,
       wave_pre_qual_reason string,
       orig_app_for_dup string,
       orig_chan_name_for_dup string,
       orig_vision_outlet_for_dup string,
       prequal_app string,
       m2m_indicator string,
       m2m_lines_requested string,
       m2m_lines_eligible string,
       blended_score_app string,
       csc_timestamp string,
       model_name string,
       final_score string,
       s_credit_class string,
       s_deposit string,
       i_credit_class string,
       i_deposit string,
       credit_bureau string,
       cgsa string,
       update_timestamp string,
       vzt_step_down_flag string,
       nitp_score string,
       nitp_rc_1 string,
       nitp_rc_2 string,
       nitp_rc_3 string,
       nitp_rc_4 string,
       nitp_rc_5 string,
       cca_file_commit_ts timestamp)
PARTITIONED BY (app_ts_dt_hr STRING)
STORED AS ORC TBLPROPERTIES ("orc.compress"="SNAPPY");

-- Build the fact table
INSERT OVERWRITE TABLE credit_app_fact PARTITION (app_ts_dt_hr)
SELECT a.cca_app_num
      ,a.cca_type
      ,a.cca_status
      ,a.cca_previous
      ,a.cca_city
      ,a.cca_state
      ,a.cca_zip
      ,a.cca_market
      ,a.cca_order_type
      ,a.cca_current_user
      ,a.cca_user_name
      ,FROM_UNIXTIME(UNIX_TIMESTAMP(a.cca_timestamp, 'MM/dd/yyyy HH:mm:ss'))
      ,COALESCE(CAST(a.cca_num_of_phones AS DOUBLE),0)
      ,a.cca_agent_code
      ,a.cca_region_ind
      ,a.cca_existing_mobile
      ,a.cca_conversion_mobile
      ,a.cca_cust_id_no
      ,a.cca_location_code
      ,a.cca_language_ind
      ,statusref.cca_status_desc
      ,d.ccd_seq_num
      ,d.ccd_user_name
      ,FROM_UNIXTIME(UNIX_TIMESTAMP(d.ccd_timestamp, 'MM/dd/yyyy HH:mm:ss'))
      ,d.ccd_reason
      ,d.ccd_credit_class_s
      ,d.ccd_deposit_s
      ,d.ccd_credit_class_i
      ,d.ccd_deposit_i
      ,d.ccd_status
      ,d.ccd_bureau
      ,reasonref.ccd_reason_desc
      ,credclassref.num_lines_apprvd		
      ,credclassref.deposit_lvl	
      ,credclassref.ccd_credit_class_desc	
      ,credclassref.dpp_down_pmt_reqd	
      ,credclassref.spend_limit	
      ,credclassref.bta_limit
      ,FROM_UNIXTIME(UNIX_TIMESTAMP(a.cca_timestamp, 'MM/dd/yyyy HH:mm:ss')) AS textapplicationdate
      ,substr(a.cca_zip,1,5) AS zip5
      ,f.source_system
      ,f.region_ind
      ,FROM_UNIXTIME(UNIX_TIMESTAMP(f.create_timestamp, 'MM/dd/yyyy HH:mm:ss'))
      ,f.ip_address
      ,f.email_address
      ,f.upgrade_indicator
      ,f.connexus_ind_key
      ,f.connexus_address_key
      ,f.connexus_household_key
      ,f.connexus_confidence_code
      ,f.wave_pre_qual
      ,f.efx_id
      ,f.efx_global_id
      ,f.efx_domestic_id
      ,f.efx_parent_id
      ,f.efx_confidence_code
      ,f.efx_legal_entity_num
      ,f.efx_legal_entity_name
      ,f.wave_pre_qual_reason
      ,f.orig_app_for_dup
      ,f.orig_chan_name_for_dup
      ,f.orig_vision_outlet_for_dup
      ,f.prequal_app
      ,f.m2m_indicator
      ,f.m2m_lines_requested
      ,f.m2m_lines_eligible
      ,f.blended_score_app
      ,FROM_UNIXTIME(UNIX_TIMESTAMP(s.csc_timestamp, 'MM/dd/yyyy HH:mm:ss'))
      ,s.model_name
      ,s.final_score
      ,s.s_credit_class
      ,s.s_deposit
      ,s.i_credit_class
      ,s.i_deposit
      ,s.credit_bureau
      ,s.cgsa
      ,FROM_UNIXTIME(UNIX_TIMESTAMP(s.update_timestamp, 'MM/dd/yyyy HH:mm:ss'))
      ,s.vzt_step_down_flag
      ,s.nitp_score
      ,s.nitp_rc_1
      ,s.nitp_rc_2
      ,s.nitp_rc_3
      ,s.nitp_rc_4
      ,s.nitp_rc_5
      ,FROM_UNIXTIME(UNIX_TIMESTAMP(a.file_commit_timestamp, 'yyyyMMddHHmmss'))
      ,a.app_ts_dt_hr
 FROM (SELECT t2.*
         FROM (SELECT t1.*,
                      d.app_ts_dt_hr,
                      ROW_NUMBER() OVER(PARTITION BY cca_app_num
                                            ORDER BY UNIX_TIMESTAMP(vzwdb_update_timestamp, 'MM/dd/yyyy HH:mm:ss') DESC,
                                                     UNIX_TIMESTAMP(file_commit_timestamp, 'yyyyMMddHHmmss') DESC) AS rnum
                 FROM (SELECT DISTINCT CONCAT(SUBSTR(ca.cca_timestamp,7,4), SUBSTR(ca.cca_timestamp,1,2), 
                                              SUBSTR(ca.cca_timestamp,4,2), SUBSTR(ca.cca_timestamp,12,2)) AS app_ts_dt_hr
                         FROM fin_eagle_${ENV}_tbls.credit_application ca,
                              fin_sseagle_${ENV}_tbls.tmp_credit_app_snapshot_min min,
                              fin_sseagle_${ENV}_tbls.tmp_credit_app_snapshot_max max
                        WHERE UNIX_TIMESTAMP(ca.file_commit_timestamp, 'yyyyMMddHHmmss') > UNIX_TIMESTAMP(min.file_commit_ts)
                          AND UNIX_TIMESTAMP(ca.file_commit_timestamp, 'yyyyMMddHHmmss') <= UNIX_TIMESTAMP(max.file_commit_ts)) d,
                      fin_eagle_${ENV}_tbls.credit_application t1
                WHERE d.app_ts_dt_hr = CONCAT(SUBSTR(t1.cca_timestamp,7,4), SUBSTR(t1.cca_timestamp,1,2),
                                       SUBSTR(t1.cca_timestamp,4,2), SUBSTR(t1.cca_timestamp,12,2)) )t2
       WHERE rnum = 1) a
INNER JOIN 
      (SELECT t2.*
         FROM (SELECT t1.*,
                      ROW_NUMBER() OVER(PARTITION BY ccd_app_num
                                            ORDER BY ccd_seq_num DESC, 
                                                     UNIX_TIMESTAMP(ccd_timestamp, 'MM/dd/yyyy HH:mm:ss') DESC,
                                                     UNIX_TIMESTAMP(vzwdb_create_timestamp, 'MM/dd/yyyy HH:mm:ss') DESC,
                                                     UNIX_TIMESTAMP(vzwdb_update_timestamp, 'MM/dd/yyyy HH:mm:ss') DESC,
                                                     UNIX_TIMESTAMP(file_commit_timestamp, 'yyyyMMddHHmmss') DESC) AS rnum
                 FROM fin_eagle_${ENV}_tbls.credit_decision t1) t2
       WHERE rnum = 1) d
   ON a.cca_app_num = d.ccd_app_num
 LEFT OUTER JOIN
      (SELECT t2.*
         FROM (SELECT t1.*,
                      ROW_NUMBER() OVER(PARTITION BY app_num
                                            ORDER BY UNIX_TIMESTAMP(vzwdb_create_timestamp, 'MM/dd/yyyy HH:mm:ss') DESC,
                                                     UNIX_TIMESTAMP(vzwdb_update_timestamp, 'MM/dd/yyyy HH:mm:ss') DESC,
                                                     UNIX_TIMESTAMP(file_commit_timestamp, 'yyyyMMddHHmmss') DESC) AS rnum
                 FROM fin_eagle_${ENV}_tbls.credit_app_fraud_dtl t1) t2
       WHERE rnum = 1) f
   ON a.cca_app_num = f.app_num
 LEFT OUTER JOIN
      (SELECT t2.*
         FROM (SELECT t1.*,
                      ROW_NUMBER() OVER(PARTITION BY app_num
                                            ORDER BY UNIX_TIMESTAMP(COALESCE(update_timestamp, vzwdb_update_timestamp), 'MM/dd/yyyy HH:mm:ss') DESC,
                                                     UNIX_TIMESTAMP(file_commit_timestamp, 'yyyyMMddHHmmss') DESC) AS rnum
                 FROM fin_eagle_${ENV}_tbls.scorecard t1) t2
       WHERE rnum = 1) s
   ON a.cca_app_num = s.app_num
 LEFT OUTER JOIN
      fin_dssseagle_${ENV}_tbls.cca_status statusref
   ON a.cca_status = statusref.cca_status
 LEFT OUTER JOIN
      fin_dssseagle_${ENV}_tbls.ccd_reason reasonref
   ON d.ccd_reason = reasonref.ccd_reason
 LEFT OUTER JOIN 
      fin_dssseagle_${ENV}_tbls.ccd_credit_class credclassref
   ON d.ccd_credit_class_s = credclassref.ccd_credit_class;

-- Insert current batch pull into the snapshot table
INSERT INTO TABLE fin_dssseagle_${ENV}_tbls.credit_app_fact_snapshot_hist
SELECT mn.file_commit_ts AS file_commit_ts_min,
       mx.file_commit_ts AS file_commit_ts_max,
       CURRENT_TIMESTAMP AS create_ts
  FROM fin_sseagle_${ENV}_tbls.tmp_credit_app_snapshot_min mn,
       fin_sseagle_${ENV}_tbls.tmp_credit_app_snapshot_max mx;

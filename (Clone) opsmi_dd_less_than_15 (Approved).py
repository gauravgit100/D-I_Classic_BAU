# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC                                          1: Change History
# MAGIC  
# MAGIC | VER  | DATE       | DEVELOPER            | DESCRIPTION                                                 |
# MAGIC |------|------------|----------------------|-------------------------------------------------------------|
# MAGIC | 1.00 | 2025-12-08 | Gaurav Kumar      | Initial version                                             |
# MAGIC |      |            |                      |                                                             |
# MAGIC  
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Reads the Configuration YAML File (DO NOT EDIT)

import sys
from pathlib import Path
sys.path.append("../shared")

from nep_read_config import load_config

load_config(dbutils, "../config")

# COMMAND ----------

# DBTITLE 1,Enable Database and Table Parameters for Widgets
opsmi_database=dbutils.widgets.get("opsmi_database")
ods_ensek_database=dbutils.widgets.get("ods_ensek_database")
nep_bi_database=dbutils.widgets.get("nep_bi_database")
ods_salesforce_database=dbutils.widgets.get("ods_salesforce_database")

# COMMAND ----------

# DBTITLE 1,Define Output Table Parameters
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {opsmi_database}.opsmi_dd_less_than_15
(
    ensek_account_id                INT,
    auddis_id1                      STRING,
    sp_schedule_status              STRING,
    combinedprojcost                DECIMAL(29,2),
    current_balance                 DECIMAL(10,2),
    dd_value                        DECIMAL(10,2),
    channel                         STRING,
    ensek_schedule_start_date       TIMESTAMP,
    ensek_schedule_status           STRING,
    latest_payment_arrangement_date DATE,
    regularpaymentday               INT,
    created_by_latest               STRING,
    association_start_date          DATE,
    task_list_item_created          DATE,
    account_id                      INT,
    last_statement_balance          DECIMAL(13,2),
    elecprojcost                    DECIMAL(28,2),
    gasprojcost                     DECIMAL(28,2),
    elecprojunits                   DECIMAL(28,2),
    gasprojunits                    DECIMAL(28,2),
    statementcreated_date           DATE,
    current_balance_date            DATE,
    payment_method_uid              STRING,
    regular_payment_date            INT,
    dd_setup_date                   DATE,
    auddis_id                       STRING,
    pa_completed_date               DATE,
    next_pa_date                    DATE,
    months_since_last_bill          INT,
    created_by                      STRING,
    pa_status                       STRING,
    dd_update_date                  DATE,
    next_dd_update_date             DATE,
    payments_left                   INT,
    remaining_cost                  DECIMAL(15,2),
    end_of_plan_balance             DECIMAL(15,2),
    final_payment_amount            DECIMAL(29,2),
    request_category                STRING,
    account_type                    STRING,
    data_refreshed_timestamp        TIMESTAMP
)
""")

# COMMAND ----------

# DBTITLE 1,pa_payments
spark.sql(f"""
DROP TABLE IF EXISTS {opsmi_database}.tmp_pa_payments_dd15
""")

spark.sql(f"""
CREATE TABLE {opsmi_database}.tmp_pa_payments_dd15 AS
SELECT *
FROM (
    SELECT
        pa.account_id,
        pa.payment_arrangement_id,
        padd.regular_payment_amount AS new_payment_amount,
        padd.regular_payment_date   AS regularpaymentday,
        pa.payment_method_uid,
        pa.arrangement_start_date,
        pa.arrangement_end_date,
        pa.created_date               AS pa_createddatetime,
        CAST(pa.created_date AS DATE) AS pa_createddate,
        pa.created_by,
        pa.payment_frequency_uid,
        ROW_NUMBER() OVER (
            PARTITION BY pa.account_id
            ORDER BY pa.arrangement_start_date DESC, COALESCE(pa.arrangement_end_date, '2099-01-01') DESC
        ) AS rn
    FROM
        {ods_ensek_database}.ensek_pit_ignition_crm_payment_payment_arrangement AS pa
    INNER JOIN {ods_ensek_database}.ensek_pit_ignition_crm_payment_payment_arrangement_direct_debit AS padd
    ON pa.payment_arrangement_id = padd.payment_arrangement_id
) AS sub
WHERE rn = 1;
""")

# COMMAND ----------

# DBTITLE 1,data_view
spark.sql(f"""
DROP TABLE IF EXISTS {opsmi_database}.tmp_data_view_dd15
""")

spark.sql(f"""
CREATE TABLE {opsmi_database}.tmp_data_view_dd15 AS
SELECT *
FROM (
    SELECT
        a.account_id AS ensek_account_id,
        a.auddis_id AS auddis_id1,
        a.sp_schedule_status,
        sf.originatingchannel__c AS channel,
        sf.migration_check,
        a.ensek_schedule_start_date,
        a.ensek_schedule_status,
        c.arrangement_start_date AS latest_payment_arrangement_date,
        c.regularpaymentday,
        c.created_by AS created_by_latest,
        la.association_start_date,
        tli.task_list_item_created,
        d.account_id,
        d.last_statement_balance,
        d.elecprojcost,
        d.gasprojcost,
        d.elecprojunits,
        d.gasprojunits,
        d.combinedprojcost,
        d.statementcreated_date,
        d.current_balance,
        d.current_balance_date,
        d.payment_method_uid,
        d.regular_payment_date,
        d.dd_setup_date,
        d.auddis_id,
        d.pa_completed_date,
        d.next_pa_date,
        d.months_since_last_bill,
        d.created_by,
        d.pa_status,
        d.dd_update_date,
        d.next_dd_update_date,
        d.payments_left,
        d.remaining_cost,
        d.end_of_plan_balance,
        CASE 
            WHEN c.new_payment_amount IS NULL OR c.new_payment_amount = 0 THEN a.regular_payment_amount
            ELSE c.new_payment_amount
        END AS final_payment_amount
    FROM (
        SELECT *
        FROM {nep_bi_database}.opsmi_f_account_payment_split
        WHERE payment_method = 'DirectDebit'
    ) AS a
    LEFT JOIN {opsmi_database}.tmp_pa_payments_dd15 AS c
        ON a.account_id = c.account_id
    LEFT JOIN {nep_bi_database}.opsmi_dd_end_plan_balance AS d
        ON a.account_id = d.account_id
    LEFT JOIN (
        SELECT
            account_id,
            association_start_date AS association_start_date
        FROM (
            SELECT
                account_id,
                association_start_date,
                ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY association_start_date DESC) AS rn
            FROM {ods_ensek_database}.ensek_pit_ignition_crm_crm_account_meter_point_registration
            WHERE association_start_date IS NOT NULL
        ) AS rankeddata
        WHERE rn = 1
    ) AS la
        ON a.account_id = la.account_id
    LEFT JOIN (
        SELECT
            tli.account_id,
            MAX(CAST(tli.task_list_item_created AS DATE)) AS task_list_item_created
        FROM {ods_ensek_database}.ensek_pit_ignition_crm_task_lists_task_list_item AS tli
        WHERE LEFT(tli.task_list_item_state_uid, 16) = 'Payment.Adequacy'
        GROUP BY tli.account_id
    ) AS tli
        ON a.account_id = tli.account_id
    LEFT JOIN (
        SELECT
            account_id,
            bgaccountreference__c,
            originatingchannel__c,
            CASE 
                WHEN originatingchannel__c = 'BG-Migration' THEN 'BG-Migration'
                ELSE
                    CASE
                        WHEN LEFT(bgaccountreference__c, 1) = 8 THEN 'BG-Migration'
                        ELSE 'Non-Migration'
                    END
            END AS migration_check
        FROM (
            SELECT
                billingaccountreferenceid__c AS account_id,
                bgaccountreference__c,
                originatingchannel__c,
                ROW_NUMBER() OVER (PARTITION BY billingaccountreferenceid__c ORDER BY createddate DESC) AS rn
            FROM {ods_salesforce_database}.sf_account
        )
        WHERE rn = 1
    ) AS sf
        ON a.account_id = sf.account_id
)
WHERE final_payment_amount > 0
  AND final_payment_amount < 15;
""")

# COMMAND ----------

# DBTITLE 1,Transformation (Final Output Table)
spark.sql(f"""
INSERT OVERWRITE TABLE {opsmi_database}.opsmi_dd_less_than_15

SELECT *,
  CASE 
    WHEN request_category IN('Migration < 4 Months','PA in last 6 months') THEN 'Non workable'
    WHEN dd_update_date>=DATEADD(MONTH,-3,CURRENT_DATE) THEN 'Non workable'
    WHEN combinedprojcost IS NULL THEN 'Non workable'
    WHEN dd_value < 15 THEN 'Non workable'
    ELSE 'Workable' 
  END AS account_type,
  CURRENT_TIMESTAMP() AS data_refreshed_timestamp
FROM (
SELECT 
  ensek_account_id,
  auddis_id1,
  sp_schedule_status,
  combinedprojcost,
  current_balance,
  CAST(ROUND((combinedprojcost+current_balance)/12,2) AS DECIMAL(10,2)) AS dd_value,
  CASE WHEN migration_check IS NULL THEN 'Non-Migration' ELSE migration_check END AS channel,
  ensek_schedule_start_date,
  ensek_schedule_status,
  latest_payment_arrangement_date,
  regularpaymentday,
  created_by_latest,
  association_start_date,
  task_list_item_created,
  account_id,
  last_statement_balance,
  elecprojcost,
  gasprojcost,
  elecprojunits,
  gasprojunits,
  statementcreated_date,
  current_balance_date,
  payment_method_uid,
  regular_payment_date,
  dd_setup_date,
  auddis_id,
  pa_completed_date,
  next_pa_date,
  months_since_last_bill,
  created_by,
  pa_status,
  dd_update_date,
  next_dd_update_date,
  payments_left,
  remaining_cost,
  end_of_plan_balance,
  final_payment_amount,
  CASE
  WHEN final_payment_amount=0.00 THEN 'DD set at £0.00'
  WHEN migration_check='BG-Migration' AND DATE(association_start_date)>=DATEADD(MONTH,-4,CURRENT_DATE) THEN 'Migration < 4 Months'
  WHEN DATE(task_list_item_created)>=DATEADD(MONTH,-6,CURRENT_DATE) THEN 'PA in last 6 months'
  WHEN DATE(task_list_item_created) IS NULL THEN 'No PA task Found'
  ELSE 'Other'
  END AS request_category
FROM {opsmi_database}.tmp_data_view_dd15 AS ddd 
WHERE ddd.ensek_schedule_status = 'active') iew
""")

# COMMAND ----------

# DBTITLE 1,Delete Temp Tables
spark.sql(f"""DROP TABLE IF EXISTS {opsmi_database}.tmp_pa_payments_dd15""")
spark.sql(f"""DROP TABLE IF EXISTS {opsmi_database}.tmp_data_view_dd15""")

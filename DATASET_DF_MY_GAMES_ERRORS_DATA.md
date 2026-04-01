# Dataset: `df_My_Games_errors_data`

## Description of data for `df_My_Games_errors_data`

```sql
WITH period AS (
  SELECT
    DATE('2025-06-01') AS start_date
  , DATE('2026-03-29') AS end_date
),

errors_and_invoices AS (
  WITH main_cte AS (
    SELECT DISTINCT
        internal_date
      , milliseconds
      , user_access_token AS uat
      , action
      , CASE
          WHEN action IN ('click-btn-pay','create-invoice') THEN 0
          WHEN action = 'show-error-message' THEN 1
          ELSE 2
        END action_order
      
      , IF(action IN ('click-btn-pay','create-invoice'), action, NULL) AS create_invoice
      , IF(action = 'show-error-message', action, NULL) AS show_error_message
      , IF(action = 'show-error-message', SAFE_CAST(value AS INT64), NULL) AS err_code
      , purchase_invoice_id
      , instance_id
      , IF(action = 'show-error-message', instance_id, NULL) AS error_instance_id
    FROM metrika_rawdata.paystation_user_sessions
    WHERE TRUE
        AND internal_date >= (SELECT start_date FROM period)
        AND internal_date <  (SELECT end_date FROM period)
        AND action IN ('show-error-message', 'create-invoice', 'click-btn-pay')
        AND client_version IN ('Pay Station 4', 'Headless Checkout')
        AND user_access_token IS NOT NULL
        ORDER BY 1, 3, 2, 5
  ),

  final AS (
    SELECT
        internal_date
      , action
      , action_order
      , milliseconds
      , LAG(milliseconds) OVER (PARTITION BY uat ORDER BY milliseconds, action_order) AS lag_milliseconds
      , (milliseconds - LAG(milliseconds) OVER (PARTITION BY uat ORDER BY milliseconds, action_order)) / 1000.0 AS diff_seconds
      , uat
      , create_invoice
      , show_error_message
      , err_code
      , purchase_invoice_id
      , LAG(purchase_invoice_id) OVER (PARTITION BY uat ORDER BY milliseconds, action_order) AS lag_purchase_invoice_id    
      , instance_id
      , error_instance_id
    FROM main_cte
  )

  SELECT DISTINCT
      internal_date
    , uat
    , action
    , err_code AS invoice_err_code
    , lag_purchase_invoice_id AS invoice_id
    , diff_seconds 
  FROM final
    WHERE TRUE
      AND action = 'show-error-message'
      AND diff_seconds <= 1
) ,

errors_from_sessions AS (
  SELECT DISTINCT
      pus.internal_date
    , pus.user_access_token AS uat
    , project_id
    , user_country
    , theme_version
    , client_version
    , pus.action
    , pus.state
    , IF(pus.action = 'create-invoice', pus.action, NULL) AS create_invoice
    , IF(pus.action = 'show-error-message', pus.action, NULL) AS show_error_message
    , IF(pus.action = 'show-error-message', SAFE_CAST(pus.value AS INT64), NULL) AS err_code
    , pus.instance_id
    , pus.purchase_invoice_id
    , eai.invoice_err_code
    , IF(eai.invoice_err_code IS NOT NULL, pus.instance_id, NULL) AS error_instance_id
    , pus.milliseconds
  FROM metrika_rawdata.paystation_user_sessions AS pus
    LEFT JOIN errors_and_invoices AS eai ON eai.invoice_id = pus.purchase_invoice_id
  WHERE TRUE
      AND pus.internal_date >= (SELECT start_date FROM period)
      AND pus.internal_date <  (SELECT end_date FROM period)
      AND pus.client_version IN ('Pay Station 4', 'Headless Checkout')
      AND pus.user_access_token IS NOT NULL
  -- ORDER BY user_access_token, milliseconds 
),

project_and_merchant AS (
  SELECT DISTINCT
      spf.project_id
    , spf.project
    , spf.merchant_id
    , spf.merchant
  FROM ps_analytics.static_ps_funnel AS spf
  WHERE TRUE
    AND internal_date >= '2025-06-01'
),

pid AS (
  SELECT DISTINCT
      spf.instance_id AS instance_id
    , spf.pid
  FROM ps_analytics.static_ps_funnel AS spf
  WHERE TRUE
    AND internal_date  >= '2025-06-01'
),

user_country AS (
  SELECT DISTINCT
      spf.user_country
    , spf.user_county_name
  FROM ps_analytics.static_ps_funnel AS spf
  WHERE TRUE
    AND internal_date >= '2025-06-01'
),

billing_amount AS (
  SELECT DISTINCT
      bi.id AS billilng_invoice
    , status
    , id_instance
  FROM xsolla-dwh.global_datamarts.billing_invoice AS bi
  WHERE TRUE
    AND date_create >= (SELECT start_date FROM period)
    AND date_create <= (SELECT end_date FROM period)
    AND status = 3
)

SELECT DISTINCT
    efs.internal_date -- дата пользовательской сессии
  , efs.uat AS user_access_token -- id пользовательской сессии
  , IF(efs.err_code IS NOT NULL, efs.uat, NULL) AS uat_with_error -- id пользовательской сессии в которой была ошибка 
  , MAX(IF(status = 3, efs.uat, NULL)) OVER (PARTITION BY efs.uat) AS successful_uat -- id пользовательской сессии в которой был успещно оплачен как минимум один инвойс. Флаг.
  , IF(status = 3, efs.uat, NULL) AS done_token -- id пользовательской сессии в которой был успешно оплачен как минимум один инвойс.
  , IF(status = 3 AND efs.invoice_err_code IS NOT NULL, efs.uat, NULL) AS done_uat_with_error -- id пользовательской сессии в которой была ошибка и был успещно оплачен как минимум один инвойс.

  , gp_quick_payment_button
  , apple_pay_quick_payment_button	
  , is_independent_windows
  , header_is_visible
  , game_id -- id игры на стороне пратнера
  , game_name -- название игры на стороне пратнера
  , uc.user_county_name -- страна пользователя
  , pnm.project -- id проекта Xsolla
  , efs.theme_version -- тип устройства
  , efs.show_error_message -- ошибка которую видит пользователь
  , efs.err_code AS Error_code -- код ошибки атрибуцированный к пользовательской сессии
  , 'Error code - '||efs.err_code||': '||err_1.description_eng AS Error_code_with_description -- код ошибки и описание атрибуцированные к ользовательской сессии
  , efs.instance_id  -- id платежного метода в Pay Station - тот который видит пользователь во время оплаты.
  , pid.pid -- id + название платежного метода, тот который видит пользователь во время оплаты. 
  , efs.purchase_invoice_id -- id инвойса
  , ba.status -- статус инвойса. 3 - done. 1 - created. Остальные -  неуспех 
  , ba.id_instance AS billing_instance_id -- id платежного метода в биллинге
  , bpid.pid AS billing_pid -- id + название платежного метода в биллинге
  , efs.invoice_err_code -- код ошибки атрибуцированный к инвойсу
  , 'Error code - '||efs.invoice_err_code||': '||err_2.description_eng AS Invoice_Error_code_with_description -- код ошибки и описание атрибуцированные к инвойсу
FROM errors_from_sessions AS efs
  LEFT JOIN ds_adhock.static_DS_8478_My_Games_tokens_data AS td ON td.token = efs.uat
  LEFT JOIN ps_analytics.static_error_error_description_eng AS err_1 ON err_1.id = efs.err_code
  LEFT JOIN ps_analytics.static_error_error_description_eng AS err_2 ON err_2.id = efs.invoice_err_code
  LEFT JOIN billing_amount AS ba ON ba.billilng_invoice = efs.purchase_invoice_id
  LEFT JOIN pid ON pid.instance_id = efs.instance_id
  LEFT JOIN pid AS bpid ON bpid.instance_id = ba.id_instance
  LEFT JOIN project_and_merchant AS pnm ON pnm.project_id = efs.project_id
  LEFT JOIN user_country AS uc ON uc.user_country = efs.user_country
WHERE TRUE
  AND pnm.merchant_id = 799928
  AND efs.user_country = 'US'
```

---

## Calculations

```sql
COUNT(DISTINCT user_access_token) AS Total_traffic

COUNT(DISTINCT IF(Error_code IS NOT NULL, user_access_token, NULL)) AS Tokens_with_error

COUNT(DISTINCT IF(Error_code IS NOT NULL, user_access_token, NULL))
/ COUNT(DISTINCT user_access_token) * 100 AS Error_rate
```

# Dataset: `df_main_my_games`

## Description of data for `df_main_my_games`

```sql
WITH billing_data AS (
  SELECT
      id AS invoice_id -- id инвойса
    , id_instance -- id платежного метода в биллинге
    , Payment_status -- статус платежа
    , status
  FROM xsolla-dwh.global_datamarts.billing_invoice AS spf
  WHERE TRUE 
    AND date_create >= '2025-01-01'
    AND date_create <= '2026-03-29'
),

pid AS (
  SELECT DISTINCT
      spf.instance_id AS instance_id
    , spf.pid
  FROM ps_analytics.static_ps_funnel AS spf
  WHERE TRUE
    AND internal_date  >= '2025-06-01'
)

SELECT
    spf.internal_date -- дата пользовательской сессии
  
  , user_id -- id пользователя на Pay Station
  , spf.user_access_token -- id пользовательской сессии
  , user_browser_type -- браузер
  , is_iframe -- Если 1 - то iframe
  , user_operation_system -- операционная система
  , theme_version -- тип устройства
  , is_saved_open -- флаг который показывает был ли у пользователя сохраненный метод оплаты в аккаунте. Если 1 - был у пользователя сохраненный метод оплаты в аккаунте
  , saved_account_info -- информация о том был ли у пользователя сохраненный метод оплаты (и какой) в аккаунте или он его сохранил во время оплаты.
  , spf.pid -- id + название платежного метода, тот который видит пользователь во время оплаты. 

  , gp_quick_payment_button
  , apple_pay_quick_payment_button	
  , is_independent_windows
  , header_is_visible
  , user_locale -- язык открытия Pay Station
  , country -- страна пользователя
  , game_id -- id игры на стороне пратнера
  , game_name -- название игры на стороне пратнера

  , spf.invoice_id -- id инвойса
  , bd.Payment_status -- статус платежа
  , bd.status -- статус инвойса. 3 - done. 1 - created. Остальные - неуспех 

  , spf.token_show_shopping_cart -- флаг который показывает была ли показана пользовтелю shopping cart на Pay Station. Если 1 - была показана пользовтелю shopping cart на Pay Station - мы считаем, что это первый этап загрузки Pay Station на котором пользователь может начать пользоваться Pay Station. ЕСли сократить до сути: Если 1 - Pay Station готова к работе.
  , spf.token_interact -- флаг который показывает было ли взаимодействие с Pay Station. Если 1 - было взаимодействие с Pay Station
  , spf.create_invoice_old -- флаг который показывает был ли создан инвой с сессии. Если 1 - был создан инвой с сессии
  , spf.instance_id -- id платежного метода в Pay Station - тот который видит пользователь во время оплаты.
  , spf.bi_instance -- id платежного метода в биллинге
  , spf.pid AS pid_billing -- id и название платежного метода в биллинге
  , spf.validation_error -- флаг который показывает была ли ошибки валидации. Если 1 - была ошибка валидации
  , spf.afs_canceled -- флаг который показывает отменен ли инвойс AFS. Если 1 - отменен AFS
  , spf.ps_canceled -- флаг который показывает отменен ли инвойс. Если 1 - отменен
  , spf.done_just -- флаг который показывает оплачен ли инвойс. Если 1 - оплачен

FROM xsolla-dwh.ps_analytics.static_ps_funnel AS spf
  LEFT JOIN ds_adhock.static_DS_8478_My_Games_tokens_data AS td ON td.token = spf.user_access_token
  LEFT JOIN billing_data AS bd ON bd.invoice_id = spf.invoice_id
  LEFT JOIN pid ON pid.instance_id = spf.bi_instance
WHERE TRUE 
  AND spf.internal_date >= '2025-06-01'
  AND spf.internal_date <= '2026-03-25'
  AND spf.merchant_id = 799928
  AND spf.user_country = 'US'
ORDER BY 1
```

---

## Calculations

```sql
COUNT(DISTINCT user_access_token) AS Open
COUNT(DISTINCT IF(token_show_shopping_cart = '1', user_access_token, NULL))/ COUNT(DISTINCT user_access_token) * 100 AS CR_OPEN_SHOW_SC
COUNT(DISTINCT IF(token_interact = '1', user_access_token, NULL)) / COUNT(DISTINCT user_access_token) AS CR_OPEN_INTERACT
COUNT(DISTINCT IF(create_invoice_old = 1 AND instance_id = bi_instance, user_access_token, NULL)) / COUNT(DISTINCT user_access_token) * 100 AS CR_OPEN_CREATE
COUNT(DISTINCT IF(validation_error = 1 OR afs_canceled = 1 OR ps_canceled = 1, user_access_token, NULL)) / COUNT(DISTINCT user_access_token) * 100 AS AFS_PS_Validation_cancel
COUNT(DISTINCT IF(instance_id = bi_instance AND done_just = 1, user_access_token, NULL)) / COUNT(DISTINCT IF(create_invoice_old = 1 AND instance_id = bi_instance, user_access_token, NULL)) * 100  AS CR_CREATE_DONE
COUNT(DISTINCT IF(instance_id = bi_instance AND done_just = 1, user_access_token, NULL)) / COUNT(DISTINCT user_access_token) * 100 AS CR_OPEN_DONE
```

### Payment status distribution by payment method

```sql
SELECT
    pid
  , Payment_status
  , COUNT(DISTINCT invoice_id) AS user_access_tokens
FROM df_main_my_games
WHERE TRUE 
  AND Payment_status IS NOT NULL
  AND pid IN (
                '3175.Apple Pay'
              , '24.PayPal'
              , '3431.Google Pay'
              , '1380.Bank card'
              )
GROUP BY 1,2
ORDER BY 1,3 DESC
```

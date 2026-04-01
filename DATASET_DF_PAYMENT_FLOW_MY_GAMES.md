# Dataset: `df_payment_flow_my_games`

## Description

Session-level rows from Pay Station funnel (`static_ps_funnel`) for merchant **799928**, country **US**, non-direct-button traffic (`is_direct_button = 0`), joined to My.Games token metadata and to a billing payment-method mapping derived from funnel rows.

Each `user_access_token` can appear on multiple rows ordered by `internal_time`; **`steps`** is the step index inside that session.

---

## Source query

```sql
WITH bill_pid AS (
  SELECT DISTINCT
    spf.instance_id
  , spf.pid AS bill_pid
  FROM xsolla-dwh.ps_analytics.static_ps_funnel AS spf
  WHERE TRUE 
    AND spf.internal_date >= '2026-01-01'
)

SELECT
    spf.internal_date
  , spf.internal_time
  , td.game_id
  , td.game_name
  , spf.user_access_token
  , spf.pid
  , bp.bill_pid
  , IF(spf.invoice_id != 0, spf.invoice_id, NULL) AS invoice_id
  , spf.done_just
  , ROW_NUMBER() OVER (PARTITION BY spf.user_access_token ORDER BY spf.internal_time) AS steps
FROM xsolla-dwh.ps_analytics.static_ps_funnel AS spf
  LEFT JOIN ds_adhock.static_DS_8478_My_Games_tokens_data AS td ON td.token = spf.user_access_token
  LEFT JOIN bill_pid AS bp ON bp.instance_id = spf.bi_instance
WHERE TRUE 
  AND spf.internal_date >= '2026-01-01'
  AND spf.internal_date <= '2026-03-29'
  AND spf.merchant_id = 799928
  AND spf.user_country = 'US'
  AND is_direct_button = 0
```

---

## Fields

| Column | Description |
|--------|-------------|
| `internal_date` | Calendar date of the user session (Pay Station). |
| `internal_time` | Timestamp of the user session (Pay Station). |
| `game_id` | Game identifier on the partner side (from `static_DS_8478_My_Games_tokens_data` via `token` = `user_access_token`). |
| `game_name` | Game name on the partner side (same join). |
| `user_access_token` | Pay Station session identifier; partitions the row-number window for `steps`. |
| `pid` | Payment method as shown to the user during checkout (id + label). |
| `bill_pid` | Payment method recorded in billing context after invoice creation: `bill_pid` CTE maps `instance_id` → `pid` from funnel rows since `2026-01-01`, joined to `spf.bi_instance`. |
| `invoice_id` | Invoice id when `spf.invoice_id != 0`; otherwise `NULL`. |
| `done_just` | Invoice paid flag: **1** = paid. |
| `steps` | `ROW_NUMBER()` over `user_access_token` ordered by `internal_time` (ordered events within the session). |

---

## CTE: `bill_pid`

Distinct pairs (`instance_id`, `pid` as `bill_pid`) from `static_ps_funnel` with `internal_date >= '2026-01-01'`, used to resolve the billing-side payment method for `spf.bi_instance`.

---

## Filters (WHERE)

| Condition | Meaning |
|-----------|---------|
| `spf.internal_date` between `2026-01-01` and `2026-03-29` | Reporting window. |
| `spf.merchant_id = 799928` | My.Games merchant. |
| `spf.user_country = 'US'` | US users only. |
| `is_direct_button = 0` | Excludes direct-button flow (as in the source query; typically `spf.is_direct_button`). |

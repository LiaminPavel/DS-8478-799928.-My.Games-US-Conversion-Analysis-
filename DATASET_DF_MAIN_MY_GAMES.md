# Dataset: `df_main_my_games`

Project-local reference: field meanings and metric definitions used in this research.

## Source

- **Primary table:** `xsolla-dwh.ps_analytics.static_ps_funnel` (`spf`)
- **Join:** `LEFT JOIN ds_adhock.static_DS_8478_My_Games_tokens_data AS td` on `td.token = spf.user_access_token`

## Row grain

One row per Pay Station funnel record aligned with the query (session-level fields from `spf` plus token-level attributes from `td` where matched).

## Filters applied when building the extract

| Condition | Meaning |
|-----------|---------|
| `spf.internal_date >= '2025-06-01'` | Start of analysis window |
| `spf.internal_date <= '2026-03-25'` | End of analysis window (as in extract) |
| `spf.merchant_id = 799928` | Partner scope (My.Games) |
| `spf.user_country = 'US'` | US-only |

## Main fields

| Field | Description |
|-------|-------------|
| `internal_date` | User session date |
| `user_id` | User id on Pay Station |
| `user_access_token` | User session id |
| `user_browser_type` | Browser |
| `is_iframe` | `1` if iframe |
| `user_operation_system` | Operating system |
| `theme_version` | Device type |
| `is_saved_open` | `1` if the user had a saved payment method on the account |
| `saved_account_info` | Whether / which payment method was saved on the account or saved during checkout |
| `pid` | Visible payment method id + name at checkout |
| `gp_quick_payment_button` | Google Pay quick-pay button flag / field (as in source) |
| `apple_pay_quick_payment_button` | Apple Pay quick-pay button flag / field (as in source) |
| `is_independent_windows` | Independent windows flag / field (as in source) |
| `header_is_visible` | Header visibility flag / field (as in source) |
| `user_locale` | Pay Station open language |
| `country` | User country |
| `game_id` | Game id on partner side |
| `game_name` | Game name on partner side |
| `token_show_shopping_cart` | `1` if shopping cart was shown on Pay Station — treated as first loaded state where Pay Station is ready to use (“Pay Station ready”) |
| `token_interact` | `1` if there was interaction with Pay Station |
| `create_invoice_old` | `1` if an invoice was created from the session |
| `instance_id` | Payment method id in Pay Station (visible to user) |
| `bi_instance` | Payment method id in billing |
| `validation_error` | `1` if validation error occurred |
| `afs_canceled` | `1` if invoice canceled by AFS |
| `ps_canceled` | `1` if invoice canceled |
| `done_just` | `1` if invoice is paid |

Additional columns may exist in `df_main_my_games` if added later; they will be documented when used.

---

## Metrics and calculation logic

Denominator convention: unless noted, **Open** is the base count of distinct sessions.

### Open

```
COUNT(DISTINCT user_access_token) AS Open
```

### CR_OPEN_SHOW_SC

Share of sessions where shopping cart was shown (string `'1'` on `token_show_shopping_cart`), out of all sessions.

```
COUNT(DISTINCT IF(token_show_shopping_cart = '1', user_access_token, NULL))
/ COUNT(DISTINCT user_access_token) * 100 AS CR_OPEN_SHOW_SC
```

### CR_OPEN_INTERACT

Share of sessions with Pay Station interaction (`token_interact = '1'`), relative to all sessions. *(Note: as written in the project SQL, this ratio is not multiplied by 100.)*

```
COUNT(DISTINCT IF(token_interact = '1', user_access_token, NULL))
/ COUNT(DISTINCT user_access_token)  * 100 AS CR_OPEN_INTERACT
```

### CR_OPEN_CREATE

Share of sessions where an invoice was created and `instance_id` matches `bi_instance`, out of all sessions.

```
COUNT(DISTINCT IF(create_invoice_old = 1 AND instance_id = bi_instance, user_access_token, NULL))
/ COUNT(DISTINCT user_access_token) * 100 AS CR_OPEN_CREATE
```

### AFS_PS_Validation_cancel

Share of sessions with at least one of: validation error, AFS cancel, or PS cancel.

```
COUNT(DISTINCT IF(validation_error = 1 OR afs_canceled = 1 OR ps_canceled = 1, user_access_token, NULL))
/ COUNT(DISTINCT user_access_token) * 100 AS AFS_PS_Validation_cancel
```

### CR_CREATE_DONE

Among sessions that created an invoice with matching `instance_id` and `bi_instance`, share where the invoice is paid (`done_just = 1` with same instance match).

```
COUNT(DISTINCT IF(instance_id = bi_instance AND done_just = 1, user_access_token, NULL))
/ COUNT(DISTINCT IF(create_invoice_old = 1 AND instance_id = bi_instance, user_access_token, NULL)) * 100
AS CR_CREATE_DONE
```

### CR_OPEN_DONE

Share of all sessions where invoice is paid with `instance_id = bi_instance`.

```
COUNT(DISTINCT IF(instance_id = bi_instance AND done_just = 1, user_access_token, NULL))
/ COUNT(DISTINCT user_access_token) * 100 AS CR_OPEN_DONE
```

---

## Implementation notes

- `token_show_shopping_cart` and `token_interact` are compared to **string** `'1'` in the provided SQL; `create_invoice_old`, `instance_id` / `bi_instance` matching, `validation_error`, `afs_canceled`, `ps_canceled`, `done_just` use **numeric** `1` where shown.
- In Python/pandas, replicate the same predicates and use distinct session counts consistent with these definitions.

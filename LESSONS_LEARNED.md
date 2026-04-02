# Lessons Learned — DS-8478_[799928. My.Games] US Conversion Analysis
Date: 2026-04-02

## What was built
- YoY Comparison (overall conversion by year).
- YoY Comparison by Payment Method (Bank card, Apple Pay, PayPal, Google Pay).
- Traffic Distribution by Payment Method.
- Payment Flow Sankey + bank-card successful flow variant.
- The language of the Pay Station opening.
- Error Rate.
- Payment status distribution by payment method.
- Cancel reasons (Bank cards).
- Executive markdown descriptions under key charts.
- SVG export flow for chart widgets via local preview iframe and `/__save_svg`.

## Problems encountered and solutions
| Problem | How it was solved | Rule added |
|---------|------------------|------------|
| HTML/JS charts did not execute reliably in notebook inline output | Served preview HTML through local HTTP server and rendered with `IFrame` | Keep iframe-preview pattern for interactive HTML widgets |
| SVG capture produced extra whitespace on some widgets | Fixed wrapper CSS placement and constrained widget container (`max-width`) before `html2canvas` snapshot | Validate wrapper CSS is inside `<style>` and applied before export |
| Notebook edits were not always visible immediately in UI after bulk JSON rewrites | Switched to cell-targeted notebook edits and avoided whole-file rewrite workflow | Enforce cell-level editing mode for `.ipynb` changes |

## Patterns that worked well
- KPI cards + chart panel + legend rows layout pattern across multiple charts.
- Unified Save as SVG behavior: hide save-row, capture root via `html2canvas`, embed PNG into SVG, POST to local handler.
- Reuse of Xsolla design tokens from `utils/style.py` and shared HTML constants from `utils/html_style.py`.
- Confluence-ready self-contained HTML exports with built-in copy flow.

## Variable naming patterns used
- DataFrames: `df_main_my_games`, `df_My_Games_errors_data`, `df_payment_flow_my_games`, `cancel_reasons_df`, `payment_status_df`.
- Chart HTML blobs: `yoy_html`, `fig_html`, `pf_html`.
- Preview/server objects: `_YPM_preview_path`, `_LANG_httpd`, `_ERR_port`, `_PSD_handler`, `_CRC_iframe_url`.
- Prefix grouping for one chart context: `pf_*`, `locale_*`, `errate_*`, `psdist_*`, `crcard_*`.

## Chart types and functions created
- Chart types:
  - Horizontal bar distributions.
  - Donut + ranked legend composition.
  - Multi-panel YoY comparison cards.
  - Sankey flow visualization.
  - KPI card blocks with delta superscripts.
- Reused utility functions/modules:
  - `utils/style.py`: `XS`, `apply_xsolla_style`, `format_bar_labels`, `set_chart_title`.
  - `utils/html_style.py`: `XS_CSS_PID_BLOCK`, `XS_LEGEND_HTML`, `XS_BAR_ANIMATION_JS`, `diff_color`.
  - `utils/display.py`: `start_chart_server`, `show`, `show_all`.

## Data sources
- BigQuery sources (from dataset documentation):
  - `xsolla-dwh.ps_analytics.static_ps_funnel`
  - `xsolla-dwh.global_datamarts.billing_invoice`
  - `metrika_rawdata.paystation_user_sessions`
  - `ps_analytics.static_error_error_description_eng`
  - `ds_adhock.static_DS_8478_My_Games_tokens_data`
- Parquet files used in notebook:
  - `df_main_my_games.parquet`
  - `df_payment_flow_my_games.parquet`
  - `df_My_Games_errors_data.parquet`
  - `df_main_billing_data_my_games.parquet`
  - `df_main_billing_data_responses_my_games.parquet`

## Time spent (estimated from git log)
- First checkpoint: 2026-03-31 (`init: initial checkpoint`).
- Last checkpoint: 2026-04-02 (`docs(analytics): add executive chart descriptions and SVG previews`).
- Estimated active research window: ~3 calendar days.
- Commit cadence: 15 checkpoints total (~5/day), with dense iteration on 2026-04-01/02.

## Recommendations for next research
- Start with a mandatory chart wrapper template that already includes iframe preview + SVG save hooks.
- Keep one canonical naming map per chart context (`{context}_*`) to avoid collisions in long notebooks.
- Generate `*_preview.html` deterministically for every chart to make QA/review repeatable.
- Normalize semantically identical cancel-reason labels (example: `Insufficient Funds` vs `Insufficient funds`) before final business interpretation.
- Move repeated local HTTP preview boilerplate into reusable helper(s) under `utils/`.

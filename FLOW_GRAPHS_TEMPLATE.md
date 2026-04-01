# ==============================================================================
Template 1:

# @title ❇️ Multi-Entry Flow 🤝

# ==============================================================================
#  Input:  df_users_choose_Xsolla_Pay
#  Все переменные с префиксом MEF_ / mef_
#  → защита от конфликтов при повторном запуске в Colab
#
#  KPI карточки: для каждого entry-метода показываем
#  % пользователей выбравших ТОТ ЖЕ метод на 2-м платеже
#  среди тех у кого есть повторный платёж
# ==============================================================================

import gc, json
import pandas as pd
from IPython.display import display, HTML


# =========================
# 1) Parameters
# =========================

MEF_ENTRY_METHODS = [
    'Xsolla Pay', 'Bank card', 'Apple Pay',
    'PayPal', 'Google Pay', 'PayPay', 'Alipay',
]
MEF_MAX_STEPS   = 10
MEF_TOP_METHODS = 10
MEF_DATE_COL    = 'internal_date'

MEF_PID_COLORS = {
    'Xsolla Pay':'#6366f1', 'Bank card':'#64748b',  'PayPal':'#3b82f6',
    'Apple Pay':'#374151',  'Google Pay':'#059669',  'Bank app':'#0891b2',
    'PIX':'#16a34a',        'Cash App':'#65a30d',    'PayPay':'#ea580c',
    'Dragonpay':'#d97706',  'Klarna':'#e879b9',      'Alipay':'#1677ff',
    'WeChat':'#07c160',     'GoPay':'#f43f5e',       'Gcash':'#0ea5e9',
    'RAZER GOLD':'#fbbf24', 'Other':'#cbd5e1',
}

def mef_pid_label(mef_pid_str):
    mef_parts = str(mef_pid_str).split('.', 1)
    return mef_parts[1].strip() if len(mef_parts) > 1 else str(mef_pid_str)


# =========================
# 2) Data preparation
# =========================

mef_df = df_users_choose_Xsolla_Pay[
    [c for c in ['user_id','internal_time', MEF_DATE_COL,'pid','first_pay_time']
     if c in df_users_choose_Xsolla_Pay.columns]
].copy()

mef_df['internal_time']  = pd.to_datetime(mef_df['internal_time'])
mef_df['first_pay_time'] = pd.to_datetime(mef_df['first_pay_time'])
mef_df['pid_label']      = mef_df['pid'].apply(mef_pid_label)
mef_df.drop(columns=['pid'], inplace=True)
gc.collect()

try:
    mef_date_range = f"{mef_df[MEF_DATE_COL].min()} \u2013 {mef_df[MEF_DATE_COL].max()}"
except Exception:
    mef_date_range = ''

# Шаг A: первый платёж из entry methods
mef_df_entry = (
    mef_df[mef_df['first_pay_time'].notna() & mef_df['pid_label'].isin(MEF_ENTRY_METHODS)]
    [['user_id','pid_label','first_pay_time']]
    .drop_duplicates(subset='user_id')
    .rename(columns={'pid_label':'entry_method','first_pay_time':'entry_time'})
    .copy()
)
mef_entry_counts         = mef_df_entry['entry_method'].value_counts()
mef_entry_methods_sorted = mef_entry_counts.index.tolist()
mef_total_users          = len(mef_df_entry)

# Шаг B: транзакции ПОСЛЕ первого платежа
mef_df_after = (
    mef_df[mef_df['user_id'].isin(set(mef_df_entry['user_id']))]
    .merge(mef_df_entry[['user_id','entry_method','entry_time']], on='user_id', how='inner')
)
mef_df_after = mef_df_after[mef_df_after['internal_time'] > mef_df_after['entry_time']].copy()
mef_df_after.sort_values(['user_id','internal_time'], inplace=True)
mef_df_after.reset_index(drop=True, inplace=True)
mef_df_after['step'] = (
    mef_df_after.groupby('user_id')['internal_time']
    .rank(method='first').astype('int16')
)
mef_df_after = mef_df_after[mef_df_after['step'] <= MEF_MAX_STEPS].copy()
gc.collect()


# =========================
# 3) Vectorized transitions
# =========================

mef_df_step0 = mef_df_entry[['user_id','entry_method']].rename(columns={'entry_method':'pid_label'}).copy()
mef_df_step0['step'] = 0

mef_df_all_steps = pd.concat([
    mef_df_step0[['user_id','step','pid_label']],
    mef_df_after[['user_id','step','pid_label']],
], ignore_index=True)
mef_df_all_steps.sort_values(['user_id','step'], inplace=True)
mef_df_all_steps.reset_index(drop=True, inplace=True)

mef_df_all_steps['next_pid']  = mef_df_all_steps.groupby('user_id')['pid_label'].shift(-1)
mef_df_all_steps['next_step'] = mef_df_all_steps.groupby('user_id')['step'].shift(-1)

mef_df_trans = mef_df_all_steps.dropna(subset=['next_pid','next_step']).copy()
mef_df_trans  = mef_df_trans[mef_df_trans['next_step'] == mef_df_trans['step'] + 1].copy()
mef_df_trans  = mef_df_trans.rename(columns={
    'step':'from_step','pid_label':'from','next_step':'to_step','next_pid':'to'
})
mef_df_trans['to_step'] = mef_df_trans['to_step'].astype('int16')

del mef_df_all_steps, mef_df_after, mef_df_step0, mef_df; gc.collect()


# =========================
# 4) Top-N per step + remap Other
# =========================

mef_top_per_step = {}
for mef_step in range(1, MEF_MAX_STEPS + 1):
    mef_sl = mef_df_trans[mef_df_trans['to_step'] == mef_step]
    if mef_sl.empty:
        mef_top_per_step[mef_step] = set(MEF_ENTRY_METHODS)
        continue
    mef_top_set = set(mef_sl['to'].value_counts().head(MEF_TOP_METHODS).index)
    for mef_em in MEF_ENTRY_METHODS:
        mef_top_set.add(mef_em)
    mef_top_per_step[mef_step] = mef_top_set

def mef_remap(mef_lbl, mef_stp):
    if mef_stp == 0: return mef_lbl
    return mef_lbl if mef_lbl in mef_top_per_step.get(mef_stp, set()) else 'Other'

mef_df_trans['from_r'] = mef_df_trans.apply(lambda r: mef_remap(r['from'], int(r['from_step'])), axis=1)
mef_df_trans['to_r']   = mef_df_trans.apply(lambda r: mef_remap(r['to'],   int(r['to_step'])),   axis=1)

mef_agg = (
    mef_df_trans.groupby(['from_step','to_step','from_r','to_r'])
    .size().reset_index(name='value')
)
mef_agg.columns = ['from_step','to_step','from','to','value']
del mef_df_trans; gc.collect()


# =========================
# 5) Sankey nodes & links
# =========================

mef_node_lookup, mef_all_nodes, mef_nid = {}, [], 0

for mef_s in range(MEF_MAX_STEPS + 1):
    if mef_s == 0:
        mef_pids = [m for m in mef_entry_methods_sorted if m in mef_df_entry['entry_method'].values]
    else:
        mef_sl = mef_agg[mef_agg['to_step'] == mef_s]
        if mef_sl.empty: continue
        mef_vol       = mef_sl.groupby('to')['value'].sum().sort_values(ascending=False)
        mef_has_other = 'Other' in mef_vol.index
        mef_entry_in_vol = [m for m in mef_entry_methods_sorted if m in mef_vol.index]
        mef_middle    = [p for p in mef_vol.index if p not in mef_entry_methods_sorted and p != 'Other']
        mef_pids = mef_entry_in_vol + mef_middle + (['Other'] if mef_has_other else [])

    for mef_pid in mef_pids:
        mef_node_lookup[(mef_s, mef_pid)] = mef_nid
        mef_all_nodes.append({'id': mef_nid, 'name': mef_pid, 'step': mef_s})
        mef_nid += 1

mef_links_data = []
for _, mef_row in mef_agg.iterrows():
    mef_src = mef_node_lookup.get((int(mef_row['from_step']), mef_row['from']))
    mef_tgt = mef_node_lookup.get((int(mef_row['to_step']),   mef_row['to']))
    if mef_src is not None and mef_tgt is not None:
        mef_links_data.append({'source': mef_src, 'target': mef_tgt, 'value': int(mef_row['value'])})

mef_step_totals = {0: mef_total_users}
for mef_s in range(1, MEF_MAX_STEPS + 1):
    mef_v = int(mef_agg[mef_agg['to_step'] == mef_s]['value'].sum())
    if mef_v > 0: mef_step_totals[mef_s] = mef_v

# Per-method users per step (for drop prev % labels)
mef_method_per_step = {}
for mef_node_item in mef_all_nodes:
    mef_mn  = mef_node_item['name']
    mef_msv = mef_node_item['step']
    if mef_msv == 0:
        mef_mu = int(mef_entry_counts.get(mef_mn, 0))
    else:
        mef_sl_tmp = mef_agg[(mef_agg['to_step'] == mef_msv) & (mef_agg['to'] == mef_mn)]
        mef_mu = int(mef_sl_tmp['value'].sum()) if not mef_sl_tmp.empty else 0
    if mef_mn not in mef_method_per_step:
        mef_method_per_step[mef_mn] = {}
    mef_method_per_step[mef_mn][str(mef_msv)] = mef_mu

mef_mps_json       = json.dumps(mef_method_per_step)
mef_entry_users_json = json.dumps({m: int(mef_entry_counts.get(m,0)) for m in mef_entry_methods_sorted})


# =========================
# 6) KPI cards
#  Метрика: % пользователей выбравших ТОТ ЖЕ метод на 2-м платеже
#  Знаменатель: entry users для этого метода с повторным платежом
# =========================

def mef_get_same_method_val(mef_method):
    """Переходов: method (шаг 0) → тот же method (шаг 1)"""
    mef_src = mef_node_lookup.get((0, mef_method))
    mef_tgt = mef_node_lookup.get((1, mef_method))
    if mef_src is None or mef_tgt is None: return 0
    return next((l['value'] for l in mef_links_data
                 if l['source']==mef_src and l['target']==mef_tgt), 0)

def mef_get_repeat_total(mef_method):
    """Все пользователи метода сделавшие повторный платёж (шаг 0 → любой шаг 1)"""
    mef_src = mef_node_lookup.get((0, mef_method))
    if mef_src is None: return 0
    return sum(l['value'] for l in mef_links_data if l['source'] == mef_src)

def mef_make_kpi(mef_label, mef_val, mef_repeat_total, mef_entry_total, mef_accent, mef_is_best=False):
    mef_pct = mef_val / mef_repeat_total * 100 if mef_repeat_total > 0 else 0
    mef_badge = ' <span class="kpi-badge-inline">BEST</span>' if mef_is_best else ''
    return (
        f'<div class="kpi-card" style="--accent:{mef_accent}">'
        f'<div class="kpi-label">{mef_label} <span class="kpi-step">2nd pay same</span></div>'
        f'<div class="kpi-value-row"><span class="kpi-value">{mef_pct:.1f}%</span>{mef_badge}</div>'
        f'<div class="kpi-note">of {mef_entry_total:,} {mef_label} entry users</div>'
        f'</div>'
    )

# Строим KPI для всех entry methods
mef_kpi_data = []
for mef_m in mef_entry_methods_sorted:
    mef_same    = mef_get_same_method_val(mef_m)
    mef_repeat  = mef_get_repeat_total(mef_m)
    mef_entry_n = int(mef_entry_counts.get(mef_m, 0))
    mef_pct_val = mef_same / mef_repeat * 100 if mef_repeat > 0 else 0
    mef_kpi_data.append((mef_m, mef_same, mef_repeat, mef_entry_n, mef_pct_val))

# Лучший метод по retention — получает бейдж BEST
mef_best_method = max(mef_kpi_data, key=lambda x: x[4])[0] if mef_kpi_data else ''

mef_kpi_html = ''
for mef_m, mef_same, mef_repeat, mef_entry_n, _ in mef_kpi_data:
    mef_kpi_html += mef_make_kpi(
        mef_m, mef_same, mef_repeat, mef_entry_n,
        MEF_PID_COLORS.get(mef_m, '#94a3b8'),
        mef_is_best=(mef_m == mef_best_method)
    )


# =========================
# 7) Step labels
# =========================

mef_ord = ['1st','2nd','3rd','4th','5th','6th','7th','8th','9th','10th','11th']
mef_step_labels = [f"{mef_ord[i]} Payment" for i in range(MEF_MAX_STEPS + 1)]
mef_step_sub    = [f"{mef_total_users:,} users"] + [
    f"{mef_step_totals.get(mef_s,0):,} users" for mef_s in range(1, MEF_MAX_STEPS + 1)
]

mef_subtitle = "Projects with Xsolla Pay enabled \u00b7 2025 year \u00b7 Successful transactions only"


# =========================
# 8) Build HTML
# =========================

mef_html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3-sankey/0.12.3/d3-sankey.min.js"></script>
<style>
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  body{background:#f0f2f8;font-family:'DM Sans',sans-serif;color:#1e293b;padding:24px 16px 36px 32px}
  .pw{max-width:1700px;margin:0 auto}
  h1{font-size:19px;font-weight:700;color:#0f172a;letter-spacing:-.4px;line-height:1.2}
  .sub{font-size:11px;color:#64748b;margin-top:3px;margin-bottom:16px}

  /* KPI */
  .kr{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:20px}
  .kpi-card{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:11px 14px;
    min-width:110px;flex:1;position:relative;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06)}
  .kpi-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--accent,#6366f1)}
  .kpi-label{font-size:10px;color:#94a3b8;font-weight:500;line-height:1.3}
  .kpi-step{display:inline-block;background:#f1f5f9;color:#94a3b8;font-size:8px;font-weight:600;
    padding:1px 5px;border-radius:4px;letter-spacing:.05em;margin-left:3px;vertical-align:middle}
  .kpi-value-row{display:flex;align-items:baseline;gap:7px;margin-top:2px}
  .kpi-value{font-size:22px;font-weight:700;color:#0f172a;font-family:'DM Mono',monospace;line-height:1.2}
  .kpi-badge-inline{display:inline-block;background:#f0fdf4;color:#16a34a;font-size:9px;font-weight:700;
    padding:2px 7px;border-radius:99px;letter-spacing:.06em;border:1px solid #86efac;
    white-space:nowrap;vertical-align:middle}
  .kpi-note{font-size:9px;color:#c7d2fe;margin-top:2px}

  /* Chart */
  .cw{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px 16px;box-shadow:0 1px 4px rgba(0,0,0,.06)}
  .cw svg{display:block;width:100%}

  /* Tooltip */
  #mef-tt{position:fixed;background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:10px 14px;
    font-size:12px;pointer-events:none;opacity:0;transition:opacity .15s;
    box-shadow:0 8px 32px rgba(0,0,0,.12);z-index:1000;max-width:340px;line-height:1.5;font-family:'DM Sans',sans-serif}
  .ttt{font-weight:600;color:#0f172a;margin-bottom:6px;font-size:12px}
  .ttr{display:flex;justify-content:space-between;gap:16px;color:#94a3b8;font-size:11px;padding:1px 0}
  .ttr span:last-child{color:#1e293b;font-family:'DM Mono',monospace;font-weight:500}
  .tts{color:#94a3b8;margin:7px 0 3px;font-size:10px;border-top:1px solid #f1f5f9;padding-top:6px;
    font-weight:600;letter-spacing:.04em;text-transform:uppercase}

  /* Legend */
  .lr{display:flex;flex-wrap:wrap;gap:10px;margin-top:14px;padding-top:12px;border-top:1px solid #f1f5f9}
  .li{display:flex;align-items:center;gap:5px;font-size:11px;color:#64748b}
  .ld{width:8px;height:8px;border-radius:3px;flex-shrink:0}

  /* Sankey */
  .sankey-link{cursor:pointer}.sankey-link path{transition:opacity .2s}
  .sankey-node{cursor:pointer}.mef-col-h{pointer-events:none}

  /* Metrics description */
  .metrics-desc{margin-top:24px;padding-top:20px;border-top:1px solid #f1f5f9}
  .md-title{font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;
    letter-spacing:.08em;margin-bottom:14px;font-family:'DM Sans',sans-serif}
  .md-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
  .md-item{background:#f8fafc;border:1px solid #f1f5f9;border-radius:8px;padding:12px 14px}
  .md-name{font-size:11px;font-weight:700;color:#6366f1;margin-bottom:6px;
    font-family:'DM Sans',sans-serif;letter-spacing:-.01em}
  .md-text{font-size:11px;color:#64748b;line-height:1.65;font-family:'DM Sans',sans-serif}
  .md-text em{font-style:normal;color:#0f172a;font-weight:600}
</style>
</head>
<body>
<div class="pw">
  <h1>Payment Method Flow &middot; Multi-Entry</h1>
  <div class="sub">__SUBTITLE__</div>
  <div class="kr">__KPI_HTML__</div>
  <div class="cw">
    <div id="mef-chart"></div>
    <div class="lr" id="mef-legend"></div>
    <div class="metrics-desc">
      <div class="md-title">Metrics glossary &nbsp;&middot;&nbsp; hover on any flow or node to see values</div>
      <div class="md-grid">
        <div class="md-item">
          <div class="md-name">Preference Rate prev step %</div>
          <div class="md-text">
            Share of users who chose a payment method among <em>all users who made any next payment</em>
            from the same method node at the previous step.
            Shows how well a method retains its audience between two consecutive steps.
          </div>
        </div>
        <div class="md-item">
          <div class="md-name">Total Preference Rate %</div>
          <div class="md-text">
            Share of users on a specific transition out of <em>all entry users of that method</em> at step 0
            &mdash; the full original cohort for each method.
            Tracks what fraction of the starting audience is still on a given path,
            regardless of how many steps have passed.
          </div>
        </div>
        <div class="md-item">
          <div class="md-name">% of step total</div>
          <div class="md-text">
            Share of users on a specific transition among <em>all users who made a payment
            at the same step number</em>, across all methods.
            Contextualises the relative weight of a flow within the full payment activity at that step.
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
<div id="mef-tt"></div>

<script>
const mef_ND=__NODES__,mef_LK=__LINKS__,mef_ST=__STEP_TOT__,mef_SL=__STEP_LBL__,
      mef_SS=__STEP_SUB__,mef_CL=__COLORS__,mef_MS=__MAX_STEPS__,
      mef_EU=__ENTRY_USERS__,mef_MPS=__METHOD_PER_STEP__;

const mef_W=Math.max(1400,(window.innerWidth||1500)-40),mef_H=760,
      mef_M={top:72,right:130,bottom:24,left:130},
      mef_CW=mef_W-mef_M.left-mef_M.right,mef_CH=mef_H-mef_M.top-mef_M.bottom,
      mef_NW=14,mef_NP=8,mef_MH=4;

const mef_svg=d3.select('#mef-chart').append('svg').attr('viewBox',`0 0 ${mef_W} ${mef_H}`);
const mef_defs=mef_svg.append('defs');
const mef_g=mef_svg.append('g').attr('transform',`translate(${mef_M.left},${mef_M.top})`);

const mef_sk=d3.sankey().nodeId(d=>d.id).nodeAlign(d=>d.step)
  .nodeSort((a,b)=>{
    if(a.step!==b.step)return 0;
    if(a.step===0){
      const ua=mef_EU[a.name]||0,ub=mef_EU[b.name]||0;
      return ub-ua;
    }
    if(a.name==='Other'&&b.name!=='Other')return 1;
    if(b.name==='Other'&&a.name!=='Other')return-1;
    const va=(a.targetLinks||[]).reduce((s,l)=>s+l.value,0)||(a.sourceLinks||[]).reduce((s,l)=>s+l.value,0);
    const vb=(b.targetLinks||[]).reduce((s,l)=>s+l.value,0)||(b.sourceLinks||[]).reduce((s,l)=>s+l.value,0);
    return vb-va;
  }).nodeWidth(mef_NW).nodePadding(mef_NP).extent([[0,0],[mef_CW,mef_CH]]);

const mef_nodes=mef_ND.map(d=>({...d})),mef_links=mef_LK.map(d=>({...d}));
const mef_gr=mef_sk({nodes:mef_nodes,links:mef_links});

mef_gr.links.forEach((lk,i)=>{
  const g2=mef_defs.append('linearGradient').attr('id',`mefg${i}`).attr('gradientUnits','userSpaceOnUse')
    .attr('x1',lk.source.x1).attr('x2',lk.target.x0);
  g2.append('stop').attr('offset','0%').attr('stop-color',mef_CL[lk.source.name]||'#94a3b8');
  g2.append('stop').attr('offset','100%').attr('stop-color',mef_CL[lk.target.name]||'#94a3b8');
});

// Column headers
const mef_sx={};
mef_gr.nodes.forEach(n=>{if(!(n.step in mef_sx))mef_sx[n.step]=n.x0+mef_NW/2;});
for(let s=0;s<=mef_MS;s++){
  const cx=mef_sx[s];if(cx===undefined)continue;
  const hg=mef_g.append('g').attr('class','mef-col-h');
  hg.append('rect').attr('x',cx-42).attr('y',-mef_M.top+4).attr('width',84).attr('height',20).attr('rx',5)
    .attr('fill',s===0?'#f0fdf4':'#f8fafc').attr('stroke',s===0?'#86efac':'#e2e8f0').attr('stroke-width',1);
  hg.append('text').attr('x',cx).attr('y',-mef_M.top+17).attr('text-anchor','middle')
    .attr('fill',s===0?'#16a34a':'#94a3b8').attr('font-size',10)
    .attr('font-family',"'DM Sans',sans-serif").attr('font-weight',600).attr('letter-spacing','.04em')
    .text((mef_SL[s]||'').toUpperCase());
  hg.append('text').attr('x',cx).attr('y',-mef_M.top+40).attr('text-anchor','middle')
    .attr('fill','#94a3b8').attr('font-size',10).attr('font-family',"'DM Mono',monospace")
    .text(mef_SS[s]||'');
  if(s>0)mef_g.append('line').attr('x1',cx).attr('y1',-mef_M.top+50).attr('x2',cx).attr('y2',mef_CH)
    .attr('stroke','#e2e8f0').attr('stroke-width',1).attr('stroke-dasharray','3,5');
}

// Links
const mef_lp=d3.sankeyLinkHorizontal();
const mef_ls=mef_g.append('g').selectAll('.sankey-link').data(mef_gr.links).enter().append('g').attr('class','sankey-link');
mef_ls.append('path').attr('d',mef_lp).attr('fill','none')
  .attr('stroke',(d,i)=>`url(#mefg${i})`).attr('stroke-width',d=>Math.max(1,d.width)).attr('opacity',.28);

// Nodes
const mef_ns=mef_g.append('g').selectAll('.sankey-node').data(mef_gr.nodes).enter().append('g').attr('class','sankey-node')
  .attr('transform',d=>`translate(${d.x0},${d.y0})`);
mef_ns.append('rect').attr('width',mef_NW).attr('height',d=>Math.max(mef_MH,d.y1-d.y0))
  .attr('rx',3).attr('fill',d=>mef_CL[d.name]||'#94a3b8').attr('opacity',.88);
// Highlight all entry methods with outline
mef_ns.filter(d=>d.step===0||(d.name in mef_EU&&mef_EU[d.name]>0)).append('rect')
  .attr('width',mef_NW).attr('height',d=>Math.max(mef_MH,d.y1-d.y0))
  .attr('rx',3).attr('fill','none')
  .attr('stroke',d=>mef_CL[d.name]||'#94a3b8').attr('stroke-width',1.2).attr('opacity',.5);

// ── Node labels: name + drop prev % ──────────────────────────────────────────
const mef_lg=mef_g.append('g');
mef_gr.nodes.forEach(d=>{
  const h=Math.max(mef_MH,d.y1-d.y0),cy=d.y0+h/2;
  const isEntry = d.step===0;
  if(h<6&&!isEntry)return;

  // Always right of bar; first column: left
  const xpos = isEntry ? d.x0-7 : d.x1+7;
  const an   = isEntry ? 'end'  : 'start';
  const ll   = mef_lg.append('g');

  // Name
  ll.append('text').attr('x',xpos).attr('y',cy-(h>14?5:0)).attr('text-anchor',an)
    .attr('fill',mef_CL[d.name]||'#64748b')
    .attr('font-size',isEntry?12:10).attr('font-weight',isEntry?600:400)
    .attr('font-family',"'DM Sans',sans-serif").attr('dominant-baseline',h<=14?'middle':'auto')
    .text(d.name);

  // Step 0: entry count
  if(isEntry && h>14){
    ll.append('text').attr('x',xpos).attr('y',cy+6).attr('text-anchor',an)
      .attr('fill',mef_CL[d.name]||'#64748b').attr('font-size',9)
      .attr('font-family',"'DM Mono',monospace").attr('font-weight',500)
      .text(`${(mef_EU[d.name]||0).toLocaleString()} entry`);
    return;
  }

  // Steps > 0: drop prev %
  if(h>14){
    const prevU = mef_MPS[d.name]&&mef_MPS[d.name][String(d.step-1)];
    const thisU = mef_MPS[d.name]&&mef_MPS[d.name][String(d.step)]||0;
    if(prevU&&prevU>0){
      const dropPct=((prevU-thisU)/prevU*100).toFixed(1);
      ll.append('text').attr('x',xpos).attr('y',cy+6).attr('text-anchor',an)
        .attr('fill','#ef4444').attr('font-size',8.5)
        .attr('font-family',"'DM Mono',monospace").attr('font-weight',500)
        .text(`drop \u2193 ${dropPct}%`);
    }
  }
});

// ── Tooltips ──────────────────────────────────────────────────────────────────
const mef_tt=d3.select('#mef-tt'),mef_fmt=n=>n.toLocaleString();
const mef_sT=(h,e)=>mef_tt.html(h).style('opacity',1).style('left',(e.clientX+14)+'px').style('top',(e.clientY-10)+'px');
const mef_hT=()=>mef_tt.style('opacity',0);
const mef_mT=e=>mef_tt.style('left',(e.clientX+14)+'px').style('top',(e.clientY-10)+'px');

mef_ls.on('mouseover',function(ev,d){
  mef_ls.selectAll('path').attr('opacity',.05);d3.select(this).select('path').attr('opacity',.75);
  // % of source = val / all outgoing from source node (Preference Rate prev step)
  const srcTot   = (d.source.sourceLinks||[]).reduce((s,l)=>s+l.value,0)||(mef_EU[d.source.name]||1);
  const prevPct  = (d.value/srcTot*100).toFixed(1);
  // Total Preference Rate = val / entry users of the source method
  const entryTot = mef_EU[d.source.name]||srcTot;
  const totalPct = (d.value/entryTot*100).toFixed(1);
  // % of step total
  const stepTot  = d.source.step===0?(mef_EU[d.source.name]||1):(mef_ST[String(d.source.step)]||1);
  const stepPct  = (d.value/stepTot*100).toFixed(1);
  mef_sT(`
    <div class="ttt">${d.source.name} \u2192 ${d.target.name}</div>
    <div class="ttr"><span>Departed \u2192 Arriving</span><span>${mef_fmt(d.value)}</span></div>
    <div class="ttr"><span>Preference Rate prev step %</span><span>${prevPct}%</span></div>
    <div class="ttr"><span>Total Preference Rate %</span><span>${totalPct}%</span></div>
    <div class="ttr"><span>% of step total</span><span>${stepPct}%</span></div>
    <div class="tts">\u2191 SOURCE (step ${d.source.step+1})</div>
    <div class="ttr"><span>${d.source.name} users</span><span>${mef_fmt(srcTot)}</span></div>
  `,ev);
}).on('mousemove',function(ev){mef_mT(ev);}).on('mouseout',function(){mef_ls.selectAll('path').attr('opacity',.28);mef_hT();});

mef_ns.on('mouseover',function(ev,d){
  const cL=new Set(),cN=new Set([d.id]);
  (d.sourceLinks||[]).forEach(l=>{cL.add(l.index);cN.add(l.target.id);});
  (d.targetLinks||[]).forEach(l=>{cL.add(l.index);cN.add(l.source.id);});
  mef_ls.selectAll('path').attr('opacity',(ld,i)=>cL.has(i)?.68:.04);
  mef_ns.selectAll('rect:first-child').attr('opacity',nd=>cN.has(nd.id)?.95:.18);

  const inc     = (d.targetLinks||[]).reduce((s,l)=>s+l.value,0);
  const out     = (d.sourceLinks||[]).reduce((s,l)=>s+l.value,0);
  const stepTot = d.step===0?(mef_EU[d.name]||1):(mef_ST[String(d.step)]||1);
  const prevSame     = mef_MPS[d.name]&&mef_MPS[d.name][String(d.step-1)];
  const thisSame     = mef_MPS[d.name]&&mef_MPS[d.name][String(d.step)]||inc;
  const nextSameLink = (d.sourceLinks||[]).find(l=>l.target.name===d.name);
  const nextSame     = nextSameLink?nextSameLink.value:0;
  const prefPrevPct  = prevSame&&prevSame>0?(thisSame/prevSame*100).toFixed(1):null;

  const sortedOut = [...(d.sourceLinks||[])].sort((a,b)=>b.value-a.value).slice(0,5);
  const nextHtml  = out>0?sortedOut.map(l=>
    `<div class="ttr"><span>\u2192 ${l.target.name}</span><span>${mef_fmt(l.value)} (${(l.value/out*100).toFixed(1)}%)</span></div>`
  ).join(''):'';

  const tgtInc  = (d.targetLinks||[]).reduce((s,l)=>s+l.value,0);
  const sortedSrc = [...(d.targetLinks||[])].sort((a,b)=>b.value-a.value).slice(0,3);
  const srcHtml = tgtInc>0?sortedSrc.map(l=>
    `<div class="ttr"><span>\u2190 ${l.source.name}</span><span>${mef_fmt(l.value)} (${(l.value/tgtInc*100).toFixed(1)}%)</span></div>`
  ).join(''):'';

  if(d.step===0){
    mef_sT(`
      <div class="ttt">${d.name} \u00b7 Step 1</div>
      <div class="ttr"><span>Entry users</span><span>${mef_fmt(mef_EU[d.name]||0)}</span></div>
      <div class="ttr"><span>With repeat purchase</span><span>${mef_fmt(out)}</span></div>
      ${nextHtml?`<div class="tts">\u2192 Next choice</div>${nextHtml}`:''}
    `,ev);
    return;
  }

  mef_sT(`
    <div class="ttt">${d.name} \u00b7 Step ${d.step+1}</div>
    ${prevSame?`<div class="ttr"><span>${d.name} at prev step</span><span>${mef_fmt(prevSame)}</span></div>`:''}
    <div class="ttr"><span>${d.name} at this step</span><span>${mef_fmt(thisSame)}</span></div>
    ${prefPrevPct?`<div class="ttr"><span>Preference Rate prev step %</span><span>${prefPrevPct}%</span></div>`:''}
    ${nextSame>0?`<div class="ttr"><span>${d.name} at next step</span><span>${mef_fmt(nextSame)}</span></div>`:''}
    <div class="ttr"><span>Share of step</span><span>${(Math.max(inc,out)/stepTot*100).toFixed(1)}%</span></div>
    ${nextHtml?`<div class="tts">\u2192 Next choice</div>${nextHtml}`:''}
    ${srcHtml?`<div class="tts">\u2190 Top sources into ${d.name} (step ${d.step+1})</div>${srcHtml}`:''}
  `,ev);
}).on('mousemove',function(ev){mef_mT(ev);})
  .on('mouseout',function(){mef_ls.selectAll('path').attr('opacity',.28);mef_ns.selectAll('rect:first-child').attr('opacity',.88);mef_hT();});

// Legend
const mef_le=document.getElementById('mef-legend'),mef_un=new Set(mef_gr.nodes.map(n=>n.name));
Object.entries(mef_CL).filter(([n])=>mef_un.has(n)).forEach(([n,c])=>{
  const it=document.createElement('div');it.className='li';
  it.innerHTML=`<div class="ld" style="background:${c}"></div>${n}`;mef_le.appendChild(it);
});

// Animation
mef_ls.selectAll('path')
  .attr('stroke-dasharray',function(){return this.getTotalLength()+' '+this.getTotalLength();})
  .attr('stroke-dashoffset',function(){return this.getTotalLength();})
  .transition().duration(1400).delay((d,i)=>50+i*4).ease(d3.easeCubicOut)
  .attr('stroke-dashoffset',0).on('end',function(){d3.select(this).attr('stroke-dasharray',null);});
mef_ns.attr('opacity',0).transition().duration(400).delay(d=>d.step*70+120).attr('opacity',1);
</script></body></html>"""

mef_html = (mef_html
    .replace('__SUBTITLE__',        mef_subtitle)
    .replace('__KPI_HTML__',        mef_kpi_html)
    .replace('__NODES__',           json.dumps(mef_all_nodes))
    .replace('__LINKS__',           json.dumps(mef_links_data))
    .replace('__STEP_TOT__',        json.dumps({str(k):v for k,v in mef_step_totals.items()}))
    .replace('__STEP_LBL__',        json.dumps(mef_step_labels))
    .replace('__STEP_SUB__',        json.dumps(mef_step_sub))
    .replace('__COLORS__',          json.dumps(MEF_PID_COLORS))
    .replace('__MAX_STEPS__',       str(MEF_MAX_STEPS))
    .replace('__ENTRY_USERS__',     mef_entry_users_json)
    .replace('__METHOD_PER_STEP__', mef_mps_json)
)


# =========================
# 9) Confluence export
# =========================

mef_conf = (
    mef_html
    .replace("background:#f0f2f8;", "background:transparent;")
    .replace("padding:24px 16px 36px 32px;", "padding:8px 4px 16px;")
    .replace("max-width:1700px;", "max-width:100%;")
)

mef_export = (
    '<div style="font-family:sans-serif;margin-top:20px;border-radius:12px;'
    'border:1px solid #e2e8f0;overflow:hidden;box-shadow:0 1px 6px rgba(0,0,0,.07);">'
    '<button onclick="var b=document.getElementById(\'mefcb\');var a=document.getElementById(\'mefca\');'
    'var o=b.style.display!==\'none\';b.style.display=o?\'none\':\'block\';'
    'a.style.transform=o?\'rotate(0deg)\':\'rotate(180deg)\';"'
    ' style="width:100%;background:#f8fafc;border:none;border-bottom:1px solid #e2e8f0;'
    'padding:14px 20px;display:flex;align-items:center;justify-content:space-between;cursor:pointer;">'
    '<div><div style="font-size:13px;font-weight:600;color:#0f172a;">Confluence HTML'
    '<span style="background:#eef2ff;color:#6366f1;font-size:10px;font-weight:600;'
    'padding:2px 8px;border-radius:99px;margin-left:6px;">READY TO PASTE</span></div>'
    '<div style="font-size:11px;color:#94a3b8;margin-top:1px;">'
    '&#8594; скопируй &#8594; вставь в HTML Macro (height: 1000px)</div></div>'
    '<span id="mefca" style="transition:transform .25s;font-size:18px;color:#94a3b8;">&#8964;</span>'
    '</button>'
    '<div id="mefcb" style="display:none;background:#fff;">'
    '<div style="position:relative;background:#f8fafc;border-bottom:1px solid #f1f5f9;">'
    '<textarea id="mefcc" readonly style="width:100%;height:140px;font-family:monospace;font-size:11px;'
    'color:#475569;background:transparent;border:none;padding:14px 20px;'
    'resize:none;outline:none;line-height:1.6;box-sizing:border-box;">'
    + mef_conf.replace("'", "&#39;") +
    '</textarea>'
    '<div id="mefck" style="position:absolute;bottom:8px;right:14px;font-size:10px;'
    'color:#cbd5e1;font-family:monospace;pointer-events:none;"></div></div>'
    '<div style="padding:12px 20px;display:flex;align-items:center;gap:12px;">'
    '<button id="mefcpb" onclick="mefCopy()" style="background:#6366f1;color:#fff;border:none;'
    'border-radius:8px;padding:9px 20px;font-size:13px;font-weight:600;cursor:pointer;transition:background .2s;">'
    'Copy to clipboard</button>'
    '<span id="mefcps" style="font-size:12px;color:#94a3b8;"></span>'
    '</div></div></div>'
    '<script>'
    'var mefta=document.getElementById("mefcc");'
    'document.getElementById("mefck").textContent=(mefta.value.length/1024).toFixed(1)+" KB";'
    'function mefCopy(){'
    'navigator.clipboard.writeText(mefta.value).then(function(){'
    'var btn=document.getElementById("mefcpb"),st=document.getElementById("mefcps");'
    'btn.style.background="#16a34a";btn.textContent="Copied!";'
    'st.textContent="Скопировано!";st.style.color="#16a34a";'
    'setTimeout(function(){btn.style.background="#6366f1";btn.textContent="Copy to clipboard";st.textContent="";},3000);'
    '}).catch(function(){mefta.select();document.execCommand("copy");});}'
    '</script>'
)

display(HTML(f'<div style="border-radius:16px;overflow:hidden;">{mef_html}</div>'))
display(HTML(mef_export))
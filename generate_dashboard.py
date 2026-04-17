#!/usr/bin/env python3
"""
Generates AI Summaries Dashboard HTML from BigQuery data.
Run: python3 generate_dashboard.py
"""

import json
import os
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")

# Support CI: write GCP key from env var to a temp file, or use local path
if os.environ.get("GCP_KEY_JSON"):
    import tempfile
    _key_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    _key_file.write(os.environ["GCP_KEY_JSON"])
    _key_file.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _key_file.name
else:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/user/Desktop/JsonKey/gcp-key.json"

from google.cloud import bigquery

client = bigquery.Client()


def query(sql):
    return client.query(sql).to_dataframe()


print("Querying adoption data...")
adoption_df = query("""
with ai_customers as (
    select id as customer_id
    from `brighterly-gcp.main_app_prod.customers`
    where ai_summarize = 1
      and status not in ('DRAFT', 'JUNK', 'SYNC')
),
kid_to_customer as (
    select k.id as kid_id, k.customer_id
    from `brighterly-gcp.main_app_prod.kids` k
),
customers_with_recordings as (
    select distinct k.customer_id
    from `brighterly-gcp.main_app_prod.bookings` b
    join `brighterly-gcp.main_app_prod.disciplines` d on b.discipline_id = d.id
    join `brighterly-gcp.main_app_prod.kids` k on d.kid_id = k.id
    where b.recording_started_at is not null
),
eligible_customers as (
    select ac.customer_id
    from ai_customers ac
    join customers_with_recordings cr on ac.customer_id = cr.customer_id
),
page_views_per_customer as (
    select
        ktc.customer_id,
        count(*) as view_count
    from `brighterly-gcp.amplitude_data_5m.EVENTS_472284` e
    join kid_to_customer ktc
        on safe_cast(json_value(e.event_properties, '$.kid_id') as int64) = ktc.kid_id
    where e.event_type = 'Page_view_landing_summarize'
    group by 1
)
select
    count(distinct ac.customer_id) as total_ai_enabled,
    count(distinct ec.customer_id) as eligible_customers,
    count(distinct pvc.customer_id) as customers_with_any_views,
    count(distinct case when pvc.view_count > 2 then pvc.customer_id end) as active_adopters,
    round(safe_divide(
        count(distinct case when pvc.view_count > 2 then pvc.customer_id end),
        count(distinct ec.customer_id)
    ) * 100, 2) as adoption_rate_pct
from ai_customers ac
left join eligible_customers ec on ac.customer_id = ec.customer_id
left join page_views_per_customer pvc on ac.customer_id = pvc.customer_id
""")

print("Querying daily summary page views...")
daily_page_views_df = query("""
select
    date(event_time) as event_date,
    count(*) as page_views,
    count(distinct device_id) as unique_visitors
from `brighterly-gcp.amplitude_data_5m.EVENTS_472284`
where event_type = 'Page_view_landing_summarize'
group by 1
order by 1
""")

print("Querying total unique visitors...")
total_unique_visitors_df = query("""
select
    count(distinct device_id) as total_unique_visitors
from `brighterly-gcp.amplitude_data_5m.EVENTS_472284`
where event_type = 'Page_view_landing_summarize'
""")

print("Querying daily summary creations...")
daily_creations_df = query("""
with amp_creations as (
    select
        date(event_time) as event_date,
        count(*) as amplitude_creations,
        count(distinct device_id) as unique_creators
    from `brighterly-gcp.amplitude_data_5m.EVENTS_472284`
    where event_type = 'Ai_summary_created'
    group by 1
),
db_creations as (
    select
        date(created_at) as event_date,
        count(*) as db_summaries
    from `brighterly-gcp.main_app_prod.ai_summaries`
    group by 1
),
views as (
    select
        date(event_time) as event_date,
        count(*) as page_views
    from `brighterly-gcp.amplitude_data_5m.EVENTS_472284`
    where event_type = 'Page_view_landing_summarize'
    group by 1
)
select
    coalesce(a.event_date, d.event_date) as event_date,
    coalesce(a.amplitude_creations, 0) as amplitude_creations,
    coalesce(a.unique_creators, 0) as unique_creators,
    coalesce(d.db_summaries, 0) as db_summaries,
    coalesce(v.page_views, 0) as page_views,
    round(safe_divide(coalesce(d.db_summaries, 0), nullif(coalesce(v.page_views, 0), 0)) * 100, 2) as view_to_creation_rate
from amp_creations a
full outer join db_creations d using(event_date)
left join views v using(event_date)
order by 1
""")

print("Querying replay/recording data (AI-enabled customers only)...")
daily_replays_df = query("""
with ai_customers as (
    select id as customer_id
    from `brighterly-gcp.main_app_prod.customers`
    where ai_summarize = 1 and status not in ('DRAFT', 'JUNK', 'SYNC')
)
select
    date(b.start) as event_date,
    count(*) as replay_views,
    count(distinct k.customer_id) as unique_customers
from `brighterly-gcp.main_app_prod.bookings` b
join `brighterly-gcp.main_app_prod.disciplines` d on b.discipline_id = d.id
join `brighterly-gcp.main_app_prod.kids` k on d.kid_id = k.id
join ai_customers ac on k.customer_id = ac.customer_id
where b.recording_started_at is not null
group by 1
order by 1
""")

print("Querying replay per customer stats...")
replay_per_customer_df = query("""
with ai_customers as (
    select id as customer_id
    from `brighterly-gcp.main_app_prod.customers`
    where ai_summarize = 1 and status not in ('DRAFT', 'JUNK', 'SYNC')
),
per_customer as (
    select
        k.customer_id,
        count(*) as replay_views
    from `brighterly-gcp.main_app_prod.bookings` b
    join `brighterly-gcp.main_app_prod.disciplines` d on b.discipline_id = d.id
    join `brighterly-gcp.main_app_prod.kids` k on d.kid_id = k.id
    join ai_customers ac on k.customer_id = ac.customer_id
    where b.recording_started_at is not null
    group by 1
)
select
    count(*) as customers_with_replays,
    round(avg(replay_views), 1) as avg_replays_per_customer,
    min(replay_views) as min_replays,
    max(replay_views) as max_replays
from per_customer
""")

print("Querying summary page views per customer...")
summary_per_customer_df = query("""
with kid_to_customer as (
    select k.id as kid_id, k.customer_id
    from `brighterly-gcp.main_app_prod.kids` k
    join `brighterly-gcp.main_app_prod.customers` c on k.customer_id = c.id
    where c.ai_summarize = 1
      and c.status not in ('DRAFT', 'JUNK', 'SYNC')
),
per_customer as (
    select
        ktc.customer_id,
        count(*) as summary_views
    from `brighterly-gcp.amplitude_data_5m.EVENTS_472284` e
    join kid_to_customer ktc
        on safe_cast(json_value(e.event_properties, '$.kid_id') as int64) = ktc.kid_id
    where e.event_type = 'Page_view_landing_summarize'
    group by 1
)
select
    count(*) as customers_with_views,
    round(avg(summary_views), 1) as avg_views_per_customer,
    min(summary_views) as min_views,
    max(summary_views) as max_views
from per_customer
""")

print("Querying EXP-90 rebill rate...")
exp90_rebill_df = query("""
with exp_customers as (
    select
        id as customer_id,
        json_value(experiments, '$."exp-90_ai-summaries-for-new-paid-customers"') as variant
    from `brighterly-gcp.main_app_prod.customers`
    where json_value(experiments, '$."exp-90_ai-summaries-for-new-paid-customers"') is not null
)
select
    e.variant,
    count(*) as total_customers,
    countif(r.has_made_second_payment) as rebilled,
    countif(r.second_payment_status = 'LOST') as lost,
    countif(r.second_payment_status = 'PENDING') as pending,
    round(safe_divide(countif(r.has_made_second_payment), countif(r.second_payment_status != 'PENDING')) * 100, 2) as rebill_rate_pct
from exp_customers e
left join `brighterly-gcp.marts.cohort_rebill_rate` r on e.customer_id = r.customer_id
group by 1
order by 1
""")

print("Querying EXP-90 auto-renew conversion...")
exp90_autorenew_df = query("""
with exp_customers as (
    select
        id as customer_id,
        json_value(experiments, '$."exp-90_ai-summaries-for-new-paid-customers"') as variant
    from `brighterly-gcp.main_app_prod.customers`
    where json_value(experiments, '$."exp-90_ai-summaries-for-new-paid-customers"') is not null
)
select
    e.variant,
    count(distinct e.customer_id) as total_customers,
    count(distinct ar.subscription_id) as subs_with_renewals,
    countif(ar.expected_renewal) as expected_renewals,
    countif(ar.renewal_status = 'SUCCEEDED') as succeeded,
    countif(ar.renewal_status = 'IN_PROGRESS') as in_progress,
    countif(ar.renewal_status in ('CANCELED', 'PAUSED', 'COMPLETED')) as lost,
    round(safe_divide(
        countif(ar.renewal_status = 'SUCCEEDED'),
        countif(ar.renewal_status != 'IN_PROGRESS')
    ) * 100, 2) as autorenew_conversion_pct
from exp_customers e
left join `brighterly-gcp.marts.subscriptions` s on e.customer_id = s.customer_id
left join `brighterly-gcp.marts.auto_renewals` ar on s.id = ar.subscription_id and ar.expected_renewal
group by 1
order by 1
""")


def df_to_records(df):
    """Convert dataframe to list of dicts, handling date serialization."""
    records = df.to_dict(orient="records")
    for r in records:
        for k, v in r.items():
            if hasattr(v, "isoformat"):
                r[k] = v.isoformat()
            elif str(v) == "nan" or str(v) == "NaN":
                r[k] = 0
    return records


data = {
    "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    "adoption": df_to_records(adoption_df),
    "daily_page_views": df_to_records(daily_page_views_df),
    "total_unique_visitors": df_to_records(total_unique_visitors_df),
    "daily_creations": df_to_records(daily_creations_df),
    "daily_replays": df_to_records(daily_replays_df),
    "replay_per_customer": df_to_records(replay_per_customer_df),
    "summary_per_customer": df_to_records(summary_per_customer_df),
    "exp90_rebill": df_to_records(exp90_rebill_df),
    "exp90_autorenew": df_to_records(exp90_autorenew_df),
}

print("Generating HTML...")

html_template = open(
    os.path.join(os.path.dirname(__file__), "template.html"), "r"
).read()

html_output = html_template.replace("/*__DATA_PLACEHOLDER__*/{}", json.dumps(data, indent=2))

output_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
with open(output_path, "w") as f:
    f.write(html_output)

print(f"Dashboard generated: {output_path}")

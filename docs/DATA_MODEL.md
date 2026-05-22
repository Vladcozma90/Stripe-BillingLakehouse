# Data Model

## Overview

This document describes the analytical data model used by the `Stripe-BillingLakehouse` project.

The Gold layer exposes a dimensional model for billing and usage analytics. It combines Stripe billing data with ERP-style business data and publishes BI-ready dimensions and facts.

The model is intentionally focused on a small but realistic billing domain:

```text
customers
plans
subscriptions
invoices
subscription items
daily usage
````

The Gold layer is the only layer intended for direct BI/reporting consumption.

---

## Modeling approach

The project uses a star-schema-style model.

Main principles:

```text
Dimensions describe business entities.
Facts describe measurable business events.
Gold dimensions own surrogate keys.
Facts store foreign keys to Gold dimensions.
Silver business keys are used to resolve Gold dimensions.
Unknown dimension matches are mapped to -1 keys.
```

Silver owns cleaned, validated, and conformed data.

Gold owns the analytical model used by BI tools.

---

## Source-to-model overview

```text
s_conform_stripe_customers
        |
        |        s_conform_erp_account_master_snapshot
        |                |
        |                v
        +------> g_dim_customers


s_conform_erp_plan_catalog
        |
        v
g_dim_plan_catalog


s_conform_stripe_subscriptions
        |
        v
g_dim_subscriptions


s_conform_stripe_invoices
        |
        v
g_fact_stripe_invoices


s_conform_stripe_subscription_items
        |
        v
g_fact_stripe_subscription_items


s_conform_erp_usage_daily
        |
        v
g_fact_usage_daily
```

The most important modeling point is that Gold may combine multiple Silver conform sources into one business-facing dimension.

Example:

```text
s_conform_stripe_customers
s_conform_erp_account_master_snapshot
        |
        v
g_dim_customers
```

This avoids exposing source-system fragmentation to BI users.

---

## Gold tables

| Table                              | Type      | Purpose                                   |
| ---------------------------------- | --------- | ----------------------------------------- |
| `g_dim_customers`                  | Dimension | Customer/account reporting dimension      |
| `g_dim_plan_catalog`               | Dimension | Product/plan reporting dimension          |
| `g_dim_subscriptions`              | Dimension | Subscription reporting dimension          |
| `g_fact_stripe_invoices`           | Fact      | Invoice-level billing events              |
| `g_fact_stripe_subscription_items` | Fact      | Subscription item-level billing structure |
| `g_fact_usage_daily`               | Fact      | Daily account/plan usage metrics          |

---

# Dimensions

## `g_dim_customers`

### Purpose

`g_dim_customers` provides a business-facing customer/account dimension.

It combines Stripe customer attributes with ERP account attributes.

### Source Silver tables

```text
s_conform_stripe_customers
s_conform_erp_account_master_snapshot
```

### Expected grain

```text
One row per customer/account business entity version.
```

If history is preserved in Gold, the effective timestamp columns define the validity period of each version.

### Primary Gold key

```text
customer_sk
```

This is the surrogate key used by Gold facts.

### Business keys

Typical business/natural keys:

```text
customer_business_key
account_id
stripe_customer_id
```

### Typical attributes

```text
customer_name
email
account_id
stripe_customer_id
plan_code
segment
country_code
region
account_status
account_created_at
churned_at
source_system
effective_start_ts
effective_end_ts
is_current
```

### Unknown row

Gold should contain an unknown customer row:

```text
customer_sk = -1
customer_business_key = 'UNKNOWN'
```

Facts use `customer_sk = -1` when no reliable customer match is found.

---

## `g_dim_plan_catalog`

### Purpose

`g_dim_plan_catalog` provides plan/product metadata used for revenue and usage analytics.

### Source Silver table

```text
s_conform_erp_plan_catalog
```

### Expected grain

```text
One row per plan version.
```

If plan attributes change over time, the plan should be represented with effective validity columns.

### Primary Gold key

```text
plan_sk
```

### Business keys

Typical business/natural keys:

```text
plan_business_key
plan_code
```

### Typical attributes

```text
plan_code
plan_name
plan_family
billing_period
currency
unit_price
is_active
effective_start_ts
effective_end_ts
is_current
```

### Unknown row

Gold should contain an unknown plan row:

```text
plan_sk = -1
plan_business_key = 'UNKNOWN'
plan_code = 'UNKNOWN'
```

Facts use `plan_sk = -1` when no reliable plan match is found.

---

## `g_dim_subscriptions`

### Purpose

`g_dim_subscriptions` provides subscription-level context for invoice and subscription-item analytics.

### Source Silver table

```text
s_conform_stripe_subscriptions
```

### Expected grain

```text
One row per subscription version.
```

### Primary Gold key

```text
subscription_sk
```

### Business keys

Typical business/natural keys:

```text
subscription_business_key
subscription_id
stripe_subscription_id
```

### Typical attributes

```text
subscription_id
stripe_customer_id
status
billing_cycle_anchor
current_period_start
current_period_end
cancel_at
canceled_at
created_at
effective_start_ts
effective_end_ts
is_current
```

### Unknown row

Gold should contain an unknown subscription row:

```text
subscription_sk = -1
subscription_business_key = 'UNKNOWN'
subscription_id = 'UNKNOWN'
```

Facts use `subscription_sk = -1` when no reliable subscription match is found.

---

# Facts

## `g_fact_stripe_invoices`

### Purpose

`g_fact_stripe_invoices` represents invoice-level billing events from Stripe.

### Source Silver table

```text
s_conform_stripe_invoices
```

### Expected grain

```text
One row per Stripe invoice.
```

### Primary business key

```text
invoice_id
```

### Foreign keys

Expected dimensional foreign keys:

```text
customer_sk
subscription_sk
```

### Typical measures

```text
amount_due
amount_paid
amount_remaining
subtotal
total
tax_amount
discount_amount
```

### Degenerate dimensions

These identifiers are kept directly on the fact because they describe the transaction itself:

```text
invoice_id
invoice_number
currency
invoice_status
collection_method
```

### Typical dates/timestamps

```text
invoice_created_at
period_start
period_end
due_date
paid_at
voided_at
```

### Dimension resolution

Typical lookup logic:

```text
invoice.stripe_customer_id -> g_dim_customers.stripe_customer_id
invoice.subscription_id    -> g_dim_subscriptions.subscription_id
```

If a lookup fails:

```text
customer_sk = -1
subscription_sk = -1
```

---

## `g_fact_stripe_subscription_items`

### Purpose

`g_fact_stripe_subscription_items` represents subscription item-level billing structure.

This table is useful for analyzing which products/plans exist inside each subscription.

### Source Silver table

```text
s_conform_stripe_subscription_items
```

### Expected grain

```text
One row per Stripe subscription item.
```

### Primary business key

```text
subscription_item_id
```

### Foreign keys

Expected dimensional foreign keys:

```text
subscription_sk
customer_sk
plan_sk
```

### Typical measures

```text
quantity
unit_amount
recurring_interval_count
```

### Degenerate dimensions

```text
subscription_item_id
price_id
subscription_id
currency
billing_scheme
```

### Dimension resolution

Typical lookup logic:

```text
subscription_id    -> g_dim_subscriptions.subscription_id
stripe_customer_id -> g_dim_customers.stripe_customer_id
plan_code          -> g_dim_plan_catalog.plan_code
```

If a lookup fails:

```text
subscription_sk = -1
customer_sk = -1
plan_sk = -1
```

---

## `g_fact_usage_daily`

### Purpose

`g_fact_usage_daily` represents daily usage activity from ERP data.

This table connects internal usage metrics with customer and plan dimensions.

### Source Silver table

```text
s_conform_erp_usage_daily
```

### Expected grain

```text
One row per account / plan / usage date.
```

### Primary business key

A practical business key can be built from:

```text
account_id
plan_code
usage_date
```

### Foreign keys

Expected dimensional foreign keys:

```text
customer_sk
plan_sk
```

### Typical measures

```text
usage_units
billable_usage_units
usage_amount
```

### Typical date

```text
usage_date
```

### Dimension resolution

Typical lookup logic:

```text
usage.account_id -> g_dim_customers.account_id
usage.plan_code  -> g_dim_plan_catalog.plan_code
```

If a lookup fails:

```text
customer_sk = -1
plan_sk = -1
```

---

# Grain summary

| Table                              | Grain                                   |
| ---------------------------------- | --------------------------------------- |
| `g_dim_customers`                  | One row per customer/account version    |
| `g_dim_plan_catalog`               | One row per plan version                |
| `g_dim_subscriptions`              | One row per subscription version        |
| `g_fact_stripe_invoices`           | One row per invoice                     |
| `g_fact_stripe_subscription_items` | One row per subscription item           |
| `g_fact_usage_daily`               | One row per account / plan / usage date |

---

# Key strategy

## Silver keys

Silver conform tables should not expose dimensional surrogate keys.

Silver should rely on:

```text
business keys
source/natural keys
effective timestamps
record_hash
is_current
audit columns
```

Examples:

```text
account_id
stripe_customer_id
subscription_id
plan_code
invoice_id
subscription_item_id
```

Silver is the conformed business/technical layer, not the reporting star schema.

## Gold keys

Gold dimensions own BI-facing surrogate keys.

Examples:

```text
customer_sk BIGINT
plan_sk BIGINT
subscription_sk BIGINT
```

Gold facts store these keys as foreign keys.

Examples:

```text
g_fact_stripe_invoices.customer_sk
g_fact_stripe_invoices.subscription_sk

g_fact_stripe_subscription_items.customer_sk
g_fact_stripe_subscription_items.subscription_sk
g_fact_stripe_subscription_items.plan_sk

g_fact_usage_daily.customer_sk
g_fact_usage_daily.plan_sk
```

Gold surrogate keys are created in Gold because Gold is the reporting model.

This keeps the separation clean:

```text
Silver = business keys, conformance, SCD2 history
Gold   = BI-facing surrogate keys and facts
```

---

# Unknown key strategy

Gold dimensions should include unknown rows with `-1` surrogate keys.

Recommended unknown rows:

```text
g_dim_customers:
    customer_sk = -1

g_dim_plan_catalog:
    plan_sk = -1

g_dim_subscriptions:
    subscription_sk = -1
```

Gold facts should not fail or drop valid business events only because a dimension lookup is missing.

Instead, unresolved dimension references should be mapped to `-1`.

This keeps BI behavior predictable and prevents fact loss.

---

# SCD2 handling

SCD2 history is primarily maintained in Silver conform tables.

Silver tracks historical changes using:

```text
business_key
record_hash
effective_start_ts
effective_end_ts
is_current
```

Gold dimensions may either:

```text
consume current Silver records only
```

or:

```text
preserve selected history from Silver
```

depending on the analytical requirement.

For this project, the recommended pattern is:

```text
Silver owns conformed SCD2 history.
Gold exposes the history needed for reporting.
Facts resolve dimension keys using business key + event date where history matters.
```

Example as-of lookup:

```text
fact.usage_date >= dim.effective_start_ts
AND fact.usage_date < coalesce(dim.effective_end_ts, '9999-12-31')
```

If only current reporting is required, facts may resolve against current dimension rows using:

```text
dim.is_current = true
```

---

# Fact and dimension relationship summary

```text
g_dim_customers
    customer_sk
        |
        +-- g_fact_stripe_invoices.customer_sk
        +-- g_fact_stripe_subscription_items.customer_sk
        +-- g_fact_usage_daily.customer_sk


g_dim_subscriptions
    subscription_sk
        |
        +-- g_fact_stripe_invoices.subscription_sk
        +-- g_fact_stripe_subscription_items.subscription_sk


g_dim_plan_catalog
    plan_sk
        |
        +-- g_fact_stripe_subscription_items.plan_sk
        +-- g_fact_usage_daily.plan_sk
```

---

# Recommended BI model

In Power BI or another BI tool, the Gold model should be imported using one-to-many relationships:

```text
g_dim_customers[customer_sk]          1 -> * g_fact_stripe_invoices[customer_sk]
g_dim_customers[customer_sk]          1 -> * g_fact_stripe_subscription_items[customer_sk]
g_dim_customers[customer_sk]          1 -> * g_fact_usage_daily[customer_sk]

g_dim_subscriptions[subscription_sk]  1 -> * g_fact_stripe_invoices[subscription_sk]
g_dim_subscriptions[subscription_sk]  1 -> * g_fact_stripe_subscription_items[subscription_sk]

g_dim_plan_catalog[plan_sk]           1 -> * g_fact_stripe_subscription_items[plan_sk]
g_dim_plan_catalog[plan_sk]           1 -> * g_fact_usage_daily[plan_sk]
```

Recommended filter direction:

```text
Dimension filters Fact
```

Avoid many-to-many relationships unless there is a clear business requirement.

---

# Design decisions

## Why combine ERP and Stripe in `g_dim_customers`

ERP account data and Stripe customer data describe the same business concept from different systems.

Keeping them separate in Gold would make reporting harder.

The Gold customer dimension gives BI users a single customer/account entity.

## Why facts use Gold surrogate keys

Facts should not join directly to Silver tables in BI.

Gold facts use Gold dimension keys because:

```text
joins are simpler
BI relationships are cleaner
source-system complexity is hidden
unknown rows are handled consistently
the model follows dimensional modeling conventions
```

## Why Silver does not create dimensional SKs

Silver is not the star-schema layer.

Silver preserves business truth and conformed history using business keys and SCD2 columns.

Creating dimensional surrogate keys in Silver and then ignoring them in Gold would add confusion without improving the model.

## Why unknown rows are mandatory in Gold

Unknown rows prevent fact loss when source relationships are incomplete.

A missing dimension match should not delete or block a valid billing/usage fact.

---

# Current scope

This data model is designed for portfolio-scale billing analytics.

It demonstrates:

```text
multi-source dimension construction
Gold-owned surrogate keys
fact/dimension modeling
unknown key handling
SCD2-aware dimension resolution
billing and usage analytics
BI-ready table structure
```

It intentionally does not model every possible Stripe object.

The goal is a clear and defensible analytical model, not a complete replica of Stripe or ERP source systems.

````

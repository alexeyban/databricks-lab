# Data Vault 2.0 Model — dvdrental

Generated: 2026-04-06 09:57 UTC
Source: 20260406_095618

## Summary

- Hubs: 13
- Links: 19
- Satellites: 28
- PIT tables: 4
- Bridge tables: 2

## Hubs

| Hub | Business Key | Source Table | Record Source |
|-----|-------------|--------------|---------------|
| HUB_ACTOR | actor_id | silver.silver_actor | cdc.dvdrental.actor |
| HUB_ADDRESS | address_id | silver.silver_address | cdc.dvdrental.address |
| HUB_CATEGORY | category_id | silver.silver_category | cdc.dvdrental.category |
| HUB_CITY | city_id | silver.silver_city | cdc.dvdrental.city |
| HUB_COUNTRY | country_id | silver.silver_country | cdc.dvdrental.country |
| HUB_CUSTOMER | customer_id | silver.silver_customer | cdc.dvdrental.customer |
| HUB_FILM | film_id | silver.silver_film | cdc.dvdrental.film |
| HUB_INVENTORY | inventory_id | silver.silver_inventory | cdc.dvdrental.inventory |
| HUB_LANGUAGE | language_id | silver.silver_language | cdc.dvdrental.language |
| HUB_PAYMENT | payment_id | silver.silver_payment | cdc.dvdrental.payment |
| HUB_RENTAL | rental_id | silver.silver_rental | cdc.dvdrental.rental |
| HUB_STAFF | staff_id | silver.silver_staff | cdc.dvdrental.staff |
| HUB_STORE | store_id | silver.silver_store | cdc.dvdrental.store |

## Links

| Link | Hubs Connected | Source Table | Confidence |
|------|---------------|--------------|-----------|
| LNK_ADDRESS_CITY | HUB_ADDRESS, HUB_CITY | silver.silver_address | HIGH |
| LNK_CITY_COUNTRY | HUB_CITY, HUB_COUNTRY | silver.silver_city | HIGH |
| LNK_CUSTOMER_STORE ⚠️ | HUB_CUSTOMER, HUB_STORE | silver.silver_customer | LOW |
| LNK_CUSTOMER_ADDRESS ⚠️ | HUB_CUSTOMER, HUB_ADDRESS | silver.silver_customer | LOW |
| LNK_FILM_LANGUAGE ⚠️ | HUB_FILM, HUB_LANGUAGE | silver.silver_film | LOW |
| LNK_FILM_ACTOR | HUB_FILM, HUB_ACTOR | silver.silver_film_actor | HIGH |
| LNK_FILM_CATEGORY | HUB_FILM, HUB_CATEGORY | silver.silver_film_category | HIGH |
| LNK_INVENTORY_FILM | HUB_INVENTORY, HUB_FILM | silver.silver_inventory | HIGH |
| LNK_INVENTORY_STORE | HUB_INVENTORY, HUB_STORE | silver.silver_inventory | HIGH |
| LNK_PAYMENT_CUSTOMER | HUB_PAYMENT, HUB_CUSTOMER | silver.silver_payment | HIGH |
| LNK_PAYMENT_STAFF ⚠️ | HUB_PAYMENT, HUB_STAFF | silver.silver_payment | LOW |
| LNK_PAYMENT_RENTAL | HUB_PAYMENT, HUB_RENTAL | silver.silver_payment | HIGH |
| LNK_RENTAL_INVENTORY | HUB_RENTAL, HUB_INVENTORY | silver.silver_rental | HIGH |
| LNK_RENTAL_CUSTOMER | HUB_RENTAL, HUB_CUSTOMER | silver.silver_rental | HIGH |
| LNK_RENTAL_STAFF | HUB_RENTAL, HUB_STAFF | silver.silver_rental | HIGH |
| LNK_STAFF_ADDRESS | HUB_STAFF, HUB_ADDRESS | silver.silver_staff | HIGH |
| LNK_STAFF_STORE ⚠️ | HUB_STAFF, HUB_STORE | silver.silver_staff | LOW |
| LNK_STORE_STAFF | HUB_STORE, HUB_STAFF | silver.silver_store | HIGH |
| LNK_STORE_ADDRESS | HUB_STORE, HUB_ADDRESS | silver.silver_store | HIGH |

## Satellites

| Satellite | Parent Hub | Tracked Columns | Split Reason |
|-----------|-----------|-----------------|-------------|
| SAT_ACTOR_CORE ⚠️ | HUB_ACTOR | first_name, last_name | S1: all payload columns |
| SAT_ADDRESS_CORE ⚠️ | HUB_ADDRESS | address, address2, district, postal_code, phone | S1: all payload columns |
| SAT_CATEGORY_CORE ⚠️ | HUB_CATEGORY | name | S1: all payload columns |
| SAT_CITY_CORE ⚠️ | HUB_CITY | city | S1: all payload columns |
| SAT_COUNTRY_CORE ⚠️ | HUB_COUNTRY | country | S1: all payload columns |
| SAT_CUSTOMER_CORE ⚠️ | HUB_CUSTOMER | first_name, last_name, email, activebool, active | S1: all payload columns |
| SAT_FILM_CORE ⚠️ | HUB_FILM | title, description, release_year, length, rating, special_features, fulltext | S2/S3: split by core pattern |
| SAT_FILM_PRICING ⚠️ | HUB_FILM | rental_duration, rental_rate, replacement_cost | S2/S3: split by pricing pattern |
| SAT_INVENTORY ⚠️ | HUB_INVENTORY | last_update | S4: marker satellite (audit columns only) |
| SAT_LANGUAGE_CORE ⚠️ | HUB_LANGUAGE | name | S1: all payload columns |
| SAT_PAYMENT_CORE ⚠️ | HUB_PAYMENT | payment_date | S2/S3: split by core pattern |
| SAT_PAYMENT_PRICING ⚠️ | HUB_PAYMENT | amount | S2/S3: split by pricing pattern |
| SAT_RENTAL_CORE ⚠️ | HUB_RENTAL | rental_date, return_date | S1: all payload columns |
| SAT_STAFF_CORE ⚠️ | HUB_STAFF | first_name, last_name, email, username, active | S1: all payload columns |
| SAT_STORE ⚠️ | HUB_STORE | last_update | S4: marker satellite (audit columns only) |
| SAT_ACTOR_DETAILS ⚠️ | HUB_ACTOR | first_name, last_name | These columns are not part of the business key and can change over time. |
| SAT_ADDRESS_DETAILS ⚠️ | HUB_ADDRESS | address, address2, district, postal_code, phone | These columns are not part of the business key and can change over time. |
| SAT_CATEGORY_DETAILS ⚠️ | HUB_CATEGORY | name | This column is not part of the business key and can change over time. |
| SAT_CITY_DETAILS ⚠️ | HUB_CITY | city | This column is not part of the business key and can change over time. |
| SAT_COUNTRY_DETAILS ⚠️ | HUB_COUNTRY | country | This column is not part of the business key and can change over time. |
| SAT_CUSTOMER_DETAILS ⚠️ | HUB_CUSTOMER | first_name, last_name, email, activebool, active | These columns are not part of the business key and can change over time. |
| SAT_FILM_DETAILS ⚠️ | HUB_FILM | title, description, release_year, length, rating, special_features, fulltext | These columns are not part of the business key and can change over time. |
| SAT_INVENTORY_DETAILS ⚠️ | HUB_INVENTORY |  | There are no columns to track for inventory. |
| SAT_LANGUAGE_DETAILS ⚠️ | HUB_LANGUAGE | name | This column is not part of the business key and can change over time. |
| SAT_PAYMENT_DETAILS ⚠️ | HUB_PAYMENT | amount, payment_date | These columns are not part of the business key and can change over time. |
| SAT_RENTAL_DETAILS ⚠️ | HUB_RENTAL | rental_date, return_date | These columns are not part of the business key and can change over time. |
| SAT_STAFF_DETAILS ⚠️ | HUB_STAFF | first_name, last_name, email, active, username | These columns are not part of the business key and can change over time. |
| SAT_STORE_DETAILS ⚠️ | HUB_STORE |  | There are no columns to track for store. |

## PIT Tables

| PIT Table | Hub | Satellites | Snapshot Grain |
|-----------|-----|-----------|---------------|
| PIT_FILM | HUB_FILM | SAT_FILM_CORE, SAT_FILM_PRICING | daily |
| PIT_RENTAL | HUB_RENTAL | SAT_RENTAL_CORE | daily |
| PIT_CUSTOMER | HUB_CUSTOMER | SAT_CUSTOMER_CORE | daily |
| PIT_PAYMENT | HUB_PAYMENT | SAT_PAYMENT_CORE, SAT_PAYMENT_PRICING | daily |

## Bridge Tables

| Bridge Table | Path |
|-------------|------|
| BRG_RENTAL_FILM | HUB_RENTAL → LNK_RENTAL_INVENTORY → HUB_INVENTORY → LNK_INVENTORY_FILM → HUB_FILM |
| BRG_FILM_CAST | HUB_FILM → LNK_FILM_ACTOR → HUB_ACTOR |

## Entity Relationship Notes

The following items were flagged as LOW confidence and require human review:

| Step | Entity | Rule | Reason |
|------|--------|------|--------|
| step2b_ai_classifier | LNK_CUSTOMER_STORE | MERGE:heuristic_only — AI did not classify this as a Link | Review: check if the FK relationship is correctly modelled |
| step2b_ai_classifier | LNK_CUSTOMER_ADDRESS | MERGE:heuristic_only — AI did not classify this as a Link | Review: check if the FK relationship is correctly modelled |
| step2b_ai_classifier | LNK_FILM_LANGUAGE | MERGE:heuristic_only — AI did not classify this as a Link | Review: check if the FK relationship is correctly modelled |
| step2b_ai_classifier | LNK_PAYMENT_STAFF | MERGE:heuristic_only — AI did not classify this as a Link | Review: check if the FK relationship is correctly modelled |
| step2b_ai_classifier | LNK_STAFF_STORE | MERGE:heuristic_only — AI did not classify this as a Link | Review: check if the FK relationship is correctly modelled |
| step2b_ai_classifier | SAT_ACTOR_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_ADDRESS_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_CATEGORY_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_CITY_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_COUNTRY_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_CUSTOMER_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_FILM_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_FILM_PRICING | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_INVENTORY | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_LANGUAGE_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_PAYMENT_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_PAYMENT_PRICING | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_RENTAL_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_STAFF_CORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_STORE | MERGE:heuristic_only — AI split satellites differently | Review: AI may have grouped these columns differently |
| step2b_ai_classifier | SAT_ACTOR_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_ADDRESS_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_CATEGORY_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_CITY_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_COUNTRY_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_CUSTOMER_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_FILM_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_INVENTORY_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_LANGUAGE_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_PAYMENT_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_RENTAL_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_STAFF_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |
| step2b_ai_classifier | SAT_STORE_DETAILS | MERGE:ai_only — AI created a satellite not in heuristic model | Review: confirm this satellite split is intentional |

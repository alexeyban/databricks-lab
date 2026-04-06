# Data Vault 2.0 Model — dvdrental

Generated: 2026-04-05 19:22 UTC
Source: 20260405_192255

## Summary

- Hubs: 13
- Links: 19
- Satellites: 15
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
| LNK_CUSTOMER_STORE | HUB_CUSTOMER, HUB_STORE | silver.silver_customer | HIGH |
| LNK_CUSTOMER_ADDRESS | HUB_CUSTOMER, HUB_ADDRESS | silver.silver_customer | HIGH |
| LNK_FILM_LANGUAGE | HUB_FILM, HUB_LANGUAGE | silver.silver_film | HIGH |
| LNK_FILM_ACTOR | HUB_FILM, HUB_ACTOR | silver.silver_film_actor | HIGH |
| LNK_FILM_CATEGORY | HUB_FILM, HUB_CATEGORY | silver.silver_film_category | HIGH |
| LNK_INVENTORY_FILM | HUB_INVENTORY, HUB_FILM | silver.silver_inventory | HIGH |
| LNK_INVENTORY_STORE | HUB_INVENTORY, HUB_STORE | silver.silver_inventory | HIGH |
| LNK_PAYMENT_CUSTOMER | HUB_PAYMENT, HUB_CUSTOMER | silver.silver_payment | HIGH |
| LNK_PAYMENT_STAFF | HUB_PAYMENT, HUB_STAFF | silver.silver_payment | HIGH |
| LNK_PAYMENT_RENTAL | HUB_PAYMENT, HUB_RENTAL | silver.silver_payment | HIGH |
| LNK_RENTAL_INVENTORY | HUB_RENTAL, HUB_INVENTORY | silver.silver_rental | HIGH |
| LNK_RENTAL_CUSTOMER | HUB_RENTAL, HUB_CUSTOMER | silver.silver_rental | HIGH |
| LNK_RENTAL_STAFF | HUB_RENTAL, HUB_STAFF | silver.silver_rental | HIGH |
| LNK_STAFF_ADDRESS | HUB_STAFF, HUB_ADDRESS | silver.silver_staff | HIGH |
| LNK_STAFF_STORE | HUB_STAFF, HUB_STORE | silver.silver_staff | HIGH |
| LNK_STORE_STAFF | HUB_STORE, HUB_STAFF | silver.silver_store | HIGH |
| LNK_STORE_ADDRESS | HUB_STORE, HUB_ADDRESS | silver.silver_store | HIGH |

## Satellites

| Satellite | Parent Hub | Tracked Columns | Split Reason |
|-----------|-----------|-----------------|-------------|
| SAT_ACTOR_CORE | HUB_ACTOR | first_name, last_name | S1: all payload columns |
| SAT_ADDRESS_CORE | HUB_ADDRESS | address, address2, district, postal_code, phone | S1: all payload columns |
| SAT_CATEGORY_CORE | HUB_CATEGORY | name | S1: all payload columns |
| SAT_CITY_CORE | HUB_CITY | city | S1: all payload columns |
| SAT_COUNTRY_CORE | HUB_COUNTRY | country | S1: all payload columns |
| SAT_CUSTOMER_CORE | HUB_CUSTOMER | first_name, last_name, email, activebool, active | S1: all payload columns |
| SAT_FILM_CORE | HUB_FILM | title, description, release_year, length, rating, special_features, fulltext | S2/S3: split by core pattern |
| SAT_FILM_PRICING | HUB_FILM | rental_duration, rental_rate, replacement_cost | S2/S3: split by pricing pattern |
| SAT_INVENTORY | HUB_INVENTORY | last_update | S4: marker satellite (audit columns only) |
| SAT_LANGUAGE_CORE | HUB_LANGUAGE | name | S1: all payload columns |
| SAT_PAYMENT_CORE | HUB_PAYMENT | payment_date | S2/S3: split by core pattern |
| SAT_PAYMENT_PRICING | HUB_PAYMENT | amount | S2/S3: split by pricing pattern |
| SAT_RENTAL_CORE | HUB_RENTAL | rental_date, return_date | S1: all payload columns |
| SAT_STAFF_CORE | HUB_STAFF | first_name, last_name, email, username, active | S1: all payload columns |
| SAT_STORE | HUB_STORE | last_update | S4: marker satellite (audit columns only) |

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

No LOW-confidence items flagged. All classifications are HIGH confidence.

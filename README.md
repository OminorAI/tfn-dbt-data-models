# TFN DBT Project

This DBT project manages data transformations for TFN (The Fuel Network) operations, focusing on supplier management and operational dashboards.

## Project Overview

This project transforms raw operational and supplier data into analytical models for business intelligence and reporting. The main focus is on creating views and tables that support supplier management dashboards and operational analytics.

### Key Models

- **tfn_supplier_manager**: A comprehensive view combining transaction data with depot information to provide insights into supplier operations, discounts, and regional performance.

### Data Sources

- **tfn_ops_dashboards.precalculated_transaction_data_partitioned**: Pre-calculated transaction data including customer information, fuel amounts, pricing, and discounts
- **tfn_monday.depot_info**: Depot metadata including supplier managers, operating licenses, GPS coordinates, and operational details

### Project Structure

Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test

Use the following commands to test dbt connections and models:

Makes sure dbt recognizes existing models
- dbt parse 

Use a selector to specify the exact model:
- dbt parse --select vw_nto_buyer_order

To test connection use:
- dbt debug

To check if all sources (existing big query tables referenced) exist run:
- dbt source freshness

You can also run tests on exact sources:
- dbt test --select source:*

## Other important commands

Use xdg-open /home/steven-sutton/.dbt
to access the profiles.yml
Within profiles.yml you will need to specify/update the destination dataset.
Replace steven-sutton with appropriate name.

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

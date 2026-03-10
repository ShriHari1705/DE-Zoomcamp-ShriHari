/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: create+replace

columns:
  - name: pickup_date
    type: DATE
    description: Date of pickup
    primary_key: true
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    primary_key: true
  - name: payment_description
    type: VARCHAR
    description: Payment method description
    primary_key: true
  - name: total_trips
    type: BIGINT
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_revenue
    type: DOUBLE
    description: Sum of total_amount
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: DOUBLE
    description: Average trip distance in miles

@bruin */

SELECT
    CAST(pickup_datetime AS DATE)   AS pickup_date,
    taxi_type,
    COALESCE(payment_description, 'Unknown') AS payment_description,
    COUNT(*)                        AS total_trips,
    SUM(total_amount)               AS total_revenue,
    AVG(trip_distance)              AS avg_trip_distance
FROM staging.trips
GROUP BY 1, 2, 3

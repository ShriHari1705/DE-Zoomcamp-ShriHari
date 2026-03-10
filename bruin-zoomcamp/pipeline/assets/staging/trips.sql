/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: create+replace

columns:
  - name: trip_id
    type: VARCHAR
    description: Surrogate key from taxi_type + vendor_id + pickup_datetime
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: total_amount
    type: DOUBLE
    description: Total amount charged
    checks:
      - name: non_negative

custom_checks:
  - name: no_future_pickups
    description: No pickup datetime should be in the future
    query: |
      SELECT COUNT(*) FROM staging.trips
      WHERE pickup_datetime > NOW()
    value: 0
    blocking: false

@bruin */

SELECT
    md5(t.taxi_type || COALESCE(CAST(t.vendor_id AS VARCHAR), '') || CAST(t.pickup_datetime AS VARCHAR)) AS trip_id,
    t.taxi_type,
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.total_amount,
    t.payment_type,
    p.payment_type_name AS payment_description,
    t.pu_location_id,
    t.do_location_id,
    t.extracted_at
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p ON t.payment_type = p.payment_type_id
WHERE t.total_amount >= 0
  AND t.trip_distance > 0
  AND t.pickup_datetime IS NOT NULL

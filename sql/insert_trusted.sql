-- Garante a recriação da tabela para idempotência
DROP TABLE IF EXISTS trusted.taxi_trips;

CREATE TABLE trusted.taxi_trips AS
SELECT 
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC(10,2)) AS trip_distance,
    CAST(total_amount AS NUMERIC(10,2)) AS total_amount
FROM raw.taxi_trips
WHERE 
    trip_distance > 0 -- Regra de qualidade: remove distâncias zeradas ou negativas
    AND passenger_count > 0;
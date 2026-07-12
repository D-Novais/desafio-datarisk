-- Garante a recriação da tabela para idempotência
DROP TABLE IF EXISTS refined.taxi_trips;

CREATE TABLE refined.taxi_trips AS
SELECT 
    DATE(pickup_datetime) AS trip_date,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    -- Calcula a duração da viagem em minutos (diferencial bacana)
    EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60 AS trip_duration_minutes
FROM trusted.taxi_trips;
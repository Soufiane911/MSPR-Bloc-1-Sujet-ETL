SELECT
    s.schedule_id::text AS schedule_id,
    s.schedule_id::text AS route_id,
    'postgres' AS source_name,
    so.name AS origin,
    sd.name AS destination,
    COALESCE(so.country, 'unknown') AS origin_country,
    COALESCE(sd.country, 'unknown') AS destination_country,
    s.distance_km,
    s.duration_min,
    t.train_type,
    CASE
        WHEN s.frequency ~ '^[0-9]+(\.[0-9]+)?$' THEN s.frequency::numeric
        ELSE 0
    END AS weekly_frequency
FROM schedules s
JOIN trains t ON s.train_id = t.train_id
JOIN stations so ON s.origin_id = so.station_id
JOIN stations sd ON s.destination_id = sd.station_id
WHERE s.distance_km IS NOT NULL
  AND s.duration_min IS NOT NULL;

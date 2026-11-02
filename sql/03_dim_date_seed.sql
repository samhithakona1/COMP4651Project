-- Origin: original
-- Calendar 2011-01-01 .. 2027-12-31. The Spark job writes the same rows (build_dims.dim_date) and the
-- loader upserts them; this seed only makes sure the dimension exists before the first load.
INSERT INTO dim_date (date_id, full_date, year, quarter, month, day, day_of_week, is_weekend)
SELECT to_char(d, 'YYYYMMDD')::int,
       d::date,
       EXTRACT(YEAR FROM d)::smallint,
       EXTRACT(QUARTER FROM d)::smallint,
       EXTRACT(MONTH FROM d)::smallint,
       EXTRACT(DAY FROM d)::smallint,
       EXTRACT(ISODOW FROM d)::smallint,
       EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2011-01-01'::date, '2027-12-31'::date, interval '1 day') AS g(d)
ON CONFLICT (date_id) DO NOTHING;

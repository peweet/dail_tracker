-- v_sitting_days_by_year
-- Official plenary sitting-day counts from Houses of the Oireachtas Commission
-- annual reports. These are fixed historical facts; source: oireachtas.ie annual reports.
CREATE OR REPLACE VIEW v_sitting_days_by_year AS
SELECT * FROM (VALUES
    (2020, 82),
    (2021, 94),
    (2022, 106),
    (2023, 100),
    (2024, 83),
    (2025, 82)
) t(year, total_sitting_days);

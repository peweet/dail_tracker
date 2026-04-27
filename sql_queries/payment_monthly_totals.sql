SELECT
    DATE_TRUNC('month', Date_Paid) AS month,
    STRFTIME(DATE_TRUNC('month', Date_Paid), '%b %Y') AS month_label,
    SUM(Amount) AS total_paid,
    COUNT(DISTINCT Full_Name) AS tds_paid
FROM aggregated_payment_tables
WHERE Amount IS NOT NULL
GROUP BY month, month_label
ORDER BY month

SELECT
    full_name,
    party,
    MAX(interest_count) AS max_interest_count,
    COUNT(DISTINCT year_declared) AS years_declared,
    MAX(year_declared) AS latest_year,
    BOOL_OR(is_landlord) AS ever_landlord,
    BOOL_OR(is_property_owner) AS ever_property_owner
FROM dail_member_interests_combined
GROUP BY full_name, party
ORDER BY max_interest_count DESC

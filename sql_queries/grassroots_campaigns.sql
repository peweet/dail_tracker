-- Replaces: compute_grassroots_campaigns
-- Grain: one row per lobbyist that filed at least one grassroots return
-- Source: returns (one row per lobby return)

SELECT
    lobbyist_name,
    COUNT(*) AS grassroots_returns_count
FROM   returns
WHERE  was_this_a_grassroots_campaign = 'Yes'
GROUP  BY lobbyist_name
ORDER  BY grassroots_returns_count DESC

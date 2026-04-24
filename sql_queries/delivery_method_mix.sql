-- Replaces: experimental_compute_delivery_method_mix
-- Grain: one row per (lobbyist, delivery channel)
-- Source: activities (one row per return x politician)

SELECT
    lobbyist_name,
    delivery,
    COUNT(*) AS delivery_count
FROM   activities
GROUP  BY lobbyist_name, delivery
ORDER  BY lobbyist_name ASC, delivery_count DESC

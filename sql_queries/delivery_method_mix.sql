
SELECT
    lobbyist_name,
    delivery,
    COUNT(*) AS delivery_count
FROM   activities
GROUP  BY lobbyist_name, delivery
ORDER  BY lobbyist_name ASC, delivery_count DESC

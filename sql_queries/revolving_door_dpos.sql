-- Replaces: experimental_compute_revolving_door_dpos
-- Grain: one row per (ex-DPO name, prior position, prior chamber)
-- Signal: former public officials now personally carrying out lobbying activity.
-- Source: activities (one row per return x politician)

SELECT
    dpos_or_former_dpos_who_carried_out_lobbying_name,
    current_or_former_dpos_position,
    current_or_former_dpos_chamber,
    COUNT(DISTINCT primary_key)        AS returns_involved_in,
    COUNT(DISTINCT lobbyist_name)      AS distinct_lobbyist_firms,
    COUNT(DISTINCT public_policy_area) AS distinct_policy_areas,
    COUNT(DISTINCT full_name)          AS distinct_politicians_targeted
FROM   activities
WHERE  dpos_or_former_dpos_who_carried_out_lobbying_name IS NOT NULL
  AND  dpos_or_former_dpos_who_carried_out_lobbying_name <> ''
GROUP  BY dpos_or_former_dpos_who_carried_out_lobbying_name,
          current_or_former_dpos_position,
          current_or_former_dpos_chamber
ORDER  BY returns_involved_in DESC

-- v_constituency_waiting_composition — the "who's waiting here" breakdown for each
-- constituency, per serving local authority. Bridges the national composition view
-- (v_ssha_waiting_list_composition, grain='la') to constituencies via the same
-- explicit LA→crosswalk map used by v_constituency_ssha_waiting_list. One row per
-- (constituency, serving council, dimension, category). Council-area grain.
--
-- Depends on v_ssha_waiting_list_composition (registered first in constituency_conn)
-- and v_constituency_la_crosswalk.
CREATE OR REPLACE VIEW v_constituency_waiting_composition AS
WITH la_map(local_authority, ssha_la) AS (
    VALUES
    ('Carlow', 'Carlow County'),
    ('Cavan', 'Cavan County'),
    ('Clare', 'Clare County'),
    ('Cork City', 'Cork City'),
    ('Cork County', 'Cork County'),
    ('Donegal', 'Donegal County'),
    ('Dublin City', 'Dublin City'),
    ('Dun Laoghaire-Rathdown', 'Dun Laoghaire Rathdown County'),
    ('Fingal', 'Fingal County'),
    ('Galway City', 'Galway City'),
    ('Galway County', 'Galway County'),
    ('Kerry', 'Kerry County'),
    ('Kildare', 'Kildare County'),
    ('Kilkenny', 'Kilkenny County'),
    ('Laois', 'Laois County'),
    ('Leitrim', 'Leitrim County'),
    ('Limerick', 'Limerick City and County'),
    ('Longford', 'Longford County'),
    ('Louth', 'Louth County'),
    ('Mayo', 'Mayo County'),
    ('Meath', 'Meath County'),
    ('Monaghan', 'Monaghan County'),
    ('Offaly', 'Offaly County'),
    ('Roscommon', 'Roscommon County'),
    ('Sligo', 'Sligo County'),
    ('South Dublin', 'South Dublin County'),
    ('Tipperary', 'Tipperary County'),
    ('Waterford', 'Waterford City and County'),
    ('Westmeath', 'Westmeath County'),
    ('Wexford', 'Wexford County'),
    ('Wicklow', 'Wicklow County')
)
SELECT
    x.constituency_name,
    x.local_authority,
    x.link_type,
    comp.dimension,
    comp.category,
    comp.ord,
    comp.count,
    comp.pct
FROM v_constituency_la_crosswalk x
JOIN la_map m ON m.local_authority = x.local_authority
JOIN v_ssha_waiting_list_composition comp
    ON comp.grain = 'la' AND comp.area = m.ssha_la AND comp.year = 2025
ORDER BY x.constituency_name, (x.link_type = 'primary') DESC, x.local_authority, comp.dimension, comp.ord;

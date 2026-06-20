-- v_procurement_supplier_groups — curated CORPORATE-GROUP map: which published payment
-- entities belong to the same parent group, so the Follow-the-money trail can offer a single
-- "BAM" node that rolls up its many legal entities (operating companies, PPP special-purpose
-- vehicles, joint ventures) instead of a dozen separate supplier strings.
--
-- WHY CURATED (not auto): a name-prefix match on "bam" wrongly sweeps in BAMFORD BUS
-- (Wrightbus) and BAMOS SERVICIOS DE MOVILIDAD — unrelated firms — and the PPP SPVs
-- (Schools/Courts Bundles) carry NO CRO number to join on. So group membership is a hand-
-- curated editorial decision, one row per published supplier_normalised.
--
-- Source: data/_meta/supplier_groups.csv (manually curated). Joined on supplier_normalised
-- (uppercased/trimmed to match the payments feed's already-uppercase key). entity_kind is one
-- of operating / ppp_spv / jv, so the UI can disclose the group's structure honestly.
--
-- ⚠️ A group rollup SUMS sum-safe euros across the member entities, which may mix VAT bases
-- across the bodies that paid them — an indicative FLOOR, never an audited total, presented
-- the same way the per-supplier summary already flags vat_mixed. zz_ prefix so the glob loads
-- it AFTER the payment feed/summary views it is joined against (see procurement_data glob).

CREATE OR REPLACE VIEW v_procurement_supplier_groups AS
SELECT
    upper(trim(supplier_normalised))      AS supplier_normalised,
    group_slug,
    group_label,
    NULLIF(trim(CAST(cro_company_num AS VARCHAR)), '') AS cro_company_num,
    entity_kind,
    note
FROM read_csv_auto('data/_meta/supplier_groups.csv', header=true, all_varchar=true);

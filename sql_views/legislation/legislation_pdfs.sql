-- v_legislation_pdfs — every official Oireachtas PDF associated with a bill
-- Sources:
--   data/silver/parquet/versions.parquet         (bill text + enacted Acts)
--   data/silver/parquet/related_docs.parquet     (memos, digests, gluais, errata)
--   data/silver/parquet/bill_amendments.parquet  (numberedList / creamList amendments)
--
-- One row per PDF. Multiple rows per bill_id. Sorted within each bill by
-- category (versions first, then related_docs, then amendments) and by
-- pdf_date desc so the most recent text appears at the top of each group.

CREATE OR REPLACE VIEW v_legislation_pdfs AS
WITH versions AS (
    SELECT
        "bill.billYear" || '_' || "bill.billNo"     AS bill_id,
        'version'                                   AS pdf_category,
        COALESCE("version.showAs", '—')             AS pdf_label,
        "version.formats.pdf.uri"                   AS pdf_url,
        TRY_CAST("version.date" AS DATE)            AS pdf_date,
        COALESCE("version.lang", 'eng')             AS pdf_lang,
        "version.docType"                           AS pdf_subtype,   -- "bill" | "act"
        1                                           AS category_order
    FROM read_parquet('data/silver/parquet/versions.parquet')
    WHERE "version.formats.pdf.uri" IS NOT NULL
),
related_docs AS (
    SELECT
        "bill.billYear" || '_' || "bill.billNo"     AS bill_id,
        'related_doc'                               AS pdf_category,
        COALESCE("relatedDoc.showAs", '—')          AS pdf_label,
        "relatedDoc.formats.pdf.uri"                AS pdf_url,
        TRY_CAST("relatedDoc.date" AS DATE)         AS pdf_date,
        COALESCE("relatedDoc.lang", 'eng')          AS pdf_lang,
        "relatedDoc.docType"                        AS pdf_subtype,   -- memo | digest | gluais | errata
        2                                           AS category_order
    FROM read_parquet('data/silver/parquet/related_docs.parquet')
    WHERE "relatedDoc.formats.pdf.uri" IS NOT NULL
),
amendments AS (
    SELECT
        bill_id,
        'amendment'                                 AS pdf_category,
        COALESCE(show_as, '—')                      AS pdf_label,
        pdf_url,
        amendment_date                              AS pdf_date,
        'eng'                                       AS pdf_lang,
        amendment_type                              AS pdf_subtype,   -- numberedList | creamList
        3                                           AS category_order
    FROM read_parquet('data/silver/parquet/bill_amendments.parquet')
    WHERE pdf_url IS NOT NULL
)
SELECT
    bill_id,
    pdf_category,
    pdf_subtype,
    pdf_label,
    pdf_url,
    pdf_date,
    pdf_lang,
    category_order
FROM versions
UNION ALL
SELECT bill_id, pdf_category, pdf_subtype, pdf_label, pdf_url, pdf_date, pdf_lang, category_order
FROM related_docs
UNION ALL
SELECT bill_id, pdf_category, pdf_subtype, pdf_label, pdf_url, pdf_date, pdf_lang, category_order
FROM amendments
ORDER BY bill_id, category_order, pdf_date DESC NULLS LAST, pdf_label;

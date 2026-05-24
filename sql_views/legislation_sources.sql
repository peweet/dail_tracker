-- v_legislation_sources — official source URLs for each bill (one row per bill)
-- Sources:
--   data/silver/parquet/sponsors.parquet     (bill_url — oireachtas.ie HTML page)
--   data/silver/parquet/versions.parquet     (official_pdf_url — latest bill text PDF)
--   data/silver/parquet/related_docs.parquet (source_document_url — English memo PDF)
--
-- For multi-PDF detail (every version, related doc, and amendment list),
-- use v_legislation_pdfs instead.
--
-- TODO_PIPELINE_VIEW_REQUIRED: legislation_url (Statute Book URI for enacted bills)

CREATE OR REPLACE VIEW v_legislation_sources AS
WITH base AS (
    SELECT DISTINCT
        bill_year || '_' || bill_no     AS bill_id,
        bill_url                        AS oireachtas_url,
        bill_no,
        bill_year
    FROM read_parquet('data/silver/parquet/sponsors.parquet')
),
latest_version AS (
    SELECT
        "bill.billYear" || '_' || "bill.billNo"  AS bill_id,
        "version.formats.pdf.uri"                AS official_pdf_url,
        ROW_NUMBER() OVER (
            PARTITION BY "bill.billYear", "bill.billNo"
            ORDER BY TRY_CAST("version.date" AS DATE) DESC NULLS LAST
        ) AS rn
    FROM read_parquet('data/silver/parquet/versions.parquet')
    WHERE "version.formats.pdf.uri" IS NOT NULL
),
latest_memo AS (
    SELECT
        "bill.billYear" || '_' || "bill.billNo"  AS bill_id,
        "relatedDoc.formats.pdf.uri"             AS source_document_url,
        ROW_NUMBER() OVER (
            PARTITION BY "bill.billYear", "bill.billNo"
            ORDER BY TRY_CAST("relatedDoc.date" AS DATE) DESC NULLS LAST
        ) AS rn
    FROM read_parquet('data/silver/parquet/related_docs.parquet')
    WHERE "relatedDoc.docType" = 'memo'
      AND COALESCE("relatedDoc.lang", 'eng') = 'eng'
      AND "relatedDoc.formats.pdf.uri" IS NOT NULL
)
SELECT
    base.bill_id,
    base.oireachtas_url,
    NULL::VARCHAR                       AS legislation_url,
    lv.official_pdf_url,
    NULL::VARCHAR                       AS source_url,
    lm.source_document_url,
    base.bill_no,
    base.bill_year
FROM base
LEFT JOIN latest_version lv ON lv.bill_id = base.bill_id AND lv.rn = 1
LEFT JOIN latest_memo    lm ON lm.bill_id = base.bill_id AND lm.rn = 1;

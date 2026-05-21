CREATE OR REFRESH STREAMING TABLE supply_chain_live.bronze.metadata
  COMMENT "Raw access metadata - bronze layer" AS
SELECT
  *
FROM
  STREAM read_files(
    "/Volumes/supply_chain_live/default/raw/metadata",
    format => "csv",
    header => "true",
    inferSchema => "true"
  )
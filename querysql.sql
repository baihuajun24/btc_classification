WITH 
output_ages AS (
  SELECT
    ARRAY_TO_STRING(outputs.addresses,',') AS output_ages_address,
    MIN(block_timestamp_month) AS earliest_inbound_month,
    MAX(block_timestamp_month) AS latest_inbound_month,
    MIN(block_timestamp) AS earliest_inbound_sec,
    MAX(block_timestamp) AS latest_inbound_sec
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions JOIN UNNEST(outputs) AS outputs
  GROUP BY output_ages_address
)
,input_ages AS (
  SELECT
    ARRAY_TO_STRING(inputs.addresses,',') AS input_ages_address,
    MIN(block_timestamp_month) AS earliest_outbound_month,
    MAX(block_timestamp_month) AS latest_outbound_month,
    MIN(block_timestamp) AS earliest_outbound_sec,
    MAX(block_timestamp) AS latest_outbound_sec
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions JOIN UNNEST(inputs) AS inputs
  GROUP BY input_ages_address
)
,output_monthly_stats AS (
  SELECT
    ARRAY_TO_STRING(outputs.addresses,',') AS output_monthly_stats_address, 
    COUNT(DISTINCT block_timestamp_month) AS output_active_months,
    COUNT(outputs) AS in_degree,
    SUM(value) AS total_received,
    MAX(value) AS max_received,
    SUM(value)/COUNT(block_timestamp_month) AS monthly_received, 
    STDDEV(value) AS stddev_received,
    COUNT(DISTINCT(`hash`)) AS total_received_tx
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions JOIN UNNEST(outputs) AS outputs
  GROUP BY output_monthly_stats_address
)
,input_monthly_stats AS (
  SELECT
    ARRAY_TO_STRING(inputs.addresses,',') AS input_monthly_stats_address, 
    COUNT(DISTINCT block_timestamp_month) AS input_active_months,
    COUNT(inputs) AS out_degree,
    SUM(value) AS total_sent,
    MAX(value) AS max_sent,
    SUM(value)/COUNT(block_timestamp_month) AS monthly_sent,
    STDDEV(value) AS stddev_sent,
    COUNT(DISTINCT(`hash`)) AS total_sent_tx
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions JOIN UNNEST(inputs) AS inputs
  GROUP BY input_monthly_stats_address
)
,output_idle_times AS (
  SELECT
    address AS idle_time_address,
    AVG(idle_time) AS mean_output_idle_time,
    STDDEV(idle_time) AS stddev_output_idle_time
  FROM
  (
    SELECT 
      event.address,
      IF(prev_block_time IS NULL, NULL, UNIX_SECONDS(block_time) - UNIX_SECONDS(prev_block_time)) AS idle_time
    FROM (
      SELECT
        ARRAY_TO_STRING(outputs.addresses,',') AS address, 
        block_timestamp AS block_time,
        LAG(block_timestamp) OVER (PARTITION BY ARRAY_TO_STRING(outputs.addresses,',') ORDER BY block_timestamp) AS prev_block_time
      FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions JOIN UNNEST(outputs) AS outputs
    ) AS event
    WHERE block_time != prev_block_time
  )
  GROUP BY address
)
,input_idle_times AS (
  SELECT
    address AS idle_time_address,
    AVG(idle_time) AS mean_input_idle_time,
    STDDEV(idle_time) AS stddev_input_idle_time
  FROM
  (
    SELECT 
      event.address,
      IF(prev_block_time IS NULL, NULL, UNIX_SECONDS(block_time) - UNIX_SECONDS(prev_block_time)) AS idle_time
    FROM (
      SELECT
        ARRAY_TO_STRING(inputs.addresses,',') AS address, 
        block_timestamp AS block_time,
        LAG(block_timestamp) OVER (PARTITION BY ARRAY_TO_STRING(inputs.addresses,',') ORDER BY block_timestamp) AS prev_block_time
      FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions JOIN UNNEST(inputs) AS inputs
    ) AS event
    WHERE block_time != prev_block_time
  )
  GROUP BY address
)
(SELECT
  output_ages_address AS address,
  UNIX_SECONDS(CAST(output_ages.latest_inbound_month AS TIMESTAMP)) AS latest_inbound_month,
  UNIX_SECONDS(CAST(output_ages.earliest_inbound_month AS TIMESTAMP)) AS earliest_inbound_month,
  UNIX_SECONDS(CAST(output_ages.earliest_inbound_sec AS TIMESTAMP)) AS earliest_inbound_sec,
  UNIX_SECONDS(CAST(output_ages.latest_inbound_sec AS TIMESTAMP)) AS latest_inbound_sec,
  UNIX_SECONDS(CAST(input_ages.latest_outbound_month AS TIMESTAMP)) AS latest_outbound_month,
  UNIX_SECONDS(CAST(input_ages.earliest_outbound_month AS TIMESTAMP)) AS earliest_outbound_month,
  UNIX_SECONDS(CAST(input_ages.earliest_outbound_sec AS TIMESTAMP)) AS earliest_outbound_sec,
  UNIX_SECONDS(CAST(input_ages.latest_outbound_sec AS TIMESTAMP)) AS latest_outbound_sec,
  UNIX_SECONDS(output_ages.latest_inbound_sec) - UNIX_SECONDS(output_ages.earliest_inbound_sec)  AS inbound_active_time,
  UNIX_SECONDS(input_ages.latest_outbound_sec) - UNIX_SECONDS(input_ages.earliest_outbound_sec)  AS outbound_active_time,
  UNIX_SECONDS(output_ages.latest_inbound_sec) - UNIX_SECONDS(input_ages.latest_outbound_sec) AS latest_io_lag,
  UNIX_SECONDS(output_ages.earliest_inbound_sec) - UNIX_SECONDS(input_ages.earliest_outbound_sec) AS earliest_io_lag,
  output_monthly_stats.output_active_months,
  output_monthly_stats.in_degree,
  output_monthly_stats.total_received,
  output_monthly_stats.monthly_received,
  output_monthly_stats.max_received,
  output_monthly_stats.stddev_received,
  output_monthly_stats.total_received_tx,
  input_monthly_stats.input_active_months,
  input_monthly_stats.out_degree,
  input_monthly_stats.total_sent,
  input_monthly_stats.monthly_sent,
  input_monthly_stats.max_sent,
  input_monthly_stats.stddev_sent,
  input_monthly_stats.total_sent_tx,
  output_monthly_stats.total_received - input_monthly_stats.total_sent AS balance,
  output_idle_times.mean_output_idle_time,
  output_idle_times.stddev_output_idle_time,
  input_idle_times.mean_input_idle_time,
  input_idle_times.stddev_input_idle_time
FROM
  output_ages, output_monthly_stats, output_idle_times,
  input_ages,  input_monthly_stats, input_idle_times
WHERE
  output_ages.output_ages_address = output_monthly_stats.output_monthly_stats_address
  AND output_ages.output_ages_address = output_idle_times.idle_time_address
  AND output_ages.output_ages_address = input_monthly_stats.input_monthly_stats_address
  AND output_ages.output_ages_address = input_ages.input_ages_address
  AND output_ages.output_ages_address = input_idle_times.idle_time_address
)

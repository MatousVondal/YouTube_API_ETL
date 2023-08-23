-- Create a new table to store the merged data
CREATE OR REPLACE TABLE `my-data-project-392011.YouTube_api_trend_analysis.merged` AS
SELECT * FROM (
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_09082023073319`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_09082023120008`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_09082023180224`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_10082023064652`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_10082023120009`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_10082023184331`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_11082023060008`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_11082023120338`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_11082023180759`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_12082023061132`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_12082023123236`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_12082023191128`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_13082023072957`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_13082023120009`
  UNION ALL
  SELECT * FROM `my-data-project-392011.YouTube_api_trend_analysis.YouTube_stats_13082023180007`

) ORDER BY video_id, time_name DESC;


-- create teable only with records with most recent time_name
CREATE OR REPLACE TABLE `my-data-project-392011.YouTube_api_trend_analysis.temp_table` AS
SELECT video_id, MAX(time_name) AS max_timestamp
FROM `my-data-project-392011.YouTube_api_trend_analysis.merged`
GROUP BY video_id;


-- Delete older records from merged_data table
DELETE FROM `my-data-project-392011.YouTube_api_trend_analysis.merged`
WHERE (video_id, time_name) NOT IN (
SELECT (video_id, max_timestamp) FROM `my-data-project-392011.YouTube_api_trend_analysis.temp_table`
);





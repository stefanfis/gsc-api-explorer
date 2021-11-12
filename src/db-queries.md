# Interesting Database Queries


## Questions

Returns Top 50 queries containing "wie ", ordered by number of overall impressions:

	SELECT gsc_queries.gsc_query, SUM(clicks), SUM(impressions) AS sum_impressions, AVG(position) FROM gsc_queries, gsc_results WHERE gsc_queries.gsc_query LIKE '%wie %' AND gsc_queries.id = gsc_results.gsc_query GROUP BY gsc_results.gsc_query ORDER BY sum_impressions DESC LIMIT 50;



## Under Performers

Returns Top-3 under performers: Average ranking within Top 3, but less than 10% CTR:

	SELECT gsc_queries.gsc_query, SUM(clicks), SUM(impressions), AVG(position) AS avg_position, AVG(ctr) AS avg_ctr FROM gsc_queries, gsc_results WHERE gsc_queries.id = gsc_results.gsc_query GROUP BY gsc_results.gsc_query HAVING avg_position <= 3 AND avg_ctr <= 0.1 LIMIT 30;



## Monthly Results

	Selects monthly results for a query

	SELECT SUBSTR(gsc_date,0,8) AS month, SUM(clicks), SUM(impressions) FROM gsc_queries, gsc_results WHERE gsc_queries.id = gsc_results.gsc_query AND gsc_queries.gsc_query LIKE '%orvieto%' GROUP BY month ORDER BY month ASC;



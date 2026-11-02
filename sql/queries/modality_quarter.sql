-- Origin: original
-- Q3: postings and pay by work modality and quarter (materialized view). Params: from_year int.
SELECT year, quarter, work_modality::text AS work_modality, posting_count, salary_n,
       median_salary, p25_salary, p75_salary
FROM mv_modality_quarter_stats
WHERE year >= %(from_year)s
ORDER BY year, quarter, work_modality;

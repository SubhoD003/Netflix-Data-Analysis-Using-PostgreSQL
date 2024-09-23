SELECT * FROM NETFLIX ORDER BY release_year

______________________________________________________________________________________________________________________

-- GENRE POPULARITY ANALYSIS 

CREATE OR REPLACE VIEW Genre_Count_Cte AS (
		SELECT TRIM(UNNEST(STRING_TO_ARRAY(listed_in,','))) AS genre, COUNT(*) AS cnt FROM netflix
		GROUP BY 1 ORDER BY 2 DESC
)

	-- 1.Determine the market share of each genre on Netflix’s platform to better allocate content acquisition budgets across different genres? 	
	SELECT genre, ROUND(cnt/(SELECT SUM(CNT) FROM genre_count_cte ) *100, 2) || '%'  AS percent_of_total
	FROM genre_count_cte 
	
	-- 2.What is the dominant genre on Netflix that can help inform future content production and partnership decisions?
	SELECT * FROM Genre_Count_Cte LIMIT 1
	
	/* 3.How can we quantify the disparity in content availability between the most popular and least popular 
	genres, and what does this reveal about customer preferences? */
 	SELECT MAX(cnt) - MIN(cnt) FROM Genre_Count_Cte

	/* 4.How can we measure the variability in the number of titles per genre to ensure a balanced content portfolio 
	and address any gaps in genre offerings? */
	WITH genre_year_count AS(
		SELECT TRIM(UNNEST(STRING_TO_ARRAY(listed_in,','))) AS genre,
		EXTRACT(YEAR FROM date_added::DATE) AS year_added, COUNT(*) AS genre_count FROM netflix
		GROUP BY 1,2 ORDER BY 1,2
	),mean_stats AS(
		SELECT genre , ROUND(AVG(genre_count),2) AS mean_count
		FROM genre_year_count 
		WHERE year_added IS NOT NULL
		GROUP BY genre
	)
	SELECT gyc.genre,
	ROUND(
		SQRT(AVG(POWER(genre_count - mean_stats.mean_count, 2))),
		2
	
	) AS standard_deviation 
	FROM genre_year_count gyc, mean_stats
	GROUP BY 1 ORDER BY 2 DESC
	
	/* 5.What are the long-term trends in genre consumption, and how can Netflix use this data to predict and 
	respond to shifts in audience preferences? */ 
	WITH genre_year_count AS (
		SELECT 
			TRIM(UNNEST(STRING_TO_ARRAY(listed_in, ','))) AS genre,
			EXTRACT(YEAR FROM date_added::DATE) AS year_added, 
			COUNT(*) AS genre_count
		FROM netflix WHERE EXTRACT(YEAR FROM date_added::DATE)  IS NOT NULL
		GROUP BY genre, year_added
		ORDER BY genre, year_added
	), genre_slope_date_cte AS(
		SELECT 
		genre, 
		year_added,
		genre_count,
		REGR_SLOPE(genre_count, year_added) OVER (PARTITION BY genre) AS slope
		FROM genre_year_count
		GROUP BY 1,2,3
		ORDER BY genre, year_added
	)
	SELECT DISTINCT genre,ROUND(CAST(slope AS NUMERIC),2) AS slope FROM genre_slope_date_cte ORDER BY 2 DESC

______________________________________________________________________________________________________________________

-- TITLE RELEASE DISTRIBUTION OVER THE YEARS 

CREATE OR REPLACE VIEW yearly_release_content_type AS(
	SELECT n.release_year, n.type, COUNT(*) AS numbers_released FROM netflix n
	GROUP BY n.release_year,n.type ORDER BY n.release_year
)

	/* 6.How can we compare the consistency of Netflix's investment in movie versus TV show content over the years, 
	and what does this reveal about changing consumer demand? */
	WITH mean_stats AS(
			SELECT "type" , ROUND(AVG(numbers_released),2) AS mean_count
			FROM yearly_release_content_type 
			GROUP BY 1
	)
	SELECT yr."type", 
		ROUND(
			SQRT(AVG(POWER(numbers_released - mean_stats.mean_count, 2))),
			2

		) AS standard_deviation 
	FROM yearly_release_content_type yr,mean_stats GROUP BY 1 


	/* 7.How has the growth rate of movies and TV shows evolved over time, and what trends can 
	Netflix leverage to optimize future content releases?*/
	SELECT DISTINCT "type" , 
		ROUND(CAST(REGR_SLOPE(numbers_released, release_year)OVER(PARTITION BY "type") AS NUMERIC),2) AS timely_trend 
	FROM yearly_release_content_type
	
______________________________________________________________________________________________________________________

-- TREND ANALYSIS OF MOVIE ADDITIONS ON NETFLIX : 90s VS 2000s

/* 8. How do movie addition trends from the 1990s compare to the 2000s, and what can Netflix learn from these patterns 
to enhance content curation for different generational cohorts? */
WITH movie_released_era_cte AS(
	SELECT "type", 
		CASE 
			WHEN release_year < 2000 THEN '90s'
			ELSE '2000s'
		END AS Era_belongs_to,
		EXTRACT(YEAR FROM date_added::DATE) AS year_added 
	FROM netflix

), movie_released_era_count_cte AS(
	SELECT "type", era_belongs_to,year_added , COUNT(*) AS cnt FROM movie_released_era_cte 
	WHERE year_added IS NOT NULL
	GROUP BY 1,2,3 ORDER BY 1,2,3 DESC

)
SELECT 
    "type", 
    REGR_SLOPE(CASE WHEN era_belongs_to = '2000s' THEN cnt END, 
                CASE WHEN era_belongs_to = '2000s' THEN year_added END) AS "2000s",
    REGR_SLOPE(CASE WHEN era_belongs_to = '90s' THEN cnt END, 
                CASE WHEN era_belongs_to = '90s' THEN year_added END) AS "90s"
FROM 
    movie_released_era_count_cte 
GROUP BY 
    "type";

______________________________________________________________________________________________________________________

-- MOST WATCHED GENRE IN EACH COUNTRY - PAST 5 YEARS VS MOST CONSISTENT GENRE 

CREATE OR REPLACE VIEW most_watched_genre AS(
	
	WITH movie_and_gnere AS(
			SELECT TRIM(UNNEST(STRING_TO_ARRAY(country,','))) as country, "type", title, listed_in, release_year FROM netflix	
	)
	SELECT country, "type", title, TRIM(UNNEST(STRING_TO_ARRAY(listed_in,','))) AS genre , release_year FROM movie_and_gnere 
	WHERE country NOT LIKE ''
	ORDER BY 1,4
)

	/* 9.What are the top-performing genres in each country over the past 5 years, and how can Netflix 
	tailor its content strategy to maximize regional audience engagement? */
	WITH genre_ranking_cte AS(
		SELECT country, "type", genre, 
			DENSE_RANK()OVER(PARTITION BY country, "type" ORDER BY COUNT(genre) DESC) AS genre_rank
		FROM most_watched_genre
		WHERE release_year >=(SELECT MAX(release_year) - 5 FROM most_watched_genre) AND country NOT LIKE ''
		GROUP BY country, genre,"type" ORDER BY country, 3 DESC

	)
	SELECT 
		country,
		STRING_AGG(CASE WHEN "type" = 'Movie' THEN genre END, ', ') AS Movie,
		STRING_AGG(CASE WHEN "type" = 'TV Show' THEN genre END, ', ') AS TV_Show
	FROM genre_ranking_cte
	WHERE genre_rank = 1
	GROUP BY country;
	
	
	/* 10.Which genres have demonstrated the most consistent demand across different markets, and how can 
	Netflix capitalize on this consistency to maintain a loyal viewership? */
	WITH genre_years AS (
		SELECT 
			country, 
			"type", 
			genre,
			release_year,
			ROW_NUMBER() OVER (PARTITION BY country, genre ORDER BY release_year)
		FROM most_watched_genre
	),
	genre_stats AS (
		SELECT 
			country,
			genre,
			"type",
			MIN(release_year) AS first_year,
			MAX(release_year) AS last_year,
			COUNT(DISTINCT release_year) AS total_years,
			COUNT(release_year) AS total_releases,
			PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY release_year) AS Q1,
			PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY release_year) AS Q3
		FROM genre_years
		GROUP BY country, genre, "type"
	),
	final_stats AS (
		SELECT 
			country,
			genre,
			"type",
			(last_year - first_year) AS year_range,            
			total_years,                                        
			total_releases,                                    
			(Q3 - Q1) AS IQR,                                   
			((last_year - first_year) + (Q3 - Q1)) AS score
		FROM genre_stats
	),
	ranked_genres AS (
		SELECT 
			country,
			genre,
			"type",
			year_range,
			total_years,
			total_releases,
			IQR,
			score,
			DENSE_RANK() OVER (PARTITION BY country ORDER BY score DESC) AS genre_rank
		FROM final_stats
	)
	SELECT 
		country,
		STRING_AGG(CASE WHEN "type" = 'Movie' THEN genre END, ', ') AS Movie,
		STRING_AGG(CASE WHEN "type" = 'TV Show' THEN genre END, ', ') AS TV_Show
	FROM ranked_genres
	WHERE genre_rank = 1 GROUP BY country
	ORDER BY country;

______________________________________________________________________________________________________________________

-- DIRECTOR VS TITLE & RATING

CREATE OR REPLACE VIEW director_details AS(

	SELECT UNNEST(STRING_TO_ARRAY(director, ',')) AS director,
	"director" AS other_director, title, "cast",
	 rating,"type", show_id FROM netflix
)


	/* 11.Who are the most successful directors within each rating category, and how can Netflix use this 
	information to drive talent partnerships and content development strategies? */
	WITH top_director AS(
		SELECT "type", rating, director, DENSE_RANK()OVER(PARTITION BY "type", rating ORDER BY COUNT(director) DESC ) director_rank
		FROM director_details WHERE rating IS NOT NULL GROUP BY 1,2,3
	)

	SELECT rating,
	STRING_AGG(CASE WHEN "type" = 'Movie' THEN director END, ', ') AS Movie,
	STRING_AGG(CASE WHEN "type" = 'TV Show' THEN director END, ', ') AS TV_Show
	FROM top_director
	WHERE director_rank = 1 GROUP BY rating
	
	
	/* 12.What are the most significant projects for each director based on cast size and production 
	complexity, and how can this data inform Netflix’s future collaborations? */
	
	WITH biggest_title_directed AS(
	
		SELECT  director, title, (array_length(string_to_array(other_director, ','),1)-1) +
		array_length(string_to_array("cast", ','), 1) AS movie_weight FROM director_details
	
	), rank_title_directed AS(
		SELECT director, title, 
		DENSE_RANK()OVER(PARTITION BY director ORDER BY movie_weight DESC) As title_rank
		FROM biggest_title_directed
	
	)
	SELECT director, STRING_AGG(title, ', ')
	FROM rank_title_directed where title_rank = 1
	GROUP BY director


______________________________________________________________________________________________________________________

-- RATING ANALYSIS 

CREATE OR REPLACE VIEW rating_details AS(

	SELECT title , "type", rating, "cast", director, country, date_added, duration, listed_in, release_year FROM netflix
) 
	
	 
	/* 13.How can we identify the movie ratings that resonate most with audiences in different countries, 
	and use this data to tailor Netflix’s content offerings by region? */
	
	WITH popular_rated_movie AS(
		
			SELECT *, DENSE_RANK()OVER(PARTITION BY rating ORDER BY rating_count DESC ) AS ranking FROM
			(
				SELECT rating, UNNEST(STRING_TO_ARRAY(country, ',')) AS country ,COUNT(*) rating_count FROM rating_details
				GROUP BY rating, country
			) WHERE country IS NOT NULL OR country != '' AND rating IS NOT NULL
	
	
	)
	SELECT rating, STRING_AGG(country, ', ') FROM popular_rated_movie WHERE ranking = 1 GROUP BY rating
	

	/* 14.Which actors or actresses are driving viewership within each rating category, and how can Netflix 
	leverage star power to increase subscriptions and engagement? */
	SELECT rating, STRING_AGG(name, ', ')
	
	FROM (
		SELECT rating, UNNEST(STRING_TO_ARRAY("cast", ',')) name, COUNT(*), 
		DENSE_RANK()OVER(PARTITION BY rating ORDER BY COUNT(*) DESC) AS ranking FROM rating_details
		GROUP BY rating, 2 ORDER BY rating
	)
	WHERE ranking = 1 AND rating IS NOT NULL GROUP BY rating 
	

	/* 15.How consistent has Netflix been in releasing content across different rating categories, and how can this data guide 
	future content planning to ensure a balanced release strategy? */
	SELECT rating, ROUND(STDDEV_POP(release_count),2) AS release_variability,
	    ROUND(CAST(REGR_SLOPE(release_count, year_added) AS NUMERIC),2) AS release_trend_slope
		FROM(
				SELECT rating , EXTRACT(YEAR FROM date_added::DATE) AS year_added, COUNT(*) AS release_count FROM rating_details GROUP BY 1,2

		)
		GROUP BY 
    	rating
	ORDER BY 
    release_variability DESC

______________________________________________________________________________________________________________________


-- Duration Trends in each Rating for TV Shows vs. Movies

	/* 16.What are the viewing duration trends for TV shows and movies across different ratings, and how can this data help Netflix 
	optimize content length for maximum audience retention? */
	WITH duration_cte AS(
		SELECT release_year, "type", rating, ROUND(AVG(SPLIT_PART(duration, ' ', 1)::INT),1) AS duration
		FROM rating_details WHERE rating IS NOT NULL
		GROUP by 1,2,3 ORDER BY release_year
	)

	SELECT rating,
	ROUND(CAST(REGR_SLOPE(CASE WHEN "type" = 'Movie' THEN duration END, release_year) AS NUMERIC),2)AS Movie_Duraton_Trend,
	ROUND(CAST(REGR_SLOPE(CASE WHEN "type" = 'TV Show' THEN duration END, release_year) AS NUMERIC),2)AS TV_Show_Duraton_Trend
	FROM duration_cte
	GROUP BY rating

-- KEYWORDS OF EACH GENRE
	
	/* 17.What are the most common keywords in movie descriptions within each genre, and how can Netflix use this insight to 
	enhance SEO and recommendation algorithms for better user discovery? */
	WITH cte AS(
		SELECT "type", title, UNNEST(STRING_TO_ARRAY(listed_in, ',')) AS genre, description FROM netflix 
	),
	word_extracted AS(
	SELECT "type", title, genre, UNNEST(STRING_TO_ARRAY(LOWER(description), ' ')) AS word FROM cte
	),words_filtered AS (
		-- Step 2: Filter out common words (optional stop words like 'the', 'and', etc.)
		SELECT 
			title, 
			genre, 
			word
		FROM 
			word_extracted
		WHERE 
			word NOT IN ('the', 'a', 'an', 'and', 'or', 'but', 'so', 'yet', 'for', 'nor', 
	 'is', 'am', 'are', 'was', 'were', 'be', 'been', 'being', 'do', 'does', 'did', 
	 'have', 'has', 'had', 'can', 'will', 'shall', 'should', 'could', 'would', 'might', 
	 'may', 'must', 'if', 'either', 'neither', 'at', 'on', 'in', 'out', 'up', 'down', 'by', 
	 'to', 'with', 'from', 'about', 'over', 'under', 'into', 'through', 'before', 'after', 
	 'between', 'among', 'around', 'above', 'below', 'behind', 'beside', 'beyond', 'during', 
	 'inside', 'outside', 'toward', 'against', 'upon', 'I', 'you', 'he', 'she', 'it', 'we', 
	 'they', 'me', 'him', 'her', 'us', 'them', 'my', 'your', 'his', 'its', 'our', 'their', 
	 'this', 'that', 'these', 'those', 'there', 'here', 'who', 'whom', 'whose', 'which', 
	 'what', 'why', 'how', 'when', 'where', 'now', 'then', 'very', 'just', 'only', 'all', 
	 'any', 'no', 'not', 'than', 'as', 'of')

	), word_count AS (   SELECT 
			genre,
			word,
			COUNT(word) AS word_count
		FROM 
			words_filtered
		GROUP BY 
			genre, word
	), most_frequent_word AS (
		-- Step 4: Find the most repeated word in each genre
		SELECT 
			genre,
			word,
			word_count,
			RANK() OVER (PARTITION BY genre ORDER BY word_count DESC) AS rank
		FROM 
			word_count
	)
	-- Step 5: Select the top word per genre
	SELECT 
		genre,
		STRING_AGG(word, ', ') AS Keywords
	FROM 
		most_frequent_word
	WHERE 
		rank = 1
	GROUP BY
	genre
	ORDER BY 
		genre;
















/* ===========================================================
   🧹 STEP 1: REMOVE DUPLICATES FROM netflix_raw
   =========================================================== */
SELECT * 
FROM netflix_raw
WHERE CONCAT(UPPER(title), type) IN (
    SELECT CONCAT(UPPER(title), type)
    FROM netflix_raw
    GROUP BY UPPER(title), type
    HAVING COUNT(*) > 1
)
ORDER BY title;


/* ===========================================================
   🧮 STEP 2: KEEP ONLY UNIQUE ROWS (USING ROW_NUMBER)
   =========================================================== */
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_raw
)
SELECT * 
FROM cte 
WHERE rn = 1;


/* ===========================================================
   🎬 STEP 3: SPLIT MULTIPLE VALUES (DIRECTOR, CAST, COUNTRY, GENRE)
   =========================================================== */

-- 🎥 3.1 CREATE TABLE AND INSERT DATA FOR DIRECTORS
CREATE TABLE netflix_director (
    show_id VARCHAR(10),
    director VARCHAR(255)
);

INSERT INTO netflix_director (show_id, director)
SELECT show_id,
       TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(director, ',', n.n), ',', -1)) AS director
FROM netflix_raw
JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
) n
ON n.n <= 1 + LENGTH(director) - LENGTH(REPLACE(director, ',', ''))
WHERE director IS NOT NULL;


-- 🌍 3.2 CREATE TABLE AND INSERT DATA FOR COUNTRIES
CREATE TABLE netflix_country (
    show_id VARCHAR(10),
    country VARCHAR(150)
);

INSERT INTO netflix_country (show_id, country)
SELECT show_id,
       TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(country, ',', n.n), ',', -1)) AS country
FROM netflix_raw
JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9 UNION ALL SELECT 10
) n
ON n.n <= 1 + LENGTH(country) - LENGTH(REPLACE(country, ',', ''))
WHERE country IS NOT NULL;


-- 🎭 3.3 CREATE TABLE AND INSERT DATA FOR CAST
CREATE TABLE netflix_cast (
    show_id VARCHAR(10),
    actor VARCHAR(255)
);

INSERT INTO netflix_cast (show_id, actor)
SELECT show_id,
       TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(`cast`, ',', n.n), ',', -1)) AS actor
FROM netflix_raw
JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
) n
ON n.n <= 1 + LENGTH(`cast`) - LENGTH(REPLACE(`cast`, ',', ''))
WHERE `cast` IS NOT NULL;


-- 🎞️ 3.4 CREATE TABLE AND INSERT DATA FOR GENRES
CREATE TABLE netflix_genre (
    show_id VARCHAR(10),
    genre VARCHAR(255)
);

INSERT INTO netflix_genre (show_id, genre)
SELECT show_id,
       TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(listed_in, ',', n.n), ',', -1)) AS genre
FROM netflix_raw
JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9 UNION ALL SELECT 10
) n
ON n.n <= 1 + LENGTH(listed_in) - LENGTH(REPLACE(listed_in, ',', ''))
WHERE listed_in IS NOT NULL;


/* ===========================================================
   ⚙️ STEP 4: FILL NULL COUNTRIES BASED ON DIRECTOR-COUNTRY LINK
   =========================================================== */
INSERT INTO netflix_country
SELECT show_id, m.country
FROM netflix_raw nr
INNER JOIN (
    SELECT director, country
    FROM netflix_country nc 
    INNER JOIN netflix_director nd ON nc.show_id = nd.show_id
    GROUP BY director, country
) m ON nr.director = m.director
WHERE nr.country IS NULL;


/* ===========================================================
   🕒 STEP 5: MOVE RATING VALUES INTO DURATION (IF DURATION IS NULL)
   =========================================================== */
WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_raw
)
SELECT 
    show_id, 
    type, 
    title, 
    date_added,        -- Keep as VARCHAR
    release_year, 
    rating,
    CASE 
        WHEN duration IS NULL THEN rating 
        ELSE duration 
    END AS duration,
    description
FROM cte
WHERE rn = 1;


/* ===========================================================
   🧾 STEP 6: CREATE FINAL CLEANED TABLE 'netflix'
   =========================================================== */
CREATE TABLE netflix AS
WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_raw
)
SELECT 
    show_id, 
    type, 
    title, 
    date_added, 
    release_year, 
    rating,
    CASE 
        WHEN duration IS NULL THEN rating 
        ELSE duration 
    END AS duration,
    description
FROM cte
WHERE rn = 1;


/* ===========================================================
   🎬 STEP 7: FIND COUNT OF MOVIES & TV SHOWS BY EACH DIRECTOR
   =========================================================== */
SELECT 
    nd.director,
    COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS no_of_movies,
    COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS no_of_tvshows
FROM netflix n
INNER JOIN netflix_director nd ON n.show_id = nd.show_id
GROUP BY nd.director
HAVING COUNT(DISTINCT n.type) > 1;


/* ===========================================================
   🌍 STEP 8: WHICH COUNTRY HAS THE HIGHEST NUMBER OF COMEDY MOVIES
   =========================================================== */
SELECT 
    nc.country, 
    COUNT(DISTINCT ng.show_id) AS no_of_movies
FROM netflix_genre ng
INNER JOIN netflix_country nc ON ng.show_id = nc.show_id
INNER JOIN netflix n ON ng.show_id = n.show_id
WHERE ng.genre = 'Comedies' 
  AND n.type = 'Movie'
GROUP BY nc.country
ORDER BY no_of_movies DESC
LIMIT 1;


/* ===========================================================
   🎥 STEP 9: WHICH DIRECTOR RELEASED THE HIGHEST NUMBER OF MOVIES EACH YEAR
   =========================================================== */
WITH cte AS (
    SELECT 
        nd.director, 
        YEAR(date_added) AS date_year, 
        COUNT(n.show_id) AS no_of_movies
    FROM netflix n
    INNER JOIN netflix_director nd ON n.show_id = nd.show_id
    WHERE n.type = 'Movie'
    GROUP BY nd.director, YEAR(date_added)
),
cte2 AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY date_year ORDER BY no_of_movies DESC, director) AS rn
    FROM cte
)
SELECT *
FROM cte2
WHERE rn = 1;


/* ===========================================================
   ⏱️ STEP 10: AVERAGE DURATION OF MOVIES IN EACH GENRE
   =========================================================== */
-- (You can compute using CAST to INT for duration if stored as text)
SELECT 
    ng.genre,
    AVG(CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED)) AS avg_duration_minutes
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE n.type = 'Movie' 
  AND duration LIKE '%min%'
GROUP BY ng.genre;

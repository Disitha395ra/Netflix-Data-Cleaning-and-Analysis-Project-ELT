# 🎬 Netflix Data Analysis using ELT (Extract, Load, Transform)

## 📌 Overview
This project demonstrates the **ELT (Extract, Load, Transform)** process for analyzing the Netflix dataset using **SQL (MySQL)**.  
It focuses on cleaning, transforming, and analyzing Netflix data to extract meaningful insights about movies, TV shows, directors, genres, and countries.

---

## 🚀 Project Workflow

### 1️⃣ Extract
The raw dataset `netflix_raw` was imported into MySQL.  
It contains attributes like:  
- `show_id`, `type`, `title`, `director`, `cast`, `country`,  
- `date_added`, `release_year`, `rating`, `duration`, `listed_in`, `description`

### 2️⃣ Load
The dataset was loaded into a **MySQL database** for further processing.  
All data cleaning and transformation tasks were performed using SQL queries.

### 3️⃣ Transform
The transformation step involves:
- Removing duplicates  
- Splitting multiple values (e.g., directors, cast, countries, genres) into separate tables  
- Handling missing values  
- Type casting (e.g., converting `date_added` from **DATE** to **VARCHAR**)  
- Creating final clean tables for analysis  

---

## 🧹 Data Cleaning & Transformation Steps

### 🔹 Remove Duplicates
```sql
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_raw
)
SELECT * 
FROM cte 
WHERE rn = 1;
```
### Split Multiple Values into Separate Tables
```
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
```
### Country Table
```
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
```
## 🧠 Analytical Queries
### 🎥 1. Fill Missing Country Values
```
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
```
### Replace Missing Duration with Rating
```
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_raw
)
SELECT show_id, type, title, CAST(date_added AS VARCHAR(20)) AS date_added,
       release_year, rating,
       CASE WHEN duration IS NULL THEN rating ELSE duration END AS duration,
       description
INTO netflix
FROM cte
WHERE rn = 1;
```
### Count of Movies and TV Shows by Director
```
SELECT nd.director,
       COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS no_of_movies,
       COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS no_of_tvshows
FROM netflix n
INNER JOIN netflix_director nd ON n.show_id = nd.show_id
GROUP BY nd.director
HAVING COUNT(DISTINCT n.type) > 1;
```
### Country with the Highest Number of Comedy Movies
```
SELECT nc.country,
       COUNT(DISTINCT ng.show_id) AS no_of_movies
FROM netflix_genre ng
INNER JOIN netflix_country nc ON ng.show_id = nc.show_id
INNER JOIN netflix n ON ng.show_id = n.show_id
WHERE ng.genre = 'Comedies'
  AND n.type = 'Movie'
GROUP BY nc.country
ORDER BY no_of_movies DESC
LIMIT 1;
```
### Top Director per Year (by Movie Count)
```
WITH cte AS (
    SELECT nd.director,
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
SELECT * FROM cte2 WHERE rn = 1;
```
## 📊 Key Insights
Identified top directors and their yearly performance
Found countries leading in specific genres (like Comedies)
Cleaned and normalized Netflix data efficiently
Built reusable SQL transformations for ELT workflow

## 🧰 Tools & Technologies
Database: MySQL / MariaDB
Language: SQL
Concepts: ELT (Extract, Load, Transform)
Dataset: Netflix Titles Dataset (from Kaggle)

## Images
![avg_duration result_image](/image/first.png)

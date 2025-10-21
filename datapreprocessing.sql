# remove duplicates
SELECT * from netflix_raw
where concat(upper(title),type) in (
select concat(upper(title),type)
from netflix_raw
group by upper(title),type
having count(*)>1
)
order by title



with cte as (
SELECT * 
,ROW_NUMBER() over(PARTITION by title, type order by show_id) as rn

from netflix_raw
)

select * from cte where rn=1

#remove multiple values (directores, cast etc (the values seperate with comma))
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

#for country

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

#for cast
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

#netflix_genere
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

//change the data type use casting , in here date_added data(type) is date so we have to convert into VARCHAR


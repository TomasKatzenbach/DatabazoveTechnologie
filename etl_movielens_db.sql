-- Vytvorenie databázy
CREATE DATABASE RHINO_MOVIELENS;
--Vytvorenie schémy
CREATE SCHEMA RHINO_MOVIELENS.staging;
--Použitie schémy
USE SCHEMA RHINO_MOVIELENS.staging;



--Vytvorenie tabulky age_group(staging)
CREATE TABLE age_group_staging(
    id INT PRIMARY KEY,
    name VARCHAR(45)
);
--Vytvorenie tabulky occupations(staging)
CREATE TABLE occupations_staging(
    id INT PRIMARY KEY,
    name VARCHAR(255)
);
--Vytvorenie tabulky movies(staging)
CREATE TABLE movies_staging(
    id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);
--Vytvorenie tabulky genres(staging)
CREATE TABLE genres_staging(
    id INT PRIMARY KEY,
    name VARCHAR(255)
);
--Vytvorenie tabulky genres_movies(staging)
CREATE TABLE genres_movies_staging(
    id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY(movie_id) REFERENCES movies_staging(id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(id)
);
--Vytvorenie tabulky users(staging)
CREATE OR REPLACE TABLE users_staging(
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(255),
    FOREIGN KEY (age) REFERENCES age_group_staging(id),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id)
);
--Vytvorenie tabulky tags(staging)
CREATE TABLE tags_staging(
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    tags VARCHAR(4000),
    created_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
    
);
--Vytvorenie tabulky ratings(staging)

CREATE TABLE ratings_staging(
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);

--Vytvorenie stage
CREATE OR REPLACE STAGE my_stage;

--Kopírovanie dát do stage
COPY INTO age_group_staging
FROM @my_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO movies_staging
FROM @my_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_staging
FROM @my_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_movies_staging
FROM @my_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO users_staging
FROM @my_stage/users1.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO tags_staging
FROM @my_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO ratings_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


--ELT - Transformácia
--dim_users

CREATE OR REPLACE TABLE Dim_Users AS
SELECT DISTINCT
    u.id as Dim_UsersID,
    u.gender as Gender,
    u.zip_code as ZipCode,
    u.age as Age,
    o.name as Occupation,
    ag.name as AgeGroup
FROM users_staging u
JOIN age_group_staging ag ON u.age = ag.id
JOIN occupations_staging o ON u.occupation_id = o.id;


--dim_movies
CREATE OR REPLACE TABLE Dim_Movies AS
SELECT DISTINCT
    m.id AS Dim_MoviesID,
    m.title AS Title,
    m.release_year AS ReleaseYear,
    g.name as Genre,
FROM movies_staging m
JOIN genres_movies_staging gm ON m.id = gm.movie_id
JOIN genres_staging g ON gm.genre_id = g.id




-- dim_time
CREATE OR REPLACE TABLE Dim_Time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY CAST(r.rated_at AS DATE), EXTRACT(HOUR FROM r.rated_at), EXTRACT(MINUTE FROM r.rated_at)) AS Dim_TimeID,
    CAST(r.rated_at AS DATE) AS Date,
    TIME(r.rated_at) AS FullTime,
    EXTRACT(HOUR FROM r.rated_at) AS Hour,
    EXTRACT(MINUTE FROM r.rated_at) AS Minute,
    CASE 
        WHEN EXTRACT(HOUR FROM r.rated_at) < 12 THEN 'AM'
        ELSE 'PM'
    END AS AMPM
FROM ratings_staging r;

--dim_tags
CREATE OR REPLACE TABLE Dim_Tags AS
SELECT DISTINCT
    tg.id,
    tg.movie_id AS MoviesId,
    tg.user_id AS UsersId,
    tg.tags AS Tag,
    tg.created_at AS CreatedAt
FROM tags_staging tg;


--fact_ratings
CREATE OR REPLACE TABLE Fact_Ratings AS
SELECT
    r.id AS Fact_RatingsID,
    r.user_id AS UsersID,
    r.movie_id AS MoviesID,
    r.rating AS Rating,
    r.rated_at AS RatedAt,
    tg.tags AS Tagg,
    dt.Dim_TimeID AS TimeID
FROM ratings_staging r
LEFT JOIN tags_staging tg 
    ON r.user_id = tg.user_id 
   AND r.movie_id = tg.movie_id
LEFT JOIN Dim_Time dt 
    ON TIME(r.rated_at) = dt.FullTime;

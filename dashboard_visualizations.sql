//Priemerné hodnotenie pre konkrétnu vekovú skupinu.
SELECT 
    ag.name AS AgeGroup,
    AVG(r.rating) AS AvgRating
FROM ratings_staging r
JOIN users_staging u ON r.user_id = u.id
JOIN age_group_staging ag ON u.age = ag.id
GROUP BY ag.name
ORDER BY ag.name;


//Percentuálne rozdelenie hodnotení pre každý filmový žáner.
SELECT 
    g.name AS Genre,
    COUNT(r.id) AS RatingsCount
FROM ratings_staging r
JOIN movies_staging m ON r.movie_id = m.id
JOIN genres_movies_staging gm ON m.id = gm.movie_id
JOIN genres_staging g ON gm.genre_id = g.id
GROUP BY g.name
ORDER BY RatingsCount DESC;

//Počet hodnotení vytvorených v čase AM a PM.
SELECT 
    dt.AMPM,
    COUNT(r.id) AS RatingsCount
FROM ratings_staging r
JOIN Dim_Time dt ON TIME(r.rated_at) = dt.FullTime
GROUP BY dt.AMPM
ORDER BY dt.AMPM;

//Top 10 používateľov podľa počtu hodnotení
SELECT 
    u.id AS UserID,
    COUNT(r.id) AS RatingsCount
FROM ratings_staging r
JOIN users_staging u ON r.user_id = u.id
GROUP BY u.id
ORDER BY RatingsCount DESC
LIMIT 10;


//Priemerné hodnotenie filmov podľa roku vydania.
SELECT 
    m.release_year AS ReleaseYear,
    AVG(r.rating) AS AvgRating
FROM ratings_staging r
JOIN movies_staging m ON r.movie_id = m.id
GROUP BY m.release_year
ORDER BY m.release_year;

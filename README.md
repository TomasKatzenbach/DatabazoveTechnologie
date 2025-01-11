
# **ETL proces datasetu AmazonBooks**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich preferencií na základe hodnotení filmov a pridružených metadát. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v preferenciách, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z MovieLens datasetu dostupného [tu](https://grouplens.org/datasets/movielens/). Dataset obsahuje nasledujúce tabuľky:
- `movies`: Informácie o filmoch.
- `ratings`: Hodnotenia filmov používateľmi.
- `tags`: Štítky priradené k filmom používateľmi.
- `users`: Informácie o používateľoch.
- `age_group`: Informácie o vekovej skupine.
- `occupations`: Informácie o povolaniach.
- `genres`: Informácie o žánroch.

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

#### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://https://github.com/TomasKatzenbach/DatabazoveTechnologie/blob/main/MovieLens_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## **2. Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu, kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_movies`**: Obsahuje podrobné informácie o filmoch (názov, žánre).
- **`dim_users`**: Obsahuje demografické údaje o používateľoch, ako sú vekové kategórie, pohlavie a povolanie.
- **`dim_tags`**: Obsahuje zoznam štítkov priradených filmom používateľmi.

- **`dim_time`**: Obsahuje podrobné časové a dátumové údaje (dátum, hodina, AM/PM).
- - **`fact_ratings`**: Obsahuje údaje o hodnoteniach filmov používateľmi, vrátane hodnotenia, času hodnotenia, a identifikátorov filmov a používateľov. 


<p align="center">
  <img src="https://https://github.com/TomasKatzenbach/DatabazoveTechnologie/blob/main/star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>

---

## 3. ETL proces v Snowflake 

ETL proces pozostával z troch hlavných fáz: **extrahovanie (Extract)**, **transformácia (Transform)** a **načítanie (Load)**. Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

### 3.1 Extract (Extrahovanie dát)

Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. 
```sql
CREATE OR REPLACE STAGE my_stage;
```


Do stage boli následne nahraté súbory obsahujúce údaje o knihách, používateľoch, hodnoteniach, zamestnaniach a úrovniach vzdelania. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.
```sql
COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

### 3.2 Transform (Transformácia dát)

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

**Dimenzia `Dim_Users`** obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia, zamestnania a vzdelania. Transformácia zahŕňala rozdelenie veku používateľov do kategórií (napr. „18-24“) a pridanie popisov zamestnaní a vzdelania. Dimenzia je typu **SCD 2**, čo umožňuje sledovať historické zmeny v zamestnaní a vzdelaní používateľov.
```sql
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
```

**Dimenzia `Dim_Time`** uchováva informácie o dátumoch hodnotení filmov, vrátane odvodených údajov, ako sú deň, mesiac, rok, deň v týždni (v textovom aj číselnom formáte) . Dimenzia je typu **SCD Typ 0**, pretože záznamy sú nemenné a uchovávajú statické informácie.
```sql
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
```
Podobne **dimenzia `Dim_Movies`** obsahuje údaje o filmoch, ako sú názov, rok vydania a žáner. Táto dimenzia je tiež typu **SCD Typ 0**, keďže údaje o filmoch sú považované za nemenné.
```sql
CREATE OR REPLACE TABLE Dim_Movies AS
SELECT DISTINCT
    m.id AS Dim_MoviesID,
    m.title AS Title,
    m.release_year AS ReleaseYear,
    g.name as Genre,
FROM movies_staging m
JOIN genres_movies_staging gm ON m.id = gm.movie_id
JOIN genres_staging g ON gm.genre_id = g.id
```
**Dimenzia `Dim_Tags`** uchováva informácie o tagoch priradených k filmom, ktoré popisujú rôzne aspekty ich obsahu alebo charakteru. Tieto údaje zahŕňajú názov tagu, jeho jedinečný identifikátor a ďalšie súvisiace metadáta. Dimenzia je typu **SCD Typ 0**, pretože záznamy sú nemenné a uchovávajú statické informácie.
```sql
CREATE OR REPLACE TABLE Dim_Tags AS
SELECT DISTINCT
    tg.id,
    tg.movie_id AS MoviesId,
    tg.user_id AS UsersId,
    tg.tags AS Tag,
    tg.created_at AS CreatedAt
FROM tags_staging tg;
```

**Faktová tabuľka `fact_ratings`** obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.
```sql
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
```

### 3.3 Load (Načítanie dát)

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska.
``` sql
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS ratings_staging;
```



ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu čitateľských preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

## 4. Vizualizácia dát

Dashboard obsahuje 5 vizualizácií, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.
<p align="center">
  <img src="https://https://github.com/TomasKatzenbach/DatabazoveTechnologie/blob/main/movielens_dashboard.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>



### Graf 1: Priemerné hodnotenie pre konkrétnu vekovú skupinu 
Táto vizualizácia zobrazuje priemerné hodnotenie filmov pre rôzne vekové skupiny. Pomáha pochopiť, ako sa hodnotenia líšia medzi jednotlivými vekovými skupinami. Tieto informácie môžu byť užitočné pri analýze preferencií rôznych vekových kategórií a prispôsobiť marketingové kampane a odporúčania filmov podľa vekových skupín.
``` sql
SELECT 
    ag.name AS AgeGroup,
    AVG(r.rating) AS AvgRating
FROM ratings_staging r
JOIN users_staging u ON r.user_id = u.id
JOIN age_group_staging ag ON u.age = ag.id
GROUP BY ag.name
ORDER BY ag.name;
```


### Graf 2: Percentuálne rozdelenie hodnotení pre každý filmový žáner

Táto vizualizácia zobrazuje, ako sa hodnotenia filmov delia medzi rôzne filmové žánre. Zobrazuje počet hodnotení pre každý žáner a pomáha identifikovať, ktoré žánre sú najpopulárnejšie medzi používateľmi. Tieto informácie môžu byť využité na analýzu preferencií divákov a môžu pomôcť pri optimalizácii marketingových kampaní, výbere žánrov na odporúčanie používateľom alebo plánovaní produkcie nových filmov zameraných na najobľúbenejšie žánre.
``` sql
SELECT 
    g.name AS Genre,
    COUNT(r.id) AS RatingsCount
FROM ratings_staging r
JOIN movies_staging m ON r.movie_id = m.id
JOIN genres_movies_staging gm ON m.id = gm.movie_id
JOIN genres_staging g ON gm.genre_id = g.id
GROUP BY g.name
ORDER BY RatingsCount DESC;
```



### Graf 3: Počet hodnotení vytvorených v čase AM a PM 
Táto vizualizácia zobrazuje, ako sa hodnotenia filmov rozdeľujú medzi časové obdobia AM (ráno) a PM (večer). Pomáha určiť, kedy používatelia najčastejšie hodnotia filmy. Zistené údaje môžu byť využité na optimalizáciu času zverejnenia nových filmov, aby sa maximalizovala interakcia používateľov počas najväčšej aktivity.
``` sql
SELECT 
    dt.AMPM,
    COUNT(r.id) AS RatingsCount
FROM ratings_staging r
JOIN Dim_Time dt ON TIME(r.rated_at) = dt.FullTime
GROUP BY dt.AMPM
ORDER BY dt.AMPM;
```


### Graf 4: Priemerné hodnotenie a počet hodnotení podľa tagov 
Táto vizualizácia zobrazuje priemerné hodnotenie filmov a počet hodnotení podľa rôznych tagov. Umožňuje zistiť, ktoré tagy sú najpopulárnejšie medzi používateľmi a aké hodnotenia tieto filmy získali. Tieto informácie môžu byť využité na analýzu preferencií divákov a môžu pomôcť pri výbere filmov na odporúčanie používateľom, ktorí preferujú určité tagy alebo tematické okruhy.
``` sql
SELECT 
    dt.Tag AS Tag,
    AVG(r.rating) AS AvgRating,
    COUNT(r.id) AS RatingsCount
FROM ratings_staging r
JOIN Dim_Tags dt ON r.movie_id = dt.MoviesId AND r.user_id = dt.UsersId
GROUP BY dt.Tag
ORDER BY RatingsCount DESC, AvgRating DESC;
```



### Graf 5: Priemerné hodnotenie filmov podľa roku vydania 
Táto vizualizácia zobrazuje priemerné hodnotenie filmov podľa roku ich vydania. Pomáha identifikovať trendy v hodnotení filmov v priebehu času a môže ukázať, či sa hodnotenia zlepšujú alebo zhoršujú v závislosti od roku vydania. Tieto informácie môžu byť využité na analýzu vývoja kvality filmov alebo na predikciu, ako budú diváci hodnotiť filmy z budúcich rokov.
``` sql
SELECT 
    m.release_year AS ReleaseYear,
    AVG(r.rating) AS AvgRating
FROM ratings_staging r
JOIN movies_staging m ON r.movie_id = m.id
GROUP BY m.release_year
ORDER BY m.release_year;
```
Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa čitateľských preferencií a správania používateľov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a knižničných služieb.

---


**Autor:** Tomáš Katzenbach


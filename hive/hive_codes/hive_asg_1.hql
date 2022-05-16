DROP TABLE IF EXISTS review;
CREATE TABLE review (id int, productid string, userid string, profilename string, hnum int, hden int, score int, zaman bigint, summ string, metin string) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '/home/beyza/Downloads/Reviews.csv' OVERWRITE INTO TABLE review;

DROP TABLE IF EXISTS year_sum;
CREATE TABLE year_sum (yil int, summ string) row format delimited fields terminated by ',' COLLECTION ITEMS TERMINATED BY ' '; 
INSERT INTO year_sum 
SELECT year(from_unixtime(zaman)) AS yil, lower(trim(regexp_replace(summ, '[\'_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"]', " "))) 
FROM review;

DROP TABLE IF EXISTS result;
CREATE TABLE result (yil int, word string, occ int) row format delimited fields terminated by ',';
INSERT OVERWRITE TABLE result 
SELECT yil, word, COUNT(*) as count 
FROM year_sum LATERAL VIEW explode(split(summ, ' +')) lTable as word 
WHERE yil>=1999 
GROUP BY yil,word 
ORDER BY count DESC, word ASC;

DROP TABLE IF EXISTS rank_result;
CREATE TABLE rank_result as (
    SELECT yil, word, occ, row_number() over (partition by yil order by occ desc) as rnk
    FROM result
);

SELECT yil, collect_set(concat_ws(" ",word, cast(occ as string)))
FROM rank_result 
WHERE rnk<=10 
GROUP BY yil;
DROP TABLE IF EXISTS review;
CREATE TABLE review (id int, productid string, userid string, profilename string, hnum int, hden int, score int, zaman bigint, summ string, metin string) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '/home/beyza/Downloads/Reviews.csv' OVERWRITE INTO TABLE review;

DROP TABLE IF EXISTS product_score_byear;
CREATE TABLE product_score_byear (yil int, productid string, score int) row format delimited fields terminated by ',' COLLECTION ITEMS TERMINATED BY ' '; 
INSERT INTO product_score_byear 
SELECT year(from_unixtime(zaman)) AS yil, productid, score 
FROM review;

DROP TABLE IF EXISTS result;
CREATE TABLE result (productid string, yil int, avg_score double) row format delimited fields terminated by ',';
INSERT OVERWRITE TABLE result 
SELECT productid, yil, round(AVG(score), 2) as avg_score 
FROM product_score_byear
WHERE yil>=2003 and yil<=2012
GROUP BY productid, yil 
ORDER BY productid DESC, yil ASC;

SELECT productid, collect_set(concat_ws(" ",cast(yil as string), ":",  cast(avg_score as string)))
FROM result  
GROUP BY productid;
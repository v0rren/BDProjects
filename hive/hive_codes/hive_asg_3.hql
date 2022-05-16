DROP TABLE IF EXISTS review;
CREATE TABLE review (id int, productid string, userid string, profilename string, hnum int, hden int, score int, zaman bigint, summ string, metin string) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '/home/beyza/Downloads/Reviews.csv' OVERWRITE INTO TABLE review;

DROP TABLE IF EXISTS product_user_1;
CREATE TABLE product_user_1 (productid string, userid string) row format delimited fields terminated by ',' COLLECTION ITEMS TERMINATED BY ' '; 
INSERT INTO product_user_1 
SELECT DISTINCT productid, userid
FROM review;

DROP TABLE IF EXISTS product_user_2;
CREATE TABLE product_user_2 (productid1 string, productid2 string, userid string) row format delimited fields terminated by ',' COLLECTION ITEMS TERMINATED BY ' '; 
INSERT INTO product_user_2 
SELECT p1.productid, p2.productid, p1.userid
FROM product_user_1 p1 inner join product_user_1 p2 on p1.userid=p2.userid and p1.productid<>p2.productid and p1.productid<p2.productid;

SELECT productid1, productid2, COUNT(*)
FROM product_user_2
GROUP BY productid1, productid2;
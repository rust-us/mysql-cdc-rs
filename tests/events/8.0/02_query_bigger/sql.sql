CREATE TABLE if not exists `LINEITEM`
(
    `L_ORDERKEY`      bigint         NOT NULL,
    `L_PARTKEY`       int            NOT NULL,
    `L_SUPPKEY`       int            NOT NULL,
    `L_LINENUMBER`    bigint         NOT NULL,
    `L_QUANTITY`      decimal(12, 3) NOT NULL,
    `L_EXTENDEDPRICE` decimal(13, 2) NOT NULL,
    `L_DISCOUNT`      decimal(10, 1) NOT NULL,
    `L_TAX`           decimal(12, 1) NOT NULL,
    `L_RETURNFLAG`    varchar(128)  NOT NULL,
    `L_LINESTATUS`    varchar(8)     NOT NULL,
    `L_SHIPDATE`      date           NOT NULL,
    `L_COMMITDATE`    date           NOT NULL,
    `L_RECEIPTDATE`   date           NOT NULL,
    `L_SHIPINSTRUCT`  varchar(128)       NOT NULL,
    `L_SHIPMODE`      varchar(128)       NOT NULL,
    `L_COMMENT`       varchar(128)      NOT NULL,
    primary key (L_ORDERKEY, L_LINENUMBER, L_SHIPDATE)
);


select * from LINEITEM;


INSERT INTO LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS,
                      L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)
VALUES (1234567890111, 1235111, 13711, 888878711, 99.911, 76.11, 888.1, 109.1, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test', 'com');


select * from LINEITEM;


INSERT INTO LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS,
                      L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)
VALUES (12345678909876, 12356789, 13789, 888878787, 99.998, 76.77, 888.7, 109.7, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test', 'com'),
       (12345678909877, 12356790, 13789, 888878788, 99.997, 76.88, 888.1, 109.8, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test', 'com'),
       (12345678909878, 12356791, 13790, 888878789, 99.999, 76.99, 888.5, 109.9, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test', '使用箭头标记 -> 不是 SQL 语句的一部分，它仅仅表示一个新行，如果一条 SQL 语句太长，我们可以通过回车键来创建一个新行来编写 SQL 语句，SQL 语句的命令结束符为分号 ;。'),
       (12345678909879, 12356792, 13791, 888878790, 99.995, 76.01, 888.3, 109.1, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test', 'com'),
       (12345678909880, 12356792, 13791, 888878791, 99.999, 76.22, 888.8, 109.0, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test', 'com');


select * from LINEITEM;
select * from LINEITEM where L_ORDERKEY in (select L_ORDERKEY from LINEITEM where L_ORDERKEY > 12345678909877);

update LINEITEM set L_QUANTITY = 88.880, L_RETURNFLAG = 'update L_RETURNFLAG ' where L_ORDERKEY = 12345678909877;
select * from LINEITEM where L_ORDERKEY in (select L_ORDERKEY from LINEITEM where L_ORDERKEY = 12345678909877);


delete from LINEITEM where L_ORDERKEY > 12345678909879;
-- Query OK, 1 row affected (0.00 sec)

delete from LINEITEM where L_ORDERKEY >= 12345678909879;
-- Query OK, 1 row affected (0.00 sec)

CREATE TABLE if not exists `Demo`
(
    `L_ORDERKEY`      bigint         NOT NULL,
    `L_PARTKEY`       int            NOT NULL,
    `L_SUPPKEY`       int            NOT NULL,
    `L_LINENUMBER`    bigint         NOT NULL,
    `L_QUANTITY`      decimal(12, 3) NOT NULL,
    `L_EXTENDEDPRICE` decimal(13, 2) NOT NULL,
    `L_DISCOUNT`      decimal(10, 1) NOT NULL,
    `L_TAX`           decimal(12, 1) NOT NULL,
    `L_RETURNFLAG`    varchar(128)  NOT NULL,
    `L_LINESTATUS`    varchar(8) NULL ,
    `L_SHIPDATE`      date     NOT NULL,
    `L_COMMITDATE`    date,
    `L_RECEIPTDATE`   date           NOT NULL,
    `L_SHIPINSTRUCT`  varchar(128)       DEFAULT NULL,
    `L_SHIPMODE`      varchar(128)       DEFAULT NULL,
    `L_COMMENT`       varchar(128)      DEFAULT NULL,
    primary key (L_ORDERKEY, L_LINENUMBER, L_SHIPDATE)
);

INSERT INTO Demo (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS,
                      L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE)
VALUES (12345678909876, 12356789, 13789, 888878787, 99.998, 76.77, 888.7, 109.7, 'code', NULL,
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test'),
       (12345678909877, 12356790, 13789, 888878788, 99.997, 76.88, 888.1, 109.8, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test'),
       (12345678909878, 12356791, 13790, 888878789, 99.999, 76.99, 888.5, 109.9, 'code', NULL,
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test'),
       (12345678909879, 12356792, 13791, 888878790, 99.995, 76.01, 888.3, 109.1, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test'),
       (12345678909880, 12356792, 13791, 888878791, 99.999, 76.22, 888.8, 109.0, 'code', 'Y',
        '1990-08-01 12:13:16', '1990-06-01 12:13:16', '1990-01-01 12:13:16', 'test@test.com', 'test');


select * from Demo;
mysql> CREATE TABLE int_table (
    `col1` tinyint(4) DEFAULT NULL,
    `col2` smallint(6) DEFAULT NULL,
    `col3` mediumint(9) DEFAULT NULL,
    `col4` int(11) DEFAULT NULL,
    `col5` bigint(20) DEFAULT NULL,
    `col6` tinyint(1) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
Query OK, 0 rows affected, 8 warnings (0.01 sec)

mysql> select * from int_table;
Empty set (0.00 sec)

mysql> insert into int_table values(1,11,111,1111,11111,true);
Query OK, 1 row affected (0.00 sec)

mysql> select * from int_table;
+------+------+------+------+-------+------+
| col1 | col2 | col3 | col4 | col5  | col6 |
+------+------+------+------+-------+------+
|    1 |   11 |  111 | 1111 | 11111 |    1 |
+------+------+------+------+-------+------+
1 row in set (0.00 sec)

mysql> update int_table set col2=22,col3=222 where col1=1;
Query OK, 1 row affected (0.00 sec)
Rows matched: 1  Changed: 1  Warnings: 0

mysql> select * from int_table;
+------+------+------+------+-------+------+
| col1 | col2 | col3 | col4 | col5  | col6 |
+------+------+------+------+-------+------+
|    1 |   22 |  222 | 1111 | 11111 |    1 |
+------+------+------+------+-------+------+
1 row in set (0.00 sec)
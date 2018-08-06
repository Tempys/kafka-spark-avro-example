CREATE TABLE IF NOT EXISTS `test.test_table` (
`SquareId` integer NOT NULL DEFAULT '0',
`Polygon` varchar NOT NULL DEFAULT '0',
`proccessingtime` timestamp NOT NULL ,
`memsql_insert_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
KEY(`memsql_insert_time`), SHARD())
-- ---------------------------------------------------
use datatest;
-- ---------------------------------------------------

 create table IF NOT EXISTS jails (id int, county varchar(20));
/*
 INSERT INTO jails (id, county) VALUES (1, 'anson');
 INSERT INTO jails (id, county) VALUES (13, 'guilford');
 INSERT INTO jails (id, county) VALUES (25, 'wake');
 INSERT INTO jails (id, county) VALUES (11, 'durham');
 INSERT INTO jails (id, county) VALUES (12, 'forsyth');
 INSERT INTO jails (id, county) VALUES (21, 'orange');
 INSERT INTO jails (id, county) VALUES (31, 'cumberland');
*/

 create table IF NOT EXISTS jail_records (
	id int
 , jail_id int
 , scrape_request_id int
 , name varchar(255)
 , dob DATE
 , age int
 , sex varchar(255)
 , height int
 , race varchar(255)
 , confined_date VARCHAR(255)
 , release_date VARCHAR(255)
 , non_court int
 , non_court_verification_info varchar(255)
 , address varchar(255)
 , days_in_jail int
 , other_data varchar(255)
 , created_at DATETIME
 , updated_at TIMESTAMP
 , incarceration_id int
 , scrape_id int
-- , record_date DATE
 , INDEX(id)
 , INDEX(jail_id)
 , INDEX(jail_id,incarceration_id)
-- , INDEX(name, jail_id,record_date)
-- , INDEX(jail_id,record_date)
-- , INDEX(record_date)
 )
  ;

/*
INSERT INTO jail_records (jail_id, name, incarceration_id, created_at) VALUES (1, 'John Smith', 1, '2019-03-21');
INSERT INTO jail_records (jail_id, name, incarceration_id, created_at) VALUES (1, 'John Smith', 1, '2019-03-22');
INSERT INTO jail_records (jail_id, name, incarceration_id, created_at) VALUES (1, 'John Smith', 1, '2019-03-23');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'John Smith', '2019-03-27');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'John Smith', '2019-03-28');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'John Smith', '2019-03-29');
-- INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'John Smith', '2019-03-30');	-- Causes the first YES below to become a NO
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-03-24');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-03-25');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-03-26');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-03-27');
-- INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-03-28'); -- NO
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-03-29');
-- INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-03-30'); -- YES
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-03-31');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-04-01');
-- INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-04-02'); -- YES
-- INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-04-03'); -- YES
-- INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-04-04'); -- YES
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-04-05');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-04-06');
INSERT INTO jail_records (jail_id, name, created_at) VALUES (1, 'Sally Smith', '2019-04-07');
*/

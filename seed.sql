-- ---------------------------------------------------
use datatest;
-- ---------------------------------------------------

DROP TABLE IF EXISTS imputed_dates;
CREATE TABLE IF NOT EXISTS imputed_dates (id int not null auto_increment primary key , incarceration_id int, imputed_date date);

 drop table if exists jails;
 create table IF NOT EXISTS jails (id int, county varchar(20));
 INSERT INTO jails (id, county) VALUES (1, 'anson');
 INSERT INTO jails (id, county) VALUES (13, 'guilford');
 INSERT INTO jails (id, county) VALUES (25, 'wake');
 INSERT INTO jails (id, county) VALUES (11, 'durham');
 INSERT INTO jails (id, county) VALUES (12, 'forsyth');
 INSERT INTO jails (id, county) VALUES (21, 'orange');
 INSERT INTO jails (id, county) VALUES (31, 'cumberland');

 DROP TABLE if exists incarcerations;
create table IF NOT EXISTS incarcerations (id int not null auto_increment primary key
		, name varchar(200)
		, dob DATE
		, age INT
		, jail_id int
		, court_case_id int
		, court_match_date date
		, first_found_date DATE
		, last_found_date DATE
		, sex VARCHAR(255)
		, height INT
		, race VARCHAR(255)
		, confined_date VARCHAR(255)
		, release_date VARCHAR(255)
		, non_court TINYINT
		, address VARCHAR(255) 
		, days_in_jail INT
		, created_at DATETIME NULL
		, updated_at TIMESTAMP NULL
		, most_recent_age INT
		, state_prison_lookup json
		, state_prison_dob date
		, prison_match_date date
		, scrape_id INT
		, INDEX(id), INDEX(jail_id, name), INDEX(last_found_date)
	);

-- drop table if exists jail_records;
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
 , created_at DATETIME NULL
 , updated_at TIMESTAMP NULL
 , incarceration_id int
 , scrape_id int
 , record_date DATE
 , INDEX(id)
 , INDEX(jail_id)
 , INDEX(jail_id,incarceration_id)
 , INDEX(name, jail_id,record_date)
 , INDEX(jail_id,record_date)
 , INDEX(record_date)
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

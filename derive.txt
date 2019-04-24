-- ---------------------------------------------------
use datatest;
-- ---------------------------------------------------


-- DROP TABLE jet_imputed_dates;
create table IF NOT EXISTS jet_imputed_dates (id int not null auto_increment primary key, incarceration_id int, imputed_date DATE);

-- DROP TABLE jet_incarcerations;
create table IF NOT EXISTS jet_incarcerations (id int not null auto_increment primary key, jail_id int, name varchar(200), created_at DATETIME, first_found_date varchar(10), last_found_date varchar(10), INDEX(id), INDEX(jail_id, name), INDEX(last_found_date));

DELIMITER //

DROP PROCEDURE IF EXISTS handleJailRecord;
CREATE PROCEDURE handleJailRecord(IN j_id INT, IN p_name varchar(200), IN p_date DATE)
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE i_found BOOLEAN DEFAULT FALSE;
	DECLARE i_id INT;
	DECLARE i_name VARCHAR(200);
	DECLARE i_lfd VARCHAR(10);
	DECLARE i_ffd VARCHAR(10);
	DECLARE i_lastdate DATE ;

	DECLARE getIncs CURSOR FOR 
		SELECT id, first_found_date, last_found_date
		FROM jet_incarcerations 
		WHERE (name = p_name) AND (jail_id = j_id)
		ORDER BY last_found_date DESC
		LIMIT 1
		;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

	-- try imputing / finding
	SET done = FALSE;
	OPEN getIncs;
	read_loop: LOOP
		FETCH getIncs INTO i_id, i_ffd, i_lfd;
		IF done THEN
			LEAVE read_loop;
		END IF;
		
		IF (i_ffd <= p_date) AND (i_lfd >= p_date) THEN
			SET i_found = TRUE;
			UPDATE jail_records SET incarceration_id = i_id WHERE (name = p_name) AND (jail_id = j_id) AND (record_date = p_date);

		ELSEIF (i_lfd = (p_date - INTERVAL 1 DAY)) THEN
			SET i_found = TRUE;
			UPDATE jet_incarcerations SET last_found_date = p_date WHERE (id = i_id);
			UPDATE jail_records SET incarceration_id = i_id WHERE (name = p_name) AND (jail_id = j_id);

		ELSEIF 
			(i_lfd > (p_date - INTERVAL 5 DAY) )
				AND		
			(NOT EXISTS(
				SELECT id from jail_records
				WHERE (jail_id = j_id)
				AND (record_date > i_lfd) AND (record_date < p_date)
				))
			THEN
				SET i_found = TRUE;
				UPDATE jail_records SET incarceration_id = i_id WHERE (name = p_name) AND (jail_id = j_id) AND (record_date = p_date);
				UPDATE jet_incarcerations SET last_found_date = p_date WHERE (id = i_id);
				SET i_lastdate = i_lfd;
				each_loop: LOOP
					IF (i_lastdate is NULL) THEN
						LEAVE each_loop;
					END IF;
					SET i_lastdate = (i_lastdate + INTERVAL 1 DAY);
					IF (i_lastdate >= p_date) THEN
						LEAVE each_loop;
					END IF;
					INSERT INTO jet_imputed_dates (incarceration_id, imputed_date) VALUES (i_id, i_lastdate);
--					SELECT concat('    o----> IMPUTING: ', p_name, ' (date: ', i_lastdate, ')') as '';
				END LOOP;

		END IF;
	END LOOP;
	CLOSE getIncs;

	IF NOT i_found THEN
		INSERT INTO jet_incarcerations (jail_id, name, first_found_date, last_found_date) VALUES (j_id, p_name, p_date, p_date);
		UPDATE jail_records SET incarceration_id = LAST_INSERT_ID() WHERE id = (name = p_name) AND (jail_id = j_id) AND (record_date = p_date);
--		SELECT concat('    +----> CREATING: ', LAST_INSERT_ID(), ': ', p_name, ' (last_found_date: ', p_date, ')', " | ", p_date) as '';
	END IF;
	
END//


DROP PROCEDURE IF EXISTS processJailRecords;
CREATE PROCEDURE processJailRecords(IN mindate DATE, IN j_id INT)
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE r_id INT;
	DECLARE r_name VARCHAR(200);
	DECLARE r_date DATE;
	DECLARE cursorRecords CURSOR FOR 
		SELECT name, record_date
		FROM jail_records
		WHERE (jail_id = j_id)
		AND (incarceration_id is null)
		AND (record_date >= mindate)
		GROUP BY name, record_date
		order by name, record_date
		;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	OPEN cursorRecords;
	read_loop: LOOP
		FETCH cursorRecords INTO r_name, r_date;
		IF done THEN
			LEAVE read_loop;
		END IF;
			SELECT concat('  Processing RECORD: ', r_name, ' (', r_date, ')') AS '';
		call handleJailRecord(j_id, r_name, r_date);
	END LOOP;
	CLOSE cursorRecords;
END//

DROP PROCEDURE IF EXISTS processJails;
CREATE PROCEDURE processJails(IN mindate DATE)
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE j_id INT;
	DECLARE j_county VARCHAR(20);
	DECLARE cursorJails CURSOR FOR 
		SELECT id , county
		FROM jails
		ORDER BY county
		;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	OPEN cursorJails;
	read_loop: LOOP
		FETCH cursorJails INTO j_id, j_county;
		IF done THEN
			LEAVE read_loop;
		END IF;
			SELECT concat('Processing JAIL: ', j_county) AS '';
		call processJailRecords(mindate, j_id);
	END LOOP;
	CLOSE cursorJails;
END//

-- -----------------------------------------------------------------------------

-- UPDATE jail_records SET record_date = DATE(created_at) WHERE record_date is NULL;

-- -----------------------------------------------------------------------------

 DELETE FROM jet_incarcerations;

	SELECT '-----------------------------------------------------' AS '';
	SELECT concat('BEGIN: ', NOW()) AS '';
	SELECT '-----------------------------------------------------' AS '';
 call processJailRecords('2018-01-01', 1); -- anson
-- call processJailRecords('2018-01-01', 11); -- durham
-- call processJailRecords('2018-01-01', 13); -- guilford
-- call processJailRecords('2018-01-01', 12); -- forsyth
-- call processJailRecords('2018-01-01', 21); -- orange
-- call processJailRecords('2018-01-01', 25); -- wake
-- call processJailRecords('2018-01-01', 31);	-- cumberland
-- call processJails('2019-03-01');
-- SELECT * FROM jail_records ORDER BY name, created_at;
-- SELECT * FROM jet_incarcerations ORDER BY name, first_found_date;
-- SELECT * FROM jet_imputed_dates ORDER BY incarceration_id;
	SELECT '-----------------------------------------------------' AS '';
	SELECT concat('END: ', NOW()) AS '';
	SELECT '-----------------------------------------------------' AS '';






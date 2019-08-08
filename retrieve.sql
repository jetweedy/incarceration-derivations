-- ---------------------------------------------------
use datatest;
-- ---------------------------------------------------


DROP PROCEDURE IF EXISTS retrieveIncStuff;
DROP PROCEDURE IF EXISTS retrieveMatchingCourtCase;
DROP PROCEDURE IF EXISTS retrieveMatchingPrison;

DELIMITER //

CREATE PROCEDURE retrieveMatchingPrison(IN i_id INT, IN p_name varchar(200), IN p_age INT, IN p_dob DATE, IN p_jail_id INT)
BEGIN

	DECLARE error_found INT DEFAULT FALSE;
	DECLARE done INT DEFAULT FALSE;
	DECLARE i_found BOOLEAN DEFAULT FALSE;
	DECLARE inc_prison_match_date DATE;
	DECLARE inc_state_prison_lookup TEXT;
	DECLARE inc_state_prison_dob DATE;
	
	DECLARE getMatchingPrison CURSOR FOR 
		SELECT prison_match_date, state_prison_lookup, state_prison_dob
		FROM incarcerations_backup
		WHERE (court_case_id IS NOT NULL)
			AND (name = p_name) 
			AND (jail_id = p_jail_id)
			AND (
				(age = p_age) OR (dob = p_dob)
--				OR ((dob IS NULL) AND (age IS NULL))
			)
		ORDER BY id
		LIMIT 1
		;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	DECLARE CONTINUE HANDLER FOR 1366 SELECT 'Error inserting integer value.';
	SET done = FALSE;
	OPEN getMatchingPrison;
	read_loop: LOOP
		FETCH getMatchingPrison INTO inc_prison_match_date, inc_state_prison_lookup, inc_state_prison_dob;
		IF done THEN
			LEAVE read_loop;
		END IF;		

		SELECT p_name, p_age, p_dob, p_jail_id, inc_prison_match_date, inc_state_prison_lookup, inc_state_prison_dob;
		UPDATE incarcerations SET prison_match_date = inc_prison_match_date, state_prison_lookup = inc_state_prison_lookup, state_prison_dob = inc_state_prison_dob WHERE id = i_id;
		
	END LOOP;
	CLOSE getMatchingPrison;

END//

CREATE PROCEDURE retrieveMatchingCourtCase(IN i_id INT, IN p_name varchar(200), IN p_age INT, IN p_dob DATE, IN p_jail_id INT)
BEGIN

	DECLARE error_found INT DEFAULT FALSE;
	DECLARE done INT DEFAULT FALSE;
	DECLARE i_found BOOLEAN DEFAULT FALSE;
	DECLARE inc_court_case_id INT;
	DECLARE inc_court_match_date DATE;
	
	DECLARE getMatchingCourtCase CURSOR FOR 
		SELECT court_case_id, court_match_date
		FROM incarcerations_backup
		WHERE (prison_match_date IS NOT NULL)
			AND (name = p_name) 
			AND (jail_id = p_jail_id)
			AND (
				(age = p_age) OR (dob = p_dob)
--				OR ((dob IS NULL) AND (age IS NULL))
			)
		ORDER BY id
		LIMIT 1
		;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	DECLARE CONTINUE HANDLER FOR 1366 SELECT 'Error inserting integer value.';
	SET done = FALSE;
	OPEN getMatchingCourtCase;
	read_loop: LOOP
		FETCH getMatchingCourtCase INTO inc_court_case_id, inc_court_match_date;
		IF done THEN
			LEAVE read_loop;
		END IF;		

		SELECT p_name, p_age, p_dob, p_jail_id, inc_court_case_id, inc_court_match_date;
		UPDATE incarcerations SET court_case_id = inc_court_case_id, court_match_date = inc_court_match_date WHERE id = i_id;
		
	END LOOP;
	CLOSE getMatchingCourtCase;

END//



CREATE PROCEDURE retrieveIncStuff()
BEGIN

	DECLARE error_found INT DEFAULT FALSE;
	DECLARE done INT DEFAULT FALSE;
	DECLARE i_found BOOLEAN DEFAULT FALSE;
	DECLARE i_id INT;
	DECLARE i_jail_id INT;
	DECLARE i_name VARCHAR(200);
	DECLARE i_age INT;
	DECLARE i_dob DATE;
	DECLARE i_prison_match_date DATE;
	DECLARE i_court_case_id INT;
	
	DECLARE getIncs CURSOR FOR 
		SELECT id, name, age, dob, jail_id, prison_match_date, court_case_id
		FROM incarcerations 
		WHERE ( (prison_match_date IS NULL) OR (court_case_id IS NULL) )
			AND (name IS NOT NULL) AND ((age IS NOT NULL) OR (dob IS NOT NULL))
--		LIMIT 1000
		;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	DECLARE CONTINUE HANDLER FOR 1366 SELECT 'Error inserting integer value.';
	SET done = FALSE;
	OPEN getIncs;
	read_loop: LOOP
		FETCH getIncs INTO i_id, i_name, i_age, i_dob, i_jail_id, i_prison_match_date, i_court_case_id;
		IF done THEN
			LEAVE read_loop;
		END IF;		

--		SELECT i_id, i_name, i_age, i_dob, i_jail_id, i_prison_match_date, i_court_case_id;
		IF i_court_case_id IS NULL THEN
			call retrieveMatchingCourtCase(i_id, i_name, i_age, i_dob, i_jail_id);
		END IF;
		IF i_prison_match_date IS NULL THEN
			call retrieveMatchingPrison(i_id, i_name, i_age, i_dob, i_jail_id);
		END IF;

	END LOOP;
	CLOSE getIncs;


END//


 call retrieveIncStuff();


-- ---------------------------------------------------
use datatest;
-- ---------------------------------------------------

DELIMITER //

DROP PROCEDURE IF EXISTS compareIncarcerations;
CREATE PROCEDURE compareIncarcerations(IN p_id INT, IN p_jail_id INT, IN p_name VARCHAR(200), IN p_ffd DATE, IN p_lfd DATE)
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE inc_id INT;
	DECLARE inc_ffd DATE;
	DECLARE inc_lfd DATE;
	DECLARE inc_name VARCHAR(100);
	DECLARE cursorComps CURSOR FOR 
		SELECT id, name, first_found_date, last_found_date 
		FROM incarcerations
		WHERE (id <> p_id) AND (name = p_name) AND (jail_id = p_jail_id)
		AND (
			(p_ffd BETWEEN first_found_date AND last_found_date)
		AND
			(p_lfd BETWEEN first_found_date AND last_found_date)
		)
		;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	OPEN cursorComps;
	read_loop: LOOP
		FETCH cursorComps INTO inc_id, inc_name, inc_ffd, inc_lfd;
		IF done THEN
			LEAVE read_loop;
		END IF;
		SELECT '------------------------------------------------------------------' AS '';
		SELECT concat('Found Duplicate: (Jail ID: ', p_jail_id , ') ', p_id, ": ", p_name, " | ", p_ffd, " - ", p_lfd) AS '';
		SELECT concat('Inside of Range: (Jail ID: ', p_jail_id , ') ', inc_id, ": ", inc_name, " | ", inc_ffd, " - ", inc_lfd) AS '';
	END LOOP;
	CLOSE cursorComps;

END//

DROP PROCEDURE IF EXISTS checkIncarcerations;
CREATE PROCEDURE checkIncarcerations(IN p_jail_id INT)
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE inc_id INT;
	DECLARE inc_name VARCHAR(200);
	DECLARE inc_ffd DATE;
	DECLARE inc_lfd DATE;
	DECLARE cursorIncs CURSOR FOR 
		SELECT id , name, first_found_date, last_found_date
		FROM incarcerations
		WHERE (jail_id = p_jail_id)
		;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	OPEN cursorIncs;
	read_loop: LOOP
		FETCH cursorIncs INTO inc_id, inc_name, inc_ffd, inc_lfd;
		IF done THEN
			LEAVE read_loop;
		END IF;
		call compareIncarcerations(inc_id, p_jail_id, inc_name, inc_ffd, inc_lfd);
	END LOOP;
	CLOSE cursorIncs;
END//



DROP PROCEDURE IF EXISTS runIncChecks;
CREATE PROCEDURE runIncChecks()
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
		call checkIncarcerations(j_id);
	END LOOP;
	CLOSE cursorJails;
END//


-- -----------------------------------------------------------------------------



SELECT '-----------------------------------------------------' AS '';
SELECT concat('BEGIN: ', NOW()) AS '';
SELECT '-----------------------------------------------------' AS '';
-- call checkIncarcerations(1);
call runIncChecks();
SELECT '-----------------------------------------------------' AS '';
SELECT concat('END: ', NOW()) AS '';
SELECT '-----------------------------------------------------' AS '';





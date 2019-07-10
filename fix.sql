-- ---------------------------------------------------
use datatest;
-- ---------------------------------------------------

DELIMITER //

DROP PROCEDURE IF EXISTS updateJRDs;
CREATE PROCEDURE updateJRDs()
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE j_id INT;
	DECLARE cursorJRs CURSOR FOR 
		SELECT id
		FROM jail_records
		WHERE record_date IS NULL
		;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	OPEN cursorJRs;
	read_loop: LOOP
		FETCH cursorJRs INTO j_id;
		IF done THEN
			LEAVE read_loop;
		END IF;
		UPDATE jail_records set record_date = date(created_at) WHERE id = j_id;
	END LOOP;
	CLOSE cursorJRs;
END//

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
call updateJRDs();

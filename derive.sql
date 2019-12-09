DELIMITER //

CREATE PROCEDURE handleJailRecord(IN jr_id INT, IN j_id INT, IN p_name varchar(200), IN p_date DATE
	, IN p_dob DATE, IN p_age INT, IN p_sex VARCHAR(255), IN p_height INT, IN p_race VARCHAR(255)
	, IN p_confined_date VARCHAR(255), IN p_release_date VARCHAR(255)
	, IN p_non_court TINYINT, IN p_address VARCHAR(255)
	, IN p_days_in_jail INT, IN p_scrape_id INT
)
BEGIN

	DECLARE error_found INT DEFAULT FALSE;
	DECLARE done INT DEFAULT FALSE;
	DECLARE i_found BOOLEAN DEFAULT FALSE;
	DECLARE i_id INT;
	DECLARE i_name VARCHAR(200);
	DECLARE i_lfd VARCHAR(10);
	DECLARE i_ffd VARCHAR(10);
	DECLARE i_lastdate DATE ;

	DECLARE getIncs CURSOR FOR 
		SELECT id, first_found_date, last_found_date
		FROM incarcerations 
		WHERE (name = p_name) AND (jail_id = j_id)
			AND (
				(most_recent_age = p_age)
				OR (most_recent_age = (p_age + 1))
				OR (most_recent_age = (p_age - 1))
				OR (dob = p_dob)

				OR ((p_dob IS NULL) AND (p_age IS NULL))

			)
			AND (
				(p_date BETWEEN first_found_date AND last_found_date)
                OR
                (last_found_date = (p_date - INTERVAL 1 DAY))
                OR
                (first_found_date = (p_date + INTERVAL 1 DAY))
                OR
                (last_found_date > (p_date - INTERVAL 14 DAY) )
            )
		ORDER BY last_found_date DESC
		LIMIT 1
		;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	DECLARE CONTINUE HANDLER FOR 1366 SELECT 'Error inserting integer value.';
	-- DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET error_found = TRUE;

	-- try imputing / finding
	SET done = FALSE;
	OPEN getIncs;
	read_loop: LOOP
		FETCH getIncs INTO i_id, i_ffd, i_lfd;
		IF done THEN
			LEAVE read_loop;
		END IF;
		
		-- IF (i_ffd <= p_date) AND (i_lfd >= p_date) THEN
		IF (p_date BETWEEN i_ffd AND i_lfd) THEN
			SET i_found = TRUE;
			UPDATE jail_records SET incarceration_id = i_id WHERE id = jr_id;
			-- SELECT concat('UPDATE jail_records SET incarceration_id = ',i_id,' WHERE id = ',jr_id) AS '';

		ELSEIF (i_lfd = (p_date - INTERVAL 1 DAY)) THEN
			SET i_found = TRUE;
			UPDATE incarcerations SET last_found_date = p_date, most_recent_age = p_age, updated_at = NOW() WHERE (id = i_id);
             SELECT concat('UPDATE INC ',i_id,' SET last_found_date = ',p_date) as '';
			-- SELECT concat('UPDATE incarcerations SET last_found_date = ',p_date,' most_recent_age = ',p_age,' updated_at = ',NOW(),' WHERE id = ',i_id) AS '';
			UPDATE jail_records SET incarceration_id = i_id WHERE id = jr_id;
			-- SELECT concat('UPDATE jail_records SET incarceration_id = ',i_id,' WHERE id = ',jr_id) AS '';

		ELSEIF (i_ffd = (p_date + INTERVAL 1 DAY)) THEN
			SET i_found = TRUE;
			UPDATE incarcerations SET first_found_date = p_date, updated_at = NOW() WHERE (id = i_id);	-- don't update most recent age, since it's later
             SELECT concat('UPDATE INC ',i_id,' SET last_found_date = ',p_date) as '';
			-- SELECT concat('UPDATE incarcerations SET last_found_date = ',p_date,' most_recent_age = ',p_age,' updated_at = ',NOW(),' WHERE id = ',i_id) AS '';
			UPDATE jail_records SET incarceration_id = i_id WHERE id = jr_id;
			-- SELECT concat('UPDATE jail_records SET incarceration_id = ',i_id,' WHERE id = ',jr_id) AS '';

		ELSEIF 
			(i_lfd > (p_date - INTERVAL 14 DAY) )
				AND		
			(NOT EXISTS(
				SELECT id from jail_records
				WHERE (jail_id = j_id)
				AND (record_date between (i_lfd + INTERVAL 1 DAY) AND p_date)
				))
			THEN
				SET i_found = TRUE;
				UPDATE jail_records SET incarceration_id = i_id WHERE id = jr_id;
				 SELECT concat('UPDATE INC ',i_id,' SET last_found_date = ',p_date) as '';
				UPDATE incarcerations SET last_found_date = p_date, most_recent_age = p_age, updated_at = NOW() WHERE (id = i_id);
				-- SELECT concat('UPDATE INC', i_id) as '';
				-- SELECT concat('UPDATE incarcerations SET last_found_date = ',p_date,' most_recent_age = ',p_age,' updated_at = ',NOW(),' WHERE id = ',i_id) AS '';
				SET i_lastdate = i_lfd;
				each_loop: LOOP
					IF (i_lastdate is NULL) THEN
						LEAVE each_loop;
					END IF;
					SET i_lastdate = (i_lastdate + INTERVAL 1 DAY);
					IF (i_lastdate >= p_date) THEN
						LEAVE each_loop;
					END IF;
					INSERT INTO imputed_dates (incarceration_id, imputed_date) VALUES (i_id, i_lastdate);
					-- SELECT concat('    o----> IMPUTING: ', p_name, ' (date: ', i_lastdate, ')') as '';
				END LOOP;

		END IF;
	END LOOP;
	CLOSE getIncs;

	IF NOT i_found THEN
		INSERT INTO incarcerations (name, jail_id, first_found_date, last_found_date
			, dob, age, sex, height, race, confined_date, release_date
			, non_court, address, days_in_jail, most_recent_age, scrape_id, created_at, updated_at, jet_flag
		) VALUES (p_name, j_id, p_date, p_date
			, p_dob, p_age, p_sex, p_height, p_race, p_confined_date, p_release_date
			, p_non_court, p_address, p_days_in_jail, p_age, p_scrape_id, NOW(), NOW(), 1
		);
		UPDATE jail_records SET incarceration_id = LAST_INSERT_ID() WHERE id = jr_id;
		 SELECT concat('    +----> CREATING: ', LAST_INSERT_ID(), ': ', p_name, ' (first_found_date: ', p_date, ')', ' (last_found_date: ', p_date, ')', " | ", p_date) as '';
	END IF;
	
END//


CREATE PROCEDURE processJailRecords(IN mindate DATE, IN j_id INT)
BEGIN
	DECLARE error_found INT DEFAULT FALSE;
	DECLARE done INT DEFAULT FALSE;
	DECLARE r_id INT;

	DECLARE r_dob DATE;
	DECLARE r_age INT;
	DECLARE r_sex VARCHAR(255);
	DECLARE r_height INT;
	DECLARE r_race VARCHAR(255);

	DECLARE r_confined_date VARCHAR(255);
	DECLARE r_release_date VARCHAR(255);
	DECLARE r_non_court TINYINT;
	DECLARE r_address VARCHAR(255);
	DECLARE r_days_in_jail INT;
	DECLARE r_most_recent_age INT;
	DECLARE r_scrape_id INT;

	DECLARE r_name VARCHAR(200);
	DECLARE r_date DATE;
	DECLARE cursorRecords CURSOR FOR 
		SELECT id, name, record_date
			, dob, age, sex, height, race
			, confined_date, release_date, non_court, address, days_in_jail, scrape_id
		FROM jail_records
		WHERE (jail_id = j_id)
		AND (record_date >= mindate)
        AND (record_date is not null)        
		 -- AND (record_date > '2019-11-13')
		 -- AND (record_date < '2019-11-14')
         -- AND (name = 'SIMS,JORGE,QUIN') AND (jail_id = 25)
		AND (incarceration_id is null)
        AND (name is not null)
		order by name, record_date
		;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done= TRUE;
	DECLARE CONTINUE HANDLER FOR 1366 SELECT 'Error inserting integer value.';
	-- DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET error_found = TRUE;

	OPEN cursorRecords;
	read_loop: LOOP
		FETCH cursorRecords INTO r_id, r_name, r_date
			, r_dob, r_age, r_sex, r_height, r_race
			, r_confined_date, r_release_date, r_non_court, r_address, r_days_in_jail, r_scrape_id
			;
		IF done THEN
			LEAVE read_loop;
		END IF;
		-- SELECT concat('  Processing RECORD: ', r_name, ' (', r_date, ')') AS '';
		call handleJailRecord(r_id, j_id, r_name, r_date
				, r_dob, r_age, r_sex, r_height, r_race
				, r_confined_date, r_release_date, r_non_court, r_address, r_days_in_jail, r_scrape_id
			);
	END LOOP;
	CLOSE cursorRecords;
END//

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
		-- SELECT concat('Processing JAIL: ', j_county) AS '';
		call processJailRecords(mindate, j_id);
	END LOOP;
	CLOSE cursorJails;
END//







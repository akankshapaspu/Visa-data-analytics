DELIMITER //
CREATE PROCEDURE usp_employer_create()
BEGIN
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS employer;
CREATE TABLE employer (
employer_id int NOT NULL,
employer_name varchar(999),
employer_address_1 varchar(999),
employer_address_2 varchar(255),
geography_id int,
employer_num_employees int,
employer_phone varchar(20),
employer_phone_ext varchar(10),
employer_yr_estab int,
PRIMARY KEY (employer_id),
FOREIGN KEY (geography_id) REFERENCES geography(geography_id)
);
END //	
DELIMITER ;
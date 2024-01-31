DELIMITER //
CREATE PROCEDURE usp_geography_create()
BEGIN
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS job_info;
DROP TABLE IF EXISTS employer;
DROP TABLE IF EXISTS applicant;
DROP TABLE IF EXISTS geography;
CREATE TABLE IF NOT EXISTS geography (
geography_id int,
postal_code varchar(99) NOT NULL,
city varchar(99) NOT NULL,
state varchar(99) NOT NULL,
primary key(geography_id)
);
END //
DELIMITER ;
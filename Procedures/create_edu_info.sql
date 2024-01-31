DELIMITER //
CREATE PROCEDURE usp_edu_info_create()
BEGIN
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS job_info;
DROP TABLE IF EXISTS applicant;
DROP TABLE IF EXISTS education_info;
CREATE TABLE education_info (
education_id int NOT NULL,
education_info varchar(40),
education_info_other varchar(999),
PRIMARY KEY (education_id)
);
END //
DELIMITER ; 
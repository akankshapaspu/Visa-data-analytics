DELIMITER //
CREATE PROCEDURE usp_applicant_create()
BEGIN
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS applicant;
CREATE TABLE applicant (
applicant_id int NOT NULL,
applicant_birth_country varchar(50),
applicant_edu_id int,
applicant_major varchar(50),
applicant_institution varchar(100),
geography_id int,
applicant_graduation_year int,
PRIMARY KEY (applicant_id),
FOREIGN KEY (applicant_edu_id) REFERENCES education_info(education_id),
FOREIGN KEY (geography_id) REFERENCES geography(geography_id)
);
END //
DELIMITER ;
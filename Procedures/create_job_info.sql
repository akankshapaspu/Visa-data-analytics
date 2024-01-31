DELIMITER //
CREATE PROCEDURE usp_job_info_create()
BEGIN
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS job_info;
CREATE TABLE job_info (
job_info_id int NOT NULL,
job_title varchar(99),
education_id int,
experience_num_months int,
job_major varchar(99),
training_field varchar(999),
training_num_months int,
geography_id int,
alt_combined_yrs INT ,
alt_combo_edu varchar(999),
alt_field_name varchar(999),
alt_occupation varchar(99),
alt_occupation_job_title varchar (999),
alt_occupation_num_months int,
PRIMARY KEY (job_info_id),
FOREIGN KEY (education_id) REFERENCES education_info(education_id),
FOREIGN KEY (geography_id) REFERENCES geography(geography_id)
);
END //
DELIMITER ;
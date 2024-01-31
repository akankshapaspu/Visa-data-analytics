DELIMITER //
CREATE PROCEDURE usp_pw_info_create()
BEGIN
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS pw_info;
CREATE TABLE pw_info (
pw_id int,
pw_track_num varchar(99),
pw_amount float,
pw_determ_date date,
pw_expire_date date,
pw_job_title varchar(999),
pw_level varchar(50),
pw_soc_code varchar(50),
pw_source_name varchar(20),
pw_unit_of_pay varchar(30),
PRIMARY KEY (pw_id)
);
END //
DELIMITER ;
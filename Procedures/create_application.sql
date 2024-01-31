DELIMITER //
CREATE PROCEDURE usp_application_create()
BEGIN
DROP TABLE IF EXISTS application;
CREATE TABLE application (
application_case_number varchar(50),
applicant_id int,
case_received_date date,
case_status varchar(30),
class_of_admission varchar(10),
country_of_citizenship varchar(50),
decision_date date,
agent_id int,
employer_id int,
job_info_id int,
pw_id int,
preparer_info_title varchar(50),
us_economic_sector varchar(50),
wage_offer_from float,
wage_offer_to float,
wage_offer_unit_of_pay_9089 varchar(25),
PRIMARY KEY (application_case_number),
FOREIGN KEY (applicant_id) REFERENCES applicant(applicant_id),
FOREIGN KEY (agent_id) REFERENCES agent(agent_id),
FOREIGN KEY (employer_id) REFERENCES employer(employer_id),
FOREIGN KEY (job_info_id) REFERENCES job_info(job_info_id),
FOREIGN KEY (pw_id) REFERENCES pw_info(pw_id)
);
END //
DELIMITER ;
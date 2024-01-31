DELIMITER //
CREATE PROCEDURE usp_agent_create()
BEGIN
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS agent;
CREATE TABLE agent (
agent_id int NOT NULL,
agent_firm_name varchar(999),
agent_city varchar(100),
agent_state varchar(100),
PRIMARY KEY (agent_id)
);
END //
DELIMITER ;
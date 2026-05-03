#ALTER USER 'root'@'localhost' IDENTIFIED BY 'Mhtsshataasai123!@$';
#FLUSH PRIVILEGES;

DROP DATABASE IF EXISTS solarmonitor;

CREATE DATABASE solarmonitor;

USE solarmonitor;

SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));

CREATE TABLE node_info(
	node_id		VARCHAR(12)		NOT NULL,
    node_name	VARCHAR(255)	NOT NULL,
    node_long	VARCHAR(15)		NOT NULL,
    node_lat	VARCHAR(15)		NOT NULL,
    panel_isc	float4				NOT NULL,
    panel_voc	float4				NOT NULL,
    panel_imp	float4				NOT NULL,
    panel_vmp	float4				NOT NULL,
    battery_voltage float4				NOT NULL,
    battery_current	float4				NOT NULL,
    PRIMARY KEY (node_id));
    
INSERT INTO node_info VALUES
('11-03-1-2-01', 'ULN_BZD_22hor_HolbooniiSurguuli', '106.966060', '47.920654', 6.39, 22.92, 5.81, 18.92, 12, 80);

INSERT INTO node_info VALUES
('11-03-1-2-02', 'ULN_BZD_22hor_ZorigooBagshiinBair', '106.967052', '47.921006', 6.39, 22.92, 5.81, 18.92, 12, 80);
-- ════════════════════════════════════════════════════════════
--  node_info — 10 PV Monitoring Nodes around Ulaanbaatar
--  Spread across: Khan-Uul, Songinokhairkhan, Bayanzurkh,
--                 Chingeltei, Bayangol, Nalaikh, Gachuurt
--
--  Node ID format : 11-03-1-2-XX
--  Name format    : ULN_<DISTRICT>_<KHOROO>hor_<LOCATION>
--  Panel mix      : 110 W (Type-A) and 150 W (Type-B) modules
--  Battery        : 12 V system, 80–120 Ah
-- ════════════════════════════════════════════════════════════

-- 03 · Khan-Uul district · Zaisan memorial hill area (~5 km south)
INSERT INTO node_info VALUES
('11-03-1-2-03', 'ULN_KHU_32hor_ZaisanDenj',
 '106.904200', '47.872100',
 6.39, 22.92, 5.81, 18.92, 12, 80);

-- 04 · Songinokhairkhan district · Orgil sub-centre (~10 km west)
INSERT INTO node_info VALUES
('11-03-1-2-04', 'ULN_SGK_14hor_OrgiilKhoroolol',
 '106.691500', '47.931800',
 6.39, 22.92, 5.81, 18.92, 12, 100);

-- 05 · Bayanzurkh district · Narantuul market corridor (~8 km east)
INSERT INTO node_info VALUES
('11-03-1-2-05', 'ULN_BZR_20hor_NarantuulZakh',
 '107.058300', '47.919400',
 7.12, 25.60, 6.58, 21.40, 12, 80);

-- 06 · Chingeltei district · Gandan monastery ridge (~7 km north)
INSERT INTO node_info VALUES
('11-03-1-2-06', 'ULN_CHG_5hor_GandanHiidiin_Oroon',
 '106.908700', '47.967600',
 7.12, 25.60, 6.58, 21.40, 12, 80);

-- 07 · Bayangol district · Amgalan residential (~4 km south-west)
INSERT INTO node_info VALUES
('11-03-1-2-07', 'ULN_BYG_11hor_AmgalanTosghon',
 '106.842400', '47.893200',
 6.39, 22.92, 5.81, 18.92, 12, 80);

-- 08 · Nalaikh district · Nalaikh town centre (~28 km south-east)
INSERT INTO node_info VALUES
('11-03-1-2-08', 'ULN_NLH_1hor_NalaihTosghon',
 '107.308500', '47.762300',
 7.12, 25.60, 6.58, 21.40, 12, 120);

-- 09 · Bayanzurkh district · Gachuurt village (~18 km north-east)
INSERT INTO node_info VALUES
('11-03-1-2-09', 'ULN_BZR_26hor_GachuurtTosgon',
 '107.181200', '47.991400',
 6.39, 22.92, 5.81, 18.92, 12, 80);

-- 10 · Songinokhairkhan district · Üüliinkhan north (~12 km north-west)
INSERT INTO node_info VALUES
('11-03-1-2-10', 'ULN_SGK_23hor_UuliinkhaanKhoroolol',
 '106.741000', '47.974800',
 6.39, 22.92, 5.81, 18.92, 12, 100);

-- 11 · Khan-Uul district · Near Chinggis Khaan International Airport (~10 km west-south)
INSERT INTO node_info VALUES
('11-03-1-2-11', 'ULN_KHU_38hor_NisehBuudaliin_Oroon',
 '106.767400', '47.843100',
 7.12, 25.60, 6.58, 21.40, 12, 120);

-- 12 · Bayanzurkh district · Dunjingarav / eastern plateau (~13 km north-east)
INSERT INTO node_info VALUES
('11-03-1-2-12', 'ULN_BZR_29hor_DunjingaravTal',
 '107.108600', '47.950200',
 7.12, 25.60, 6.58, 21.40, 12, 80);
CREATE TABLE working_log (
	log_id		INT		 			auto_increment,
    log_node	INT						,
    log_sent_time		DATETIME		,
    log_inserted_time	DATETIME		,
    log_seq				INT 			,
    log_panel_voltage	VARCHAR(30)		,
    log_panel_current	VARCHAR(30)		,
    log_battery_voltage	VARCHAR(8)		,
    log_lux				FLOAT			DEFAULT NULL,
    log_cleaning_stat	SMALLINT		,
    log_cleaning_reason	VARCHAR(50)		,
    log_con_type		VARCHAR(10)		,
    
    PRIMARY KEY (log_id));

CREATE TABLE cmd_log (
	cmd_id		INT		 			auto_increment,
    cmd_sent	DATETIME,
    cmd_node	int,
    cmd_text	VARCHAR(100)	DEFAULT NULL,

    PRIMARY KEY (cmd_id));

CREATE TABLE IF NOT EXISTS node_last_proto (
  node_id    VARCHAR(12)  NOT NULL,
  proto      VARCHAR(10)  NOT NULL,          -- 'MQTT' or 'TCP'
  tcp_addr   VARCHAR(21)  DEFAULT NULL,      -- 'host:port' of active TCP socket
  last_seen  DATETIME     DEFAULT NULL,
  PRIMARY KEY (node_id),
  CONSTRAINT fk_nlp_node FOREIGN KEY (node_id)
    REFERENCES node_info(node_id) ON DELETE CASCADE
);
 
CREATE TABLE IF NOT EXISTS node_schedule (
  node_id     VARCHAR(12)  NOT NULL,
  slot        TINYINT      NOT NULL,          -- 0 or 1
  sched_time  TIME         NOT NULL,          -- HH:MM:00
  enabled     TINYINT(1)   NOT NULL DEFAULT 1,
  updated_at  DATETIME     DEFAULT NULL,
  PRIMARY KEY (node_id, slot),
  CONSTRAINT fk_ns_node FOREIGN KEY (node_id)
    REFERENCES node_info(node_id) ON DELETE CASCADE
);
 
INSERT IGNORE INTO node_schedule (node_id, slot, sched_time, enabled, updated_at)
  SELECT node_id, 0, '08:00:00', 0, NOW() FROM node_info;
 
INSERT IGNORE INTO node_schedule (node_id, slot, sched_time, enabled, updated_at)
  SELECT node_id, 1, '17:00:00', 0, NOW() FROM node_info;

ALTER TABLE working_log ADD COLUMN IF NOT EXISTS log_lux FLOAT DEFAULT NULL;

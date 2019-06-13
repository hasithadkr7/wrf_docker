drop DATABASE IF EXISTS `testdb`;
CREATE DATABASE `testdb`;
use `testdb`;

CREATE TABLE `variable` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `variable` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `type` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `type` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `type_UNIQUE` (`type` ASC)
);

CREATE TABLE `unit` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `unit` VARCHAR(10) NOT NULL,
  `type` ENUM('Accumulative', 'Instantaneous', 'Mean') NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `source` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `source` VARCHAR(45) NOT NULL,
  `parameters` JSON NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `source_UNIQUE` (`source` ASC)
);

CREATE TABLE `station` (
  `id` INT NOT NULL,
  `stationId` VARCHAR(45) NOT NULL,
  `name` VARCHAR(45) NOT NULL,
  `latitude` DOUBLE NOT NULL,
  `longitude` DOUBLE NOT NULL,
  `resolution` INT NOT NULL DEFAULT 0 COMMENT 'Resolution in meters. Default value is 0, and it means point data.',
  `description` VARCHAR(255) NULL,
  PRIMARY KEY (`id`, `stationId`),
  UNIQUE INDEX `stationId_UNIQUE` (`stationId` ASC),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC)
);

CREATE TABLE `run` (
  `id` VARCHAR(64) NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `start_date` DATETIME DEFAULT NULL,
  `end_date` DATETIME DEFAULT NULL,
  `station` INT NOT NULL,
  `variable` INT NOT NULL,
  `unit` INT NOT NULL,
  `type` INT NOT NULL,
  `source` INT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC),
  INDEX `station_idx` (`station` ASC),
  INDEX `variable_idx` (`variable` ASC),
  INDEX `unit_idx` (`unit` ASC),
  INDEX `type_idx` (`type` ASC),
  INDEX `source_idx` (`source` ASC),
  CONSTRAINT `station`
    FOREIGN KEY (`station`)
    REFERENCES `station` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `variable`
    FOREIGN KEY (`variable`)
    REFERENCES `variable` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `unit`
    FOREIGN KEY (`unit`)
    REFERENCES `unit` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `type`
    FOREIGN KEY (`type`)
    REFERENCES `type` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `source`
    FOREIGN KEY (`source`)
    REFERENCES `source` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE
);

CREATE TABLE `data` (
  `id` VARCHAR(64) NOT NULL,
  `time` DATETIME NOT NULL,
  `value` DECIMAL(8,3) NOT NULL,
  PRIMARY KEY (`id`, `time`),
  CONSTRAINT `id`
    FOREIGN KEY (`id`)
    REFERENCES `run` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

# Create Views
CREATE VIEW `run_view` AS
  SELECT 
    `run`.`id` AS `id`,
    `run`.`name` AS `name`,
    `run`.`start_date` AS `start_date`,
    `run`.`end_date` AS `end_date`,
    `station`.`name` AS `station`,
    `variable`.`variable` AS `variable`,
    `unit`.`unit` AS `unit`,
    `type`.`type` AS `type`,
    `source`.`source` AS `source`
  FROM
    (((((`run`
    JOIN `station` ON ((`run`.`station` = `station`.`id`)))
    JOIN `variable` ON ((`run`.`variable` = `variable`.`id`)))
    JOIN `unit` ON ((`run`.`unit` = `unit`.`id`)))
    JOIN `type` ON ((`run`.`type` = `type`.`id`)))
    JOIN `source` ON ((`run`.`source` = `source`.`id`)));



INSERT INTO variable VALUES
  (1, 'Precipitation'),
  (2, 'Discharge'),
  (3, 'Waterlevel'),
  (4, 'Waterdepth'),
  (5, 'Temperature');

INSERT INTO type VALUES
  (1, 'Observed'),
  (2, 'Forecast'), # Store CUrW continous timeseries data. E.g. 'Observed', 'Forecast'
  (3, 'Forecast-14-d-before'), # Store CUrW chunk timeseries data. E.g. 'Forecast-0-d', 'Forecast-1-d' etc
  (4, 'Forecast-13-d-before'),
  (5, 'Forecast-12-d-before'),
  (6, 'Forecast-11-d-before'),
  (7, 'Forecast-10-d-before'),
  (8, 'Forecast-9-d-before'),
  (9, 'Forecast-8-d-before'),
  (10, 'Forecast-7-d-before'),
  (11, 'Forecast-6-d-before'),
  (12, 'Forecast-5-d-before'),
  (13, 'Forecast-4-d-before'),
  (14, 'Forecast-3-d-before'),
  (15, 'Forecast-2-d-before'),
  (16, 'Forecast-1-d-before'),
  (17, 'Forecast-0-d'),
  (18, 'Forecast-1-d-after'),
  (19, 'Forecast-2-d-after'),
  (20, 'Forecast-3-d-after'),
  (21, 'Forecast-4-d-after'),
  (22, 'Forecast-5-d-after'),
  (23, 'Forecast-6-d-after'),
  (24, 'Forecast-7-d-after'),
  (25, 'Forecast-8-d-after'),
  (26, 'Forecast-9-d-after'),
  (27, 'Forecast-10-d-after'),
  (28, 'Forecast-11-d-after'),
  (29, 'Forecast-12-d-after'),
  (30, 'Forecast-13-d-after'),
  (31, 'Forecast-14-d-after');

INSERT INTO source VALUES
  (1, 'HEC-HMS', NULL),
  (2, 'SHER', NULL),
  (3, 'WRF', NULL),
  (4, 'FLO2D', NULL),
  (5, 'EPM', NULL),
  (6, 'WeatherStation', NULL),
  (7, 'WaterLevelGuage', NULL);

INSERT INTO unit (`id`, `unit`, `type`)
VALUES
  (1, 'mm', 'Accumulative'),      # Precipitation, Evaporation
  (2, 'm3/s', 'Instantaneous'),   # Discharge
  (3, 'm', 'Instantaneous'),      # Waterdepth, Waterlevel
  (4, 'm3', 'Instantaneous'),     # Storage
  (5, 'm/s', 'Instantaneous'),    # Wind speed
  (6, 'oC', 'Instantaneous'),     # Temperature
  (7, '%', 'Instantaneous'),      # Relative humidity
  (8, 'kPa', 'Instantaneous'),    # Vapour pressure
  (9, 'Pa', 'Instantaneous'),     # Pressure
  (10, 's', 'Instantaneous');     # Time

INSERT INTO station (`id`, `stationId`, `name`, `latitude`, `longitude`, `resolution`)
VALUES
  (100001, 'curw_attanagalla', 'Attanagalla', 7.111666667,  80.14983333, 0),
  (100002, 'curw_colombo', 'Colombo', 6.898158, 79.8653, 0),
  (100003, 'curw_daraniyagala', 'Daraniyagala', 6.924444444, 80.33805556, 0),
  (100004, 'curw_glencourse', 'Glencourse', 6.978055556, 80.20305556, 0),
  (100005, 'curw_hanwella', 'Hanwella', 6.909722222, 80.08166667, 0),
  (100006, 'curw_holombuwa', 'Holombuwa', 7.185166667, 80.26480556, 0),
  (100007, 'curw_kitulgala', 'Kitulgala', 6.989166667, 80.41777778, 0),
  (100008, 'curw_norwood', 'Norwood', 6.835638889, 80.61466667, 0),
  (100009, 'curw_kalutara', 'Kalutara', 6.6, 79.95, 0),
  (100010, 'curw_kalawana', 'Kalawana', 6.54, 80.38, 0),
  (100011, 'curw_ratnapura', 'Ratnapura', 6.72, 80.38, 0),
  (100012, 'curw_kahawatta', 'Kahawatta', 6.6, 80.58, 0),
  (100013, 'curw_dodampe', 'Dodampe', 6.72712, 80.3274, 0);

# Insert Waterlevel Extraction point locations
INSERT INTO station (`id`, `stationId`, `name`, `latitude`, `longitude`, `resolution`)
VALUES
  (1200001, "sim_flo2d_n'street_river", "N'Street-River", 79.877339451000068, 6.959254587000032, 0),
  (1200002, "sim_flo2d_n'street_canal", "N'Street-Canal", 79.877348052000059, 6.954733548000036, 0),
  (1200003, 'sim_flo2d_wellawatta', 'Wellawatta', 79.861655902000052, 6.880106281000053, 0),
  (1200004, 'sim_flo2d_dematagoda_canal', 'Dematagoda-Canal', 79.879631729000039, 6.943435232000070, 0),
  (1200005, 'sim_flo2d_dehiwala', 'Dehiwala', 79.863636663000079, 6.863283492000051, 0),
  (1200006, 'sim_flo2d_parliament_lake_bridge_kotte_canal', 'Parliament Lake Bridge-Kotte Canal', 79.902332838000063, 6.900527256000032, 0),
  (1200007, 'sim_flo2d_parliament_lake_out', 'Parliament Lake-Out', 79.915921237000077, 6.891509735000057, 0),
  (1200008, 'sim_flo2d_madiwela_us', 'Madiwela-US', 79.926958523000053, 7.042986704000043, 0),
  (1200009, 'sim_flo2d_ambathale', 'Ambathale', 79.947506252000039, 6.939037595000059, 0),
  (1200010, 'sim_flo2d_madiwela_out', 'Madiwela-Out', 79.947510206000061, 6.936777033000055, 0),
  (1200011, 'sim_flo2d_salalihini_river', 'Salalihini-River', 79.918081126000061, 6.948027491000062, 0),
  (1200012, 'sim_flo2d_salalihini_canal', 'Salalihini-Canal', 79.920347460000073, 6.945771037000043, 0),
  (1200013, 'sim_flo2d_kittampahuwa_river', 'Kittampahuwa-River', 79.890921590000062, 6.954759129000024, 0),
  (1200014, 'sim_flo2d_kittampahuwa_out', 'Kittampahuwa-Out', 79.890925824000078, 6.952498601000059, 0),
  (1200015, 'sim_flo2d_kolonnawa_canal', 'Kolonnawa-Canal', 79.890980733000049, 6.923111719000076, 0),
  (1200016, 'sim_flo2d_heen_ela', 'Heen Ela', 79.884245272000044, 6.895972700000073, 0),
  (1200017, 'sim_flo2d_torington', 'Torington', 79.877450821000082, 6.900481020000029, 0),
  (1200018, 'sim_flo2d_parliament_lake', 'Parliament Lake', 79.918183212000031, 6.891513804000056, 0);



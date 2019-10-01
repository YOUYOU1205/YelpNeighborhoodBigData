CREATE TABLE public.business_agg 
(
  city              VARCHAR(50),
  categories        TEXT,
  state             TEXT,
  postal_code       TEXT,
  longitude         FLOAT8,
  latitude          FLOAT8,
  AverageReview     FLOAT8,
  AverageStars      FLOAT8,
  TotalReview       BIGINT,
  BusinessCount     BIGINT
);


-- ----------------------------
--  Table structure for `restaurant_establishments` , source: NV restaurant inspection
-- ----------------------------
CREATE TABLE IF NOT EXISTS `restaurant_establishments` (
  `permit_number` char(9) COLLATE utf8_unicode_ci NOT NULL,
  `facility_id` char(9) COLLATE utf8_unicode_ci NOT NULL,
  `PE` int(11) NOT NULL,
  `restaurant_name` varchar(75) COLLATE utf8_unicode_ci DEFAULT NULL,
  `location_name` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,
  `search_text` varchar(300) COLLATE utf8_unicode_ci NOT NULL,
  `address` varchar(35) COLLATE utf8_unicode_ci DEFAULT NULL,
  `latitude` decimal(12,8) DEFAULT NULL,
  `longitude` decimal(12,8) DEFAULT NULL,
  `city_id` int(11) NOT NULL DEFAULT '0',
  `city_name` varchar(35) COLLATE utf8_unicode_ci NOT NULL,
  `zip_code` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
  `nciaa` char(1) COLLATE utf8_unicode_ci DEFAULT NULL,
  `plan_review` char(4) COLLATE utf8_unicode_ci DEFAULT NULL,
  `record_status` char(2) COLLATE utf8_unicode_ci DEFAULT NULL,
  `current_grade` char(2) COLLATE utf8_unicode_ci DEFAULT NULL,
  `current_demerits` int(11) DEFAULT NULL,
  `date_current` datetime DEFAULT NULL,
  `previous_grade` char(1) COLLATE utf8_unicode_ci DEFAULT NULL,
  `date_previous` datetime DEFAULT NULL,
  PRIMARY KEY (`permit_number`),
  UNIQUE KEY `permit_number` (`permit_number`),
  KEY `restaurant_name` (`restaurant_name`),
  KEY `location_name` (`location_name`),
  KEY `address` (`address`),
  KEY `city_id` (`city_id`),
  KEY `category_id` (`PE`),
  KEY `zip_code` (`zip_code`),
  KEY `record_status` (`record_status`),
  KEY `current_grade` (`current_grade`),
  KEY `current_demerits` (`current_demerits`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

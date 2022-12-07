CREATE OR REPLACE PROCEDURE `idot-crash-demo.IDOT_Crash_Analysis.sp_create_load_source_tables`()
BEGIN

--create the empty crash table, and replace it if it already exists!
CREATE OR REPLACE TABLE IDOT_Crash_Analysis.crash
(
  index INT64,
  ICN INT64,
  CrashID INT64,
  CrashYr INT64,
  CrashMonth STRING,
  CrashDay INT64,
  NumberOfVehicles INT64,
  CrashHour STRING,
  Township STRING,
  TotalFatals FLOAT64,
  TotalInjured FLOAT64,
  NoInjuries FLOAT64,
  AInjuries FLOAT64,
  BInjuries FLOAT64,
  CInjuries FLOAT64,
  CrashSeverity STRING,
  RouteNumber FLOAT64,
  RailroadCrossingNumber STRING,
  TimeOfCrash STRING,
  IntersectionRelated BOOL,
  HitAndRun BOOL,
  CrashDate DATE,
  NumberOfLanes STRING,
  WorkZoneRelated BOOL,
  City_Township_Flag STRING,
  TSCrashLatitude FLOAT64,
  TSCrashLongitude FLOAT64,
  CrashReportCounty STRING,
  DayOfWeek STRING,
  TypeOfFirstCrash STRING,
  CityName STRING,
  CityClass STRING,
  ClassOfTrafficway STRING,
  Cause1 STRING,
  Cause2 STRING,
  TrafficControlDevice STRING,
  TrafficControlDeviceCond STRING,
  RoadSurfaceCond STRING,
  RoadDefects STRING,
  CrashInjurySeverity STRING,
  LightingCond STRING,
  WeatherCond STRING,
  RoadAlignment STRING,
  TrafficwayDescrip STRING,
  RoadwayFunctionalClass STRING,
  WorkZoneType STRING,
  WereWorkersPresent BOOL,
  AccessControl STRING,
  FlowCondition STRING,
  DidInvolveSecondaryCrash STRING,
  UrbanRural STRING,
  Toll STRING
);

--load the crash data table from files in GCS

LOAD DATA INTO IDOT_Crash_Analysis.crash
  FROM FILES(
    skip_leading_rows=1,
    format='CSV',
    uris = ['gs://idot-crash-demo-source-data/crash*.csv']
  );

--create the empty vehicle table, and replace it if it already exists!
CREATE OR REPLACE TABLE IDOT_Crash_Analysis.vehicle
(
  ICN INT64,
  CrashID INT64,
  CrashReportUnitNbr INT64,
  NbrOccupants STRING,
  IsTowed BOOL,
  IsFire BOOL,
  IsHazMatSpill BOOL,
  IsCommercial BOOL,
  EventMostSevereCode FLOAT64,
  EventMostSevereLocCode FLOAT64,
  CrashEvent2Code FLOAT64,
  VehDefects STRING,
  VehManeuverPrior STRING,
  VehType STRING,
  VehUse STRING,
  EventMostSevere STRING,
  EventMostSevereLoc STRING,
  CrashEvent1 STRING,
  CrashEvent2 STRING,
  CrashEvent3 STRING,
  Event1Loc STRING,
  Event2Loc STRING,
  Event3Loc STRING,
  VehYear STRING,
  VehMake STRING,
  VehModel STRING,
  ExceedingSpeedLimit BOOL,
  LevelOfAutomationDuringCrash STRING,
  ExtentOfDamage STRING,
  SpeedingRelated STRING
);

--load the vehicle data table from files in GCS

  LOAD DATA INTO IDOT_Crash_Analysis.vehicle
  FROM FILES(
    skip_leading_rows=1,
    format='CSV',
    uris = ['gs://idot-crash-demo-source-data/vehicle*.csv']
  );


--create the empty person table, and replace it if it already exists!
CREATE OR REPLACE TABLE IDOT_Crash_Analysis.person
(
  ICN INT64,
  CrashID INT64,
  UnitNo INT64,
  AgeAtCrash INT64,
  Gender STRING,
  StateProvinceCode STRING,
  DRAC INT64,
  PersonInjuryClass FLOAT64,
  SAFT FLOAT64,
  PedBikeVisibility STRING,
  ApparentPhysCond STRING,
  AirBagDeployment STRING,
  PedBikeAction STRING,
  BACTestGiven STRING,
  EjectExtricate STRING,
  DriverAction STRING,
  PedBikeLocation STRING,
  DriverVision STRING,
  SafetyEquipUsed STRING,
  DistractionReason STRING,
  IncidentResponder STRING,
  EjectionPath STRING
);

--load the person data table from files in GCS
  LOAD DATA INTO IDOT_Crash_Analysis.person
  FROM FILES(
    skip_leading_rows=1,
    format='CSV',
    uris = ['gs://idot-crash-demo-source-data/person*.csv']
  ); 

  END;
  
  CREATE OR REPLACE PROCEDURE `idot-crash-demo.IDOT_Crash_Analysis.sp_create_views_and_tables`()
BEGIN

# Create view that denormalizes the data
  # one record per crash with nested arrays for the related vehicle and persons records
  #
CREATE OR REPLACE VIEW
  IDOT_Crash_Analysis.crashdata_v AS
SELECT
  INDEX,
  ICN,
  CrashID,
  -- create a LOCAL timestamp data type value for each record by combining values from multiple columns
  -- for partitioning, as well as filtering and display in Data Studio which does not handle UTC and localization
  PARSE_TIMESTAMP('%F %l:%M %p',CONCAT(STRING(crash.CrashDate),' ',REPLACE(crash.TimeOfCrash,'00:','12:'))) CrashTimestamp,
  -- create a UTC timestamp data type value for each record by combining values from multiple columns
  PARSE_TIMESTAMP('%F %l:%M %p',CONCAT(STRING(crash.CrashDate),' ',REPLACE(crash.TimeOfCrash,'00:','12:')),'America/Chicago') CrashTimestampUTC,
  CrashYr,
  CrashMonth,
  CrashDay,
  NumberOfVehicles,
  CrashHour,
  Township,
  TotalFatals,
  TotalInjured,
  NoInjuries,
  AInjuries,
  BInjuries,
  CInjuries,
  CrashSeverity,
  CASE
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='1' AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'US Route'
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='2'
  AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'Interstate Business Loop'
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='3' AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'Business US Route'
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='4'
  AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'By pass and US one-way couple'
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='5' AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'Illinois Route'
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='6'
  AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'Illinois one-way couple'
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='7' AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'Interstate Business Loop one-way couples'
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='8'
  AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'Nonmarked Route'
    WHEN LEFT(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),1)='9' AND LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN 'Interstate'
  ELSE
  'UNKOWN'
END
  AS RouteType,
  CASE
    WHEN LENGTH(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')))=4 THEN CAST(SAFE_CAST(SUBSTR(TRIM(CAST(RouteNumber AS STRING FORMAT '9999')),2) AS NUMERIC) AS STRING format '999')
  ELSE
  CAST(RouteNumber AS STRING FORMAT '9999')
END
  AS RouteNumber,
  RailroadCrossingNumber,
  --they use 00: for 12 am but the rest is 12 hour clock, standardizing
  REPLACE(crash.TimeOfCrash,'00:','12:') TimeOfCrash,
  IntersectionRelated,
  HitAndRun,
  CrashDate,
  NumberOfLanes,
  WorkZoneRelated,
  City_Township_Flag,
  TSCrashLatitude TSCrashLatitude,
  --some bad longitude records missing negative sign
  CASE
    WHEN TSCrashLongitude>0 THEN TSCrashLongitude * -1
  ELSE
  TSCrashLongitude
END
  AS TSCrashLongitude,
  -- create a geometry data type value for each record by combining lat and long values from 2 columns
  ST_GEOGPOINT(CASE
      WHEN TSCrashLongitude>0 THEN TSCrashLongitude * -1
    ELSE
    TSCrashLongitude
  END
    , crash.TSCrashLatitude) AS crash_geo,
  CrashReportCounty,
  DayOfWeek,
  TypeOfFirstCrash,
  CityName,
  CityClass,
  CASE ClassOfTrafficway
    WHEN '0' THEN 'Unmarked Highway rural'
    WHEN '1' THEN 'Controlled rural'
    WHEN '2' THEN 'State numbered rural'
    WHEN '3' THEN 'County and local roads rural'
    WHEN '4' THEN 'Toll roads rural'
    WHEN '5' THEN 'Controlled urban'
    WHEN '6' THEN 'State numbered urban'
    WHEN '7' THEN 'Unmarked highway urban'
    WHEN '8' THEN 'City streets urban'
    WHEN '9' THEN 'Toll roads urban'
  ELSE
  'UNKOWN'
END
  ClassOfTrafficway,
  Cause1,
  Cause2,
  TrafficControlDevice,
  TrafficControlDeviceCond,
  RoadSurfaceCond,
  RoadDefects,
  CrashInjurySeverity,
  LightingCond,
  WeatherCond,
  RoadAlignment,
  TrafficwayDescrip,
  RoadwayFunctionalClass,
  WorkZoneType,
  WereWorkersPresent,
  AccessControl,
  FlowCondition,
  DidInvolveSecondaryCrash,
  UrbanRural,
  Toll,
  -- create nested array of related persons table records
  ARRAY(
  SELECT
    AS STRUCT ICN, CrashID, UnitNo,
    CASE UnitNo
      WHEN 1 THEN 'DRIVER'
      WHEN 2 THEN 'PEDESTRIAN'
      WHEN 3 THEN 'PEDALCYCLIST'
      WHEN 4 THEN 'EQUESTRIAN'
      WHEN 5 THEN 'OCCUPANT OF NON MOTORIZED VEHICLE'
      WHEN 6 THEN 'NONCONTACT VEHICLE'
      WHEN 7 THEN 'PASSENGER'
      WHEN 8 THEN 'DISABLED VEHICLE'
    ELSE
    'UNKOWN'
  END
    AS PersonType, AgeAtCrash, Gender, StateProvinceCode, DRAC, PersonInjuryClass, SAFT, PedBikeVisibility, ApparentPhysCond, AirBagDeployment, PedBikeAction, BACTestGiven, EjectExtricate, DriverAction, PedBikeLocation, DriverVision, SafetyEquipUsed, DistractionReason, IncidentResponder, EjectionPath
  FROM
    IDOT_Crash_Analysis.person
  WHERE
    CrashID=crash.CrashID) AS person,
  -- create nested array of related vehicle table records
  ARRAY(
  SELECT
    AS STRUCT *
  FROM
    IDOT_Crash_Analysis.vehicle
  WHERE
    CrashID=crash.CrashID) AS vehicle
FROM
  IDOT_Crash_Analysis.crash crash;
  #
  # Materialize the results of the view into a table
  # Partition the table by crashtimestamp, monthly
  #

CREATE OR REPLACE TABLE
  IDOT_Crash_Analysis.crashdata
PARTITION BY
  TIMESTAMP_TRUNC(CrashTimestamp, MONTH) AS (
  SELECT
    *
  FROM
    IDOT_Crash_Analysis.crashdata_v);

  #######################################################
  # Clustering of 3 years of data #
  # identify all potentially related crashe records based
  # on distance and time difference.
  # distance is within 500 feet
  # time difference is + or minus 5 minutes
  #######################################################
  
create or replace view IDOT_Crash_Analysis.crashdata_clustering_v
as
SELECT
  crashes.crashID,
  crashes.crashtimestamp,
  crashes.crash_geo,
  related_crashes.crashID related_crashid,
  related_crashes.crashtimestamp related_crashtimestamp,
  related_crashes.crash_geo related_crash_geo,
  ABS(TIMESTAMP_DIFF(crashes.crashtimestamp,related_crashes.crashtimestamp, MINUTE)) AS time_diff_minutes,
  ST_DISTANCE(crashes.crash_geo,related_crashes.crash_geo)*3.28 AS distance_feet
FROM
  IDOT_Crash_Analysis.crashdata crashes,
  IDOT_Crash_Analysis.crashdata related_crashes
WHERE
  crashes.crashID<>related_crashes.crashid
  AND crashes.CrashTimestamp BETWEEN "2019-01-01"
  AND "2021-12-31"
  AND related_crashes.CrashTimestamp BETWEEN "2019-01-01"
  AND "2021-12-31"
  AND related_crashes.crashtimestamp BETWEEN TIMESTAMP_SUB(crashes.crashtimestamp, INTERVAL 5 MINUTE)
  AND TIMESTAMP_ADD(crashes.crashtimestamp, INTERVAL 5 MINUTE)
  AND crashes.crash_geo IS NOT NULL
  AND related_crashes.crash_geo IS NOT NULL
  AND ST_DWITHIN(crashes.crash_geo, related_crashes.crash_geo, 500/3.28)
ORDER BY
  crashtimestamp DESC,
  time_diff_minutes ASC;

  create or replace table IDOT_Crash_Analysis.crashdata_clustering
  as select * from IDOT_Crash_Analysis.crashdata_clustering_v;

  END;
  
  CREATE OR REPLACE PROCEDURE `idot-crash-demo.IDOT_Crash_Analysis.sp_demo_queries`()
BEGIN
 
  #######################################################
  # Total Records #
  #######################################################
SELECT
  COUNT(*) total_crash_records
FROM
  IDOT_Crash_Analysis.crashdata;
  #
  #
  #
  #######################################################
  # Min and Max Date  #
  #######################################################
SELECT
  min (CrashDate) min_date,
  max (CrashDate) max_date
FROM
  IDOT_Crash_Analysis.crashdata;
  #######################################################
  # All records from 2019 #
  #######################################################
SELECT
  *
FROM
  IDOT_Crash_Analysis.crashdata
WHERE
  CrashTimestamp BETWEEN "2021-08-01"
  AND "2021-09-01"
LIMIT
  1000;
  #######################################################
  # Clustering of 3 years of data #
  # identify all potentially related crashe records based
  # on distance and time difference.
  # distance is within 500 feet
  # time difference is + or minus 5 minutes
  #######################################################
  

# query the view
  select * from IDOT_Crash_Analysis.crashdata_clustering_v;
  #
  #
  #######################################################
  # Geo formats #
  #######################################################
SELECT
  crashID,
  crashtimestamp,
  crash_geo geo_geog_data_type,
  ST_ASTEXT(crash_geo) geo_as_text,
  ST_ASGEOJSON(crash_geo) geo_as_geojson,
  ST_ASBINARY(crash_geo) geo_as_binary,
  ST_GEOHASH(crash_geo) geo_as_geohash
FROM
  IDOT_Crash_Analysis.crashdata
LIMIT
  10;

  END;

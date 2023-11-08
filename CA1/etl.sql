-- Connect to the source MariaDB database
USE MusicCompDB;

-- Drop the temporary table
DROP TEMPORARY TABLE IF EXISTS MusicCompDB.temp_votes;

-- Extract data from the source VOTES table to temporary staging table
CREATE TEMPORARY TABLE temp_votes AS
SELECT *
FROM VOTES;

-- Transform staging table by adding AGE_GROUPID and COUNTYID columns
ALTER TABLE temp_votes
ADD COLUMN AGE_GROUPID INT, 
ADD COLUMN COUNTYID INT,
ADD COLUMN VOTE_COST DECIMAL(10, 2) DEFAULT NULL;

-- Update the staging table with the correct values for the new columns using subqueries
UPDATE temp_votes
SET AGE_GROUPID = (SELECT AGE_GROUP FROM VIEWERS WHERE VIEWERS.VIEWERID = temp_votes.VIEWERID),
COUNTYID = (SELECT COUNTYID FROM VIEWERS WHERE VIEWERS.VIEWERID = temp_votes.VIEWERID),
VOTE_COST =
  CASE
    WHEN VOTE_CATEGORY = 1 THEN 0 -- Jury voters are not charged
    WHEN EDITION_YEAR BETWEEN 2013 AND 2015 THEN
      CASE
        WHEN VOTEMODE IN ('Facebook', 'Instagram') THEN 0.20
        WHEN VOTEMODE IN ('TV', 'Phone') THEN 0.50
      END
    WHEN EDITION_YEAR BETWEEN 2016 AND 2022 THEN
      CASE
        WHEN VOTEMODE IN ('Facebook', 'Instagram') THEN 0.50
        WHEN VOTEMODE IN ('TV', 'Phone') THEN 1.00
      END
  END;


-- Connect to the destination MariaDB database
USE MusicCompDB_DIM;


-- Populate AGEGROUP_DIM
INSERT INTO AGEGROUP_DIM (AGE_GROUPID, AGE_GROUP_DESC)
SELECT DISTINCT AGE_GROUPID, AGE_GROUP_DESC
FROM MusicCompDB.AGEGROUP;

-- Populate COUNTY_DIM
INSERT INTO COUNTY_DIM (COUNTYID, COUNTYNAME)
SELECT DISTINCT COUNTYID, COUNTYNAME
FROM MusicCompDB.COUNTY;

-- Populate EDITION_DIM
INSERT INTO EDITION_DIM (EDYEAR, EDPRESENTER)
SELECT DISTINCT EDYEAR, EDPRESENTER
FROM MusicCompDB.EDITION;

-- Populate PARTICIPANTS_DIM
INSERT INTO PARTICIPANTS_DIM (PARTNAME, COUNTYID)
SELECT DISTINCT PARTNAME, COUNTYID
FROM MusicCompDB.PARTICIPANTS;

-- Populate VIEWERS_DIM
INSERT INTO VIEWERS_DIM (VIEWERID, AGE_GROUPID, COUNTYID)
SELECT DISTINCT VIEWERID, AGE_GROUP, COUNTYID
FROM MusicCompDB.VIEWERS;

-- Populate VIEWERCATEGORY_DIM
INSERT INTO VIEWERCATEGORY_DIM (CATID, CATNAME)
SELECT DISTINCT CATID, CATNAME
FROM MusicCompDB.VIEWERCATEGORY;

-- Load data into the destination VOTES_FACT table
INSERT INTO VOTES_FACT (VIEWERID, AGE_GROUPID, COUNTYID, EDYEAR, PARTNAME, VOTE_CATEGORY, VOTEMODE, VOTE, VOTE_COST)
SELECT VIEWERID, AGE_GROUPID, COUNTYID, EDITION_YEAR, PARTNAME, VOTE_CATEGORY, VOTEMODE, VOTE, VOTE_COST
FROM MusicCompDB.temp_votes;


-- Drop the temporary table
DROP TEMPORARY TABLE IF EXISTS MusicCompDB.temp_votes;

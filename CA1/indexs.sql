-- Question 1
-- Index on V.AGE_GROUPID
-- This index will speed up the join operation between VOTES_FACT and AGEGROUP_DIM on the AGE_GROUPID column
CREATE INDEX idx_age_groupid ON VOTES_FACT (AGE_GROUPID);

-- Index on V.COUNTYID
-- This index will speed up the join operation between VOTES_FACT and COUNTY_DIM on the COUNTYID column
CREATE INDEX idx_countyid ON VOTES_FACT (COUNTYID);

-- Covering index including VOTE_COST on (EDYEAR, AGE_GROUPID, COUNTYID, VOTE, VOTE_COST)
-- This index will speed up the GROUP BY operation on the VOTES_FACT table and the SUM operation on the VOTE column
CREATE INDEX idx_covering_q1 ON VOTES_FACT (EDYEAR, AGE_GROUPID, COUNTYID, VOTE, VOTE_COST);


-- Question 2
-- Index on V.PARTNAME
-- This index will speed up the join operation between VOTES_FACT and PARTICIPANTS_DIM on the PARTNAME column
CREATE INDEX idx_partname ON VOTES_FACT (PARTNAME);

-- Index on P.COUNTYID
-- This index will speed up the join operation between PARTICIPANTS_DIM and COUNTY_DIM on the COUNTYID column
CREATE INDEX idx_countyid_q2 ON PARTICIPANTS_DIM (COUNTYID);

-- Covering index including VOTE_COST on (EDYEAR, VOTE_CATEGORY, COUNTYID, PARTNAME, VOTE, VOTE_COST)
-- This index will speed up the GROUP BY operation on the VOTES_FACT table and the SUM operation on the VOTE column
CREATE INDEX idx_covering_q2 ON VOTES_FACT (EDYEAR, VOTE_CATEGORY, COUNTYID, PARTNAME, VOTE, VOTE_COST);


-- Question 3
-- Index on V.COUNTYID
-- This index will speed up the join operation between VOTES_FACT and COUNTY_DIM on the COUNTYID column
CREATE INDEX idx_countyid_q3 ON VOTES_FACT (COUNTYID);

-- Covering index including VOTE_COST on (EDYEAR, VOTEMODE, VOTE_CATEGORY, COUNTYID, VOTE, VOTE_COST)
-- This index will speed up the GROUP BY operation on the VOTES_FACT table and the SUM operation on the VOTE column
CREATE INDEX idx_covering_q3 ON VOTES_FACT (EDYEAR, VOTEMODE, VOTE_CATEGORY, COUNTYID, VOTE, VOTE_COST);

-- Question 1
-- Drop index on V.AGE_GROUPID
DROP INDEX idx_age_groupid ON VOTES_FACT;

-- Drop index on V.COUNTYID
DROP INDEX idx_countyid ON VOTES_FACT;

-- Drop covering index on (EDYEAR, AGE_GROUPID, COUNTYID, VOTE)
DROP INDEX idx_covering_q1 ON VOTES_FACT;


-- Question 2
-- Drop index on V.PARTNAME
DROP INDEX idx_partname ON VOTES_FACT;

-- Drop index on P.COUNTYID
DROP INDEX idx_countyid_q2 ON PARTICIPANTS_DIM;

-- Drop covering index on (EDYEAR, VOTE_CATEGORY, COUNTYID, PARTNAME, VOTE)
DROP INDEX idx_covering_q2 ON VOTES_FACT;


-- Question 3
-- Drop index on V.COUNTYID
DROP INDEX idx_countyid_q3 ON VOTES_FACT;

-- Drop covering index on (EDYEAR, VOTEMODE, VOTE_CATEGORY, COUNTYID, VOTE)
DROP INDEX idx_covering_q3 ON VOTES_FACT;

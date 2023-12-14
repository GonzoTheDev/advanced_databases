CREATE KEYSPACE c20703429 WITH replication = {'class':'SimpleStrategy' , 'replication_factor' : 2};
USE c20703429;
CREATE TABLE IF NOT EXISTS c20703429.votes_fact (
        vote_id int PRIMARY KEY,
        viewerid int,
        age_groupid int,
        countyid int,
        edyear int,
        partname varchar,
        vote_category int,
        votemode varchar,
        vote int,
        vote_cost decimal
    );

tracing on;
Select * from votes_fact where edyear=2020 ALLOW FILTERING;
tracing off;

CREATE INDEX edyear on votes_fact(edyear);

tracing on;
Select * from votes_fact where edyear=2020;
tracing off;

CREATE TABLE IF NOT EXISTS c20703429.votes_by_viewer (
    viewerid int PRIMARY KEY,
    vote_ids list<int>
);

Select * from votes_by_viewer;

CREATE TABLE IF NOT EXISTS c20703429.votes_by_viewer (
    viewerid int PRIMARY KEY,
    vote_ids FROZEN<list<int>>
);
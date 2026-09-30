-- Seeds the load test's data directly in Postgres: users' follow graph and their feed history.
-- Minutes, instead of hours through the API. Replaces everything in the database.
--
--   psql -v users=100000 -v skew=2.13 -v posts_per_user=5 -f seed.sql   (loadtest/setup.sh runs it)
--
-- Users are ids u000000, u000001, ... (there's no users table). The same ids get tokens from
-- `devtools tokens`, and k6 picks whom to follow the same way as below.

\timing on

BEGIN;
SET LOCAL synchronous_commit = off;  -- a seed can be redone: don't wait for the disk on commit

TRUNCATE feed_items, follows, posts, outbox, posting_buckets;

-- 1. Who follows whom. Each user follows 10 to 90 others (50 on average). Which ones: user
-- number floor(users * random()^skew). Low numbers come up far more often, so follower counts
-- follow a power law, like a real network: with 100,000 users and skew 2.13, u000000 is
-- followed by about 20% of everyone, the median user by about 35.
INSERT INTO follows (follower_id, followee_id, created_at)
SELECT 'u' || lpad(f::text, 6, '0'), 'u' || lpad(t::text, 6, '0'), now() - interval '60 days'
FROM generate_series(0, :users - 1) AS f
CROSS JOIN LATERAL (
    -- "+ f * 0" ties the count to each user, so it's drawn again for every one of them
    SELECT floor(:users * power(random(), :skew))::int AS t
    FROM generate_series(1, 10 + floor(random() * 81)::int + f * 0)
) AS picks
WHERE t <> f
ON CONFLICT DO NOTHING;

-- 2. History: every user posted posts_per_user times over the last 30 days.
INSERT INTO posts (id, author_id, text, created_at)
SELECT gen_random_uuid(), 'u' || lpad(a::text, 6, '0'), 'seeded post ' || n, now() - random() * interval '30 days'
FROM generate_series(0, :users - 1) AS a, generate_series(1, :posts_per_user) AS n;

-- 3. Feeds: each post in every follower's feed, as the fanout would have put it there.
INSERT INTO feed_items (user_id, post_id, created_at)
SELECT f.follower_id, p.id, p.created_at
FROM posts p
JOIN follows f ON f.followee_id = p.author_id;

COMMIT;

-- Fresh statistics for the planner, and a visibility map so index-only scans work.
VACUUM ANALYZE follows;
VACUUM ANALYZE posts;
VACUUM ANALYZE feed_items;

\timing off

-- What was made. The data should be several times the database's memory limit (1.5 GB).
SELECT
    (SELECT count(*) FROM follows) AS follows,
    (SELECT count(*) FROM posts) AS posts,
    (SELECT count(*) FROM feed_items) AS feed_items,
    pg_size_pretty(pg_database_size(current_database())) AS database_size;

SELECT
    max(followers) AS most_followers,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY followers) AS median_followers,
    percentile_disc(0.99) WITHIN GROUP (ORDER BY followers) AS p99_followers
FROM (SELECT count(*) AS followers FROM follows GROUP BY followee_id) AS counts;

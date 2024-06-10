 # Handling Gaps and Islands Problems
 # Problem: Identify groups of sequential records 
 # the goal is to identify groups of sequential records in a dataset, often known as “islands.” These groups consist of consecutive rows that share a common attribute.
 # Concepts:

# IS DISTINCT FROM: is an operator in PostgreSQL that compares two values and returns TRUE if the values are different, treating NULL values as distinct from any non-NULL values.
# In the given answer, event_user IS DISTINCT FROM LAG(event_user) OVER (...) is used to detect changes in event_user across consecutive records. This comparison tells us whether
# the current record's event_user is different from the previous record's event_user within the same source_id.
# FILTER(WHERE): The FILTER clause in PostgreSQL allows you to apply a condition within an aggregate function. It works like a WHERE clause but is used specifically with aggregate functions.
# In the answer, COUNT(break) FILTER (WHERE break) is used to count the occurrences of break values (indicating a change in event_user) across records within each source_id group. The WHERE 
# break part filters the COUNT function to only consider rows where break is TRUE.

--  NOTE : In PostgreSQL, you can use the IS DISTINCT FROM operator and FILTER(WHERE) clause to identify gaps and islands in a dataset

WITH
  tmp (id, source_id, event_user, event_date) AS (
    VALUES
      (1, 1, 'A', NOW()),
      (2, 1, 'A', NOW() + INTERVAL '1 day'),
      (3, 1, 'B', NOW() + INTERVAL '2 day'),
      (4, 1, 'B', NOW() + INTERVAL '3 day'),
      (5, 1, 'A', NOW() + INTERVAL '4 day'),
      (6, 1, 'A', NOW() + INTERVAL '5 day'),
      (7, 1, 'A', NOW() + INTERVAL '6 day'),
      (8, 2, 'A', NOW() + INTERVAL '7 day'),
      (9, 2, 'B', NOW() + INTERVAL '8 day'),
      (10, 2, 'A', NOW() + INTERVAL '9 day'),
      (11, 2, 'B', NOW() + INTERVAL '10 day'),
      (12, 2, 'B', NOW() + INTERVAL '11 day')
  ),
  breaks AS
     (
              SELECT   id,
                       event_user IS DISTINCT
              FROM     lag(event_user) over ( partition BY source_id ORDER BY event_date, event_user ) break,
                       source_id,
                       event_user,
                       event_date
              FROM     tmp
     )
SELECT   id,
         source_id,
         count(break) filter ( WHERE break ) over ( partition BY source_id ORDER BY event_date ) AS series_id,
         event_user,
         event_date
FROM     breaks
ORDER BY event_date;
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

query_1 = """
WITH
  ny AS (
    SELECT cust, AVG(quant) AS "1_avg_quant"
    FROM sales
    WHERE state = 'NY'
    GROUP BY cust
  ),
  ct AS (
    SELECT cust, AVG(quant) AS "2_avg_quant"
    FROM sales
    WHERE state = 'CT'
    GROUP BY cust
  ),
  all_cust AS (
    SELECT DISTINCT cust
    FROM sales
  )
SELECT
  a.cust,
  n."1_avg_quant",
  c."2_avg_quant"
FROM all_cust a
LEFT JOIN ny n ON a.cust = n.cust
LEFT JOIN ct c ON a.cust = c.cust;
"""

query_2 = """
WITH ny AS (
    SELECT cust, prod, AVG(quant) AS "1_avg_quant"
    FROM sales
    WHERE state = 'NY'
    GROUP BY cust, prod
),
ct AS (
    SELECT cust, prod, AVG(quant) AS "2_avg_quant"
    FROM sales
    WHERE state = 'CT'
    GROUP BY cust, prod
)
SELECT *
FROM ny
NATURAL JOIN ct
WHERE "2_avg_quant" > "1_avg_quant"
ORDER BY cust, prod;
"""

query_3 = """
WITH mar AS (
    SELECT cust, state, MIN(quant) AS "1_min_quant"
    FROM sales
    WHERE month = 3
    GROUP BY cust, state
),
may AS (
    SELECT cust, state, MAX(quant) AS "2_max_quant"
    FROM sales
    WHERE month = 5
    GROUP BY cust, state
)
SELECT *
FROM mar
NATURAL JOIN may
ORDER BY cust, state;
"""

query_4 = """
WITH jan AS (
  SELECT cust, state, SUM(quant) AS "1_sum_quant"
  FROM sales
  WHERE month = 1
  GROUP BY cust, state
),
feb AS (
  SELECT cust, state, SUM(quant) AS "2_sum_quant"
  FROM sales
  WHERE month = 2
  GROUP BY cust, state
),
mar AS (
  SELECT cust, state, SUM(quant) AS "3_sum_quant"
  FROM sales
  WHERE month = 3
  GROUP BY cust, state
)
SELECT *
FROM jan
NATURAL JOIN feb
NATURAL JOIN mar
WHERE "1_sum_quant" > 100 OR "2_sum_quant" > 100 OR "3_sum_quant" > 100
ORDER BY cust, state;
"""

query_5 = """
WITH jan AS (
  SELECT cust, AVG(quant) AS "1_avg_quant"
  FROM sales
  WHERE month = 1
  GROUP BY cust
),
feb AS (
  SELECT cust, AVG(quant) AS "2_avg_quant"
  FROM sales
  WHERE month = 2
  GROUP BY cust
)
SELECT *
FROM jan
NATURAL JOIN feb
WHERE "1_avg_quant" < "2_avg_quant"
ORDER BY cust;
"""

def query():
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    #cur.execute("SELECT * FROM sales WHERE quant > 10")
    cur.execute(query_5)

    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()

.headers on
.nullvalue ""

-- Generic daily report template.
-- This file is intended to be run via the companion shell script, which
-- substitutes these placeholders before execution:
--   __AS_OF_DATE__
--   __MARKET__
--   __BUY_SECTION__
--   __BUY_RULE__
--   __FP_THRESHOLD__

DROP TABLE IF EXISTS temp.report_raw;

CREATE TEMP TABLE report_raw (
  section_sort INTEGER,
  section TEXT,
  as_of_date TEXT,
  market TEXT,
  ticker TEXT,
  state_prev TEXT,
  state_today TEXT,
  from_state TEXT,
  to_state TEXT,
  event_date TEXT,
  entry_window_date TEXT,
  first_time_in_ew_ever INTEGER,
  days_in_stabilizing_before_ew INTEGER,
  days_in_current_episode INTEGER,
  days_in_ew_trading INTEGER,
  ew_score_fastpass REAL,
  ew_level_fastpass INTEGER,
  ew_score_rolling REAL,
  ew_level_rolling INTEGER,
  regime TEXT,
  entry_window_exit_state TEXT,
  fail10_prob REAL,
  up20_prob REAL,
  rule_hit TEXT
);

INSERT INTO report_raw (
  section_sort,
  section,
  as_of_date,
  market,
  ticker,
  state_prev,
  state_today,
  from_state,
  to_state,
  event_date,
  entry_window_date,
  first_time_in_ew_ever,
  days_in_stabilizing_before_ew,
  days_in_current_episode,
  days_in_ew_trading,
  ew_score_fastpass,
  ew_level_fastpass,
  ew_score_rolling,
  ew_level_rolling,
  rule_hit
)
WITH RECURSIVE
params AS (
  SELECT
    date('__AS_OF_DATE__') AS as_of_date,
    date('__AS_OF_DATE__', '-1 day') AS prev_date
),
st_today AS (
  SELECT s.ticker, s.state
  FROM rc_state_daily s
  JOIN params p ON s.date = p.as_of_date
),
st_prev AS (
  SELECT s.ticker, s.state
  FROM rc_state_daily s
  JOIN (
    SELECT
      s1.ticker,
      MAX(s1.date) AS prev_trade_date
    FROM rc_state_daily s1
    JOIN params p ON s1.date < p.as_of_date
    GROUP BY s1.ticker
  ) prev
    ON prev.ticker = s.ticker
   AND prev.prev_trade_date = s.date
),
state_rows AS (
  SELECT
    s.ticker,
    s.date,
    s.state,
    ROW_NUMBER() OVER (
      PARTITION BY s.ticker
      ORDER BY s.date
    ) AS rn
  FROM rc_state_daily s
),
first_ew AS (
  SELECT ticker, MIN(date) AS first_ew_date
  FROM rc_state_daily
  WHERE state = 'ENTRY_WINDOW'
  GROUP BY ticker
),
new_ew AS (
  SELECT
    t.ticker,
    t.date AS event_date,
    t.from_state,
    t.to_state
  FROM rc_transition t
  JOIN params p ON t.date = p.as_of_date
  WHERE t.to_state = 'ENTRY_WINDOW'
),
new_pass AS (
  SELECT
    t.ticker,
    t.date AS event_date,
    t.from_state,
    t.to_state
  FROM rc_transition t
  JOIN params p ON t.date = p.as_of_date
  WHERE t.to_state = 'PASS'
),
pass_entry_window AS (
  SELECT
    np.ticker,
    np.event_date AS pass_date,
    (
      SELECT MAX(tr.date)
      FROM rc_transition tr
      JOIN params p ON 1 = 1
      WHERE tr.ticker = np.ticker
        AND tr.to_state = 'ENTRY_WINDOW'
        AND tr.date <= p.prev_date
    ) AS entry_window_date
  FROM new_pass np
),
pass_window_stats AS (
  SELECT
    pew.ticker,
    pew.pass_date,
    pew.entry_window_date,
    (
      SELECT COUNT(*)
      FROM rc_state_daily s
      WHERE s.ticker = pew.ticker
        AND s.state = 'ENTRY_WINDOW'
        AND s.date >= pew.entry_window_date
        AND s.date < pew.pass_date
    ) AS days_in_ew_trading,
    (
      SELECT ew2.ew_score_rolling
      FROM rc_ew_score_daily ew2
      WHERE ew2.ticker = pew.ticker
        AND ew2.date >= pew.entry_window_date
        AND ew2.date < pew.pass_date
        AND ew2.ew_level_rolling IS NOT NULL
      ORDER BY ew2.date DESC
      LIMIT 1
    ) AS ew_score_rolling_end,
    (
      SELECT ew2.ew_level_rolling
      FROM rc_ew_score_daily ew2
      WHERE ew2.ticker = pew.ticker
        AND ew2.date >= pew.entry_window_date
        AND ew2.date < pew.pass_date
        AND ew2.ew_level_rolling IS NOT NULL
      ORDER BY ew2.date DESC
      LIMIT 1
    ) AS ew_level_rolling_end
  FROM pass_entry_window pew
  WHERE pew.entry_window_date IS NOT NULL
),
episode_at_new_ew AS (
  SELECT
    p.episode_id,
    p.ticker,
    p.downtrend_entry_date,
    p.entry_window_date
  FROM rc_pipeline_episode p
  JOIN new_ew ne
    ON ne.ticker = p.ticker
   AND ne.event_date = p.entry_window_date
),
episode_at_pass AS (
  SELECT
    p.episode_id,
    p.ticker,
    p.downtrend_entry_date,
    p.entry_window_date
  FROM rc_pipeline_episode p
  JOIN pass_entry_window pew
    ON pew.ticker = p.ticker
   AND pew.entry_window_date = p.entry_window_date
),
active_ew_candidates AS (
  SELECT
    s.ticker,
    p.entry_window_date,
    p.downtrend_entry_date,
    ROW_NUMBER() OVER (
      PARTITION BY s.ticker
      ORDER BY p.entry_window_date DESC
    ) AS rn
  FROM st_today s
  JOIN params pa ON 1 = 1
  JOIN rc_pipeline_episode p
    ON p.ticker = s.ticker
   AND p.entry_window_date <= pa.as_of_date
   AND (p.entry_window_exit_date IS NULL OR pa.as_of_date <= p.entry_window_exit_date)
  WHERE s.state = 'ENTRY_WINDOW'
),
active_ew AS (
  SELECT
    ticker,
    entry_window_date,
    downtrend_entry_date
  FROM active_ew_candidates
  WHERE rn = 1
),
episode_catalog AS (
  SELECT ticker, entry_window_date, downtrend_entry_date
  FROM episode_at_new_ew
  UNION
  SELECT ticker, entry_window_date, downtrend_entry_date
  FROM episode_at_pass
  UNION
  SELECT ticker, entry_window_date, downtrend_entry_date
  FROM active_ew
),
episode_day_counts AS (
  SELECT
    ec.ticker,
    ec.entry_window_date,
    (
      SELECT COUNT(*)
      FROM rc_state_daily s
      JOIN params p ON 1 = 1
      WHERE s.ticker = ec.ticker
        AND s.date >= ec.downtrend_entry_date
        AND s.date <= p.as_of_date
    ) AS days_in_current_episode
  FROM episode_catalog ec
),
entry_points AS (
  SELECT ticker, event_date AS entry_window_date FROM new_ew
  UNION
  SELECT ticker, entry_window_date FROM pass_entry_window WHERE entry_window_date IS NOT NULL
  UNION
  SELECT ticker, entry_window_date FROM active_ew
),
stab_run AS (
  SELECT
    ep.ticker,
    ep.entry_window_date,
    sr.rn - 1 AS rn
  FROM entry_points ep
  JOIN state_rows sr
    ON sr.ticker = ep.ticker
   AND sr.date = ep.entry_window_date
  WHERE sr.rn > 1

  UNION ALL

  SELECT
    prev.ticker,
    prev.entry_window_date,
    prev.rn - 1 AS rn
  FROM stab_run prev
  JOIN state_rows s
    ON s.ticker = prev.ticker
   AND s.rn = prev.rn
  WHERE s.state = 'STABILIZING'
    AND prev.rn > 1
),
stab_counts AS (
  SELECT
    stab_run.ticker,
    stab_run.entry_window_date,
    COUNT(*) AS days_in_stabilizing_before_ew
  FROM stab_run
  JOIN state_rows sr2
    ON sr2.ticker = stab_run.ticker
   AND sr2.rn = stab_run.rn
  WHERE sr2.state = 'STABILIZING'
  GROUP BY stab_run.ticker, stab_run.entry_window_date
),
fp_at_entry AS (
  SELECT
    ep.ticker,
    ep.entry_window_date,
    ew.ew_score_fastpass AS fp_score,
    ew.ew_level_fastpass AS fp_level
  FROM entry_points ep
  LEFT JOIN rc_ew_score_daily ew
    ON ew.ticker = ep.ticker
   AND ew.date = ep.entry_window_date
),
rolling_today AS (
  SELECT
    ew.ticker,
    ew.date AS as_of_date,
    ew.ew_score_rolling,
    ew.ew_level_rolling
  FROM rc_ew_score_daily ew
  JOIN params p ON ew.date = p.as_of_date
),
buys_market AS (
  SELECT
    np.ticker,
    np.event_date AS pass_date,
    pew.entry_window_date,
    fpe.fp_score,
    fpe.fp_level
  FROM new_pass np
  JOIN pass_entry_window pew ON pew.ticker = np.ticker
  LEFT JOIN fp_at_entry fpe
    ON fpe.ticker = np.ticker
   AND fpe.entry_window_date = pew.entry_window_date
  WHERE fpe.fp_score >= __FP_THRESHOLD__
),
buys_2_se AS (
  SELECT
    ne.ticker,
    ne.event_date AS buy_date,
    ne.event_date AS entry_window_date,
    fpe.fp_score,
    fpe.fp_level
  FROM new_ew ne
  LEFT JOIN fp_at_entry fpe
    ON fpe.ticker = ne.ticker
   AND fpe.entry_window_date = ne.event_date
  WHERE '__MARKET__' = 'SE'
    AND fpe.fp_score >= 0.80
),
buys_3_se AS (
  SELECT
    np.ticker,
    np.event_date AS pass_date,
    pew.entry_window_date,
    fpe.fp_score,
    fpe.fp_level,
    pws.ew_score_rolling_end,
    pws.ew_level_rolling_end
  FROM new_pass np
  JOIN pass_entry_window pew
    ON pew.ticker = np.ticker
  LEFT JOIN pass_window_stats pws
    ON pws.ticker = np.ticker
   AND pws.pass_date = np.event_date
   AND pws.entry_window_date = pew.entry_window_date
  LEFT JOIN fp_at_entry fpe
    ON fpe.ticker = np.ticker
   AND fpe.entry_window_date = pew.entry_window_date
  WHERE '__MARKET__' = 'SE'
    AND pws.ew_level_rolling_end = 1
),
buys_4_se AS (
  SELECT
    b2.ticker,
    b2.buy_date,
    b2.entry_window_date,
    b2.fp_score,
    b2.fp_level,
    b3.pass_date,
    b3.ew_score_rolling_end,
    b3.ew_level_rolling_end
  FROM buys_2_se b2
  JOIN buys_3_se b3
    ON b3.ticker = b2.ticker
   AND b3.entry_window_date = b2.entry_window_date
),
ew_snapshot_ranked AS (
  SELECT
    a.ticker,
    a.entry_window_date,
    a.downtrend_entry_date,
    COALESCE(sc.days_in_stabilizing_before_ew, 0) AS days_in_stabilizing_before_ew,
    edc.days_in_current_episode,
    fpe.fp_score,
    fpe.fp_level,
    rt.ew_score_rolling,
    rt.ew_level_rolling,
    ROW_NUMBER() OVER (
      ORDER BY
        (fpe.fp_score IS NULL) ASC,
        fpe.fp_score DESC,
        (rt.ew_score_rolling IS NULL) ASC,
        rt.ew_score_rolling DESC,
        a.ticker ASC
    ) AS rn
  FROM active_ew a
  JOIN params p ON 1 = 1
  LEFT JOIN stab_counts sc
    ON sc.ticker = a.ticker
   AND sc.entry_window_date = a.entry_window_date
  LEFT JOIN episode_day_counts edc
    ON edc.ticker = a.ticker
   AND edc.entry_window_date = a.entry_window_date
  LEFT JOIN fp_at_entry fpe
    ON fpe.ticker = a.ticker
   AND fpe.entry_window_date = a.entry_window_date
  LEFT JOIN rolling_today rt
    ON rt.ticker = a.ticker
   AND rt.as_of_date = p.as_of_date
),
ew_snapshot AS (
  SELECT *
  FROM ew_snapshot_ranked
  WHERE rn <= 25
),
alerts_raw AS (
  SELECT
    a.ticker,
    1 AS alert_sort,
    'LONG_EPISODE' AS alert_type,
    a.entry_window_date,
    a.days_in_stabilizing_before_ew,
    a.days_in_current_episode,
    a.fp_score,
    a.fp_level,
    a.ew_score_rolling,
    a.ew_level_rolling
  FROM ew_snapshot_ranked a
  WHERE a.days_in_current_episode >= 30

  UNION ALL

  SELECT
    a.ticker,
    2 AS alert_sort,
    'NO_STABILIZING_BEFORE_EW' AS alert_type,
    a.entry_window_date,
    a.days_in_stabilizing_before_ew,
    a.days_in_current_episode,
    a.fp_score,
    a.fp_level,
    a.ew_score_rolling,
    a.ew_level_rolling
  FROM ew_snapshot_ranked a
  WHERE a.days_in_stabilizing_before_ew = 0

  UNION ALL

  SELECT
    b.ticker,
    3 AS alert_sort,
    'LATE_BUY_SIGNAL' AS alert_type,
    b.entry_window_date,
    COALESCE(sc.days_in_stabilizing_before_ew, 0) AS days_in_stabilizing_before_ew,
    edc.days_in_current_episode,
    b.fp_score,
    b.fp_level,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling
  FROM buys_market b
  JOIN params p ON 1 = 1
  LEFT JOIN episode_at_pass epx
    ON epx.ticker = b.ticker
   AND epx.entry_window_date = b.entry_window_date
  LEFT JOIN stab_counts sc
    ON sc.ticker = b.ticker
   AND sc.entry_window_date = b.entry_window_date
  LEFT JOIN episode_day_counts edc
    ON edc.ticker = b.ticker
   AND edc.entry_window_date = b.entry_window_date
  WHERE edc.days_in_current_episode >= 30
),
alerts AS (
  SELECT
    ar.ticker,
    (
      SELECT GROUP_CONCAT(alert_type, ';')
      FROM (
        SELECT x.alert_type
        FROM alerts_raw x
        WHERE x.ticker = ar.ticker
        ORDER BY x.alert_sort
      )
    ) AS alert_type,
    MIN(ar.entry_window_date) AS entry_window_date,
    MAX(ar.days_in_stabilizing_before_ew) AS days_in_stabilizing_before_ew,
    MAX(ar.days_in_current_episode) AS days_in_current_episode,
    MAX(ar.fp_score) AS fp_score,
    MAX(ar.fp_level) AS fp_level,
    MAX(ar.ew_score_rolling) AS ew_score_rolling,
    MAX(ar.ew_level_rolling) AS ew_level_rolling
  FROM alerts_raw ar
  GROUP BY ar.ticker
)
SELECT * FROM (
  SELECT
    1 AS section_sort,
    'NEW_EW' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    ne.ticker,
    sp.state AS state_prev,
    st.state AS state_today,
    ne.from_state,
    ne.to_state,
    ne.event_date AS event_date,
    ne.event_date AS entry_window_date,
    CASE WHEN fe.first_ew_date = ne.event_date THEN 1 ELSE 0 END AS first_time_in_ew_ever,
    COALESCE(sc.days_in_stabilizing_before_ew, 0) AS days_in_stabilizing_before_ew,
    edc.days_in_current_episode,
    NULL AS days_in_ew_trading,
    fpe.fp_score AS ew_score_fastpass,
    fpe.fp_level AS ew_level_fastpass,
    rt.ew_score_rolling,
    rt.ew_level_rolling,
    NULL AS rule_hit
  FROM new_ew ne
  JOIN params p ON 1 = 1
  LEFT JOIN st_prev sp ON sp.ticker = ne.ticker
  LEFT JOIN st_today st ON st.ticker = ne.ticker
  LEFT JOIN first_ew fe ON fe.ticker = ne.ticker
  LEFT JOIN episode_at_new_ew epx
    ON epx.ticker = ne.ticker
   AND epx.entry_window_date = ne.event_date
  LEFT JOIN stab_counts sc
    ON sc.ticker = ne.ticker
   AND sc.entry_window_date = ne.event_date
  LEFT JOIN episode_day_counts edc
    ON edc.ticker = ne.ticker
   AND edc.entry_window_date = ne.event_date
  LEFT JOIN fp_at_entry fpe
    ON fpe.ticker = ne.ticker
   AND fpe.entry_window_date = ne.event_date
  LEFT JOIN rolling_today rt
    ON rt.ticker = ne.ticker
   AND rt.as_of_date = p.as_of_date

  UNION ALL

  SELECT
    1 AS section_sort,
    'NEW_EW' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    '(none)' AS ticker,
    NULL AS state_prev,
    NULL AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    NULL AS event_date,
    NULL AS entry_window_date,
    NULL AS first_time_in_ew_ever,
    NULL AS days_in_stabilizing_before_ew,
    NULL AS days_in_current_episode,
    NULL AS days_in_ew_trading,
    NULL AS ew_score_fastpass,
    NULL AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    'EMPTY_SECTION' AS rule_hit
  FROM params p
  WHERE NOT EXISTS (SELECT 1 FROM new_ew)

  UNION ALL

  SELECT
    2 AS section_sort,
    'NEW_PASS' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    np.ticker,
    sp.state AS state_prev,
    st.state AS state_today,
    np.from_state,
    np.to_state,
    np.event_date AS event_date,
    pew.entry_window_date AS entry_window_date,
    CASE WHEN fe.first_ew_date = pew.entry_window_date THEN 1 ELSE 0 END AS first_time_in_ew_ever,
    COALESCE(sc.days_in_stabilizing_before_ew, 0) AS days_in_stabilizing_before_ew,
    edc.days_in_current_episode,
    pws.days_in_ew_trading,
    fpe.fp_score AS ew_score_fastpass,
    fpe.fp_level AS ew_level_fastpass,
    pws.ew_score_rolling_end AS ew_score_rolling,
    pws.ew_level_rolling_end AS ew_level_rolling,
    NULL AS rule_hit
  FROM new_pass np
  JOIN params p ON 1 = 1
  LEFT JOIN st_prev sp ON sp.ticker = np.ticker
  LEFT JOIN st_today st ON st.ticker = np.ticker
  LEFT JOIN pass_entry_window pew ON pew.ticker = np.ticker
  LEFT JOIN first_ew fe ON fe.ticker = np.ticker
  LEFT JOIN episode_at_pass epx
    ON epx.ticker = np.ticker
   AND epx.entry_window_date = pew.entry_window_date
  LEFT JOIN stab_counts sc
    ON sc.ticker = np.ticker
   AND sc.entry_window_date = pew.entry_window_date
  LEFT JOIN episode_day_counts edc
    ON edc.ticker = np.ticker
   AND edc.entry_window_date = pew.entry_window_date
  LEFT JOIN fp_at_entry fpe
    ON fpe.ticker = np.ticker
   AND fpe.entry_window_date = pew.entry_window_date
  LEFT JOIN pass_window_stats pws
    ON pws.ticker = np.ticker
   AND pws.pass_date = np.event_date
   AND pws.entry_window_date = pew.entry_window_date

  UNION ALL

  SELECT
    2 AS section_sort,
    'NEW_PASS' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    '(none)' AS ticker,
    NULL AS state_prev,
    NULL AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    NULL AS event_date,
    NULL AS entry_window_date,
    NULL AS first_time_in_ew_ever,
    NULL AS days_in_stabilizing_before_ew,
    NULL AS days_in_current_episode,
    NULL AS days_in_ew_trading,
    NULL AS ew_score_fastpass,
    NULL AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    'EMPTY_SECTION' AS rule_hit
  FROM params p
  WHERE NOT EXISTS (SELECT 1 FROM new_pass)

  UNION ALL

  SELECT
    3 AS section_sort,
    '__BUY_SECTION__' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    b.ticker,
    sp.state AS state_prev,
    st.state AS state_today,
    NULL AS from_state,
    'PASS' AS to_state,
    b.pass_date AS event_date,
    b.entry_window_date AS entry_window_date,
    CASE WHEN fe.first_ew_date = b.entry_window_date THEN 1 ELSE 0 END AS first_time_in_ew_ever,
    COALESCE(sc.days_in_stabilizing_before_ew, 0) AS days_in_stabilizing_before_ew,
    edc.days_in_current_episode,
    NULL AS days_in_ew_trading,
    b.fp_score AS ew_score_fastpass,
    b.fp_level AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    '__BUY_RULE__' AS rule_hit
  FROM buys_market b
  JOIN params p ON 1 = 1
  LEFT JOIN st_prev sp ON sp.ticker = b.ticker
  LEFT JOIN st_today st ON st.ticker = b.ticker
  LEFT JOIN first_ew fe ON fe.ticker = b.ticker
  LEFT JOIN episode_at_pass epx
    ON epx.ticker = b.ticker
   AND epx.entry_window_date = b.entry_window_date
  LEFT JOIN stab_counts sc
    ON sc.ticker = b.ticker
   AND sc.entry_window_date = b.entry_window_date
  LEFT JOIN episode_day_counts edc
    ON edc.ticker = b.ticker
   AND edc.entry_window_date = b.entry_window_date

  UNION ALL

  SELECT
    3 AS section_sort,
    '__BUY_SECTION__' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    '(none)' AS ticker,
    NULL AS state_prev,
    NULL AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    NULL AS event_date,
    NULL AS entry_window_date,
    NULL AS first_time_in_ew_ever,
    NULL AS days_in_stabilizing_before_ew,
    NULL AS days_in_current_episode,
    NULL AS days_in_ew_trading,
    NULL AS ew_score_fastpass,
    NULL AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    'EMPTY_SECTION' AS rule_hit
  FROM params p
  WHERE NOT EXISTS (SELECT 1 FROM buys_market)

  UNION ALL

  SELECT
    4 AS section_sort,
    'BUYS_2_SE' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    b.ticker,
    sp.state AS state_prev,
    st.state AS state_today,
    NULL AS from_state,
    'ENTRY_WINDOW' AS to_state,
    b.buy_date AS event_date,
    b.entry_window_date AS entry_window_date,
    CASE WHEN fe.first_ew_date = b.entry_window_date THEN 1 ELSE 0 END AS first_time_in_ew_ever,
    COALESCE(sc.days_in_stabilizing_before_ew, 0) AS days_in_stabilizing_before_ew,
    edc.days_in_current_episode,
    NULL AS days_in_ew_trading,
    b.fp_score AS ew_score_fastpass,
    b.fp_level AS ew_level_fastpass,
    rt.ew_score_rolling,
    rt.ew_level_rolling,
    'SE_BUY_2_ENTRY_FP80' AS rule_hit
  FROM buys_2_se b
  JOIN params p ON 1 = 1
  LEFT JOIN st_prev sp ON sp.ticker = b.ticker
  LEFT JOIN st_today st ON st.ticker = b.ticker
  LEFT JOIN first_ew fe ON fe.ticker = b.ticker
  LEFT JOIN episode_at_new_ew epx
    ON epx.ticker = b.ticker
   AND epx.entry_window_date = b.entry_window_date
  LEFT JOIN stab_counts sc
    ON sc.ticker = b.ticker
   AND sc.entry_window_date = b.entry_window_date
  LEFT JOIN episode_day_counts edc
    ON edc.ticker = b.ticker
   AND edc.entry_window_date = b.entry_window_date
  LEFT JOIN rolling_today rt
    ON rt.ticker = b.ticker
   AND rt.as_of_date = p.as_of_date

  UNION ALL

  SELECT
    4 AS section_sort,
    'BUYS_2_SE' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    '(none)' AS ticker,
    NULL AS state_prev,
    NULL AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    NULL AS event_date,
    NULL AS entry_window_date,
    NULL AS first_time_in_ew_ever,
    NULL AS days_in_stabilizing_before_ew,
    NULL AS days_in_current_episode,
    NULL AS days_in_ew_trading,
    NULL AS ew_score_fastpass,
    NULL AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    'EMPTY_SECTION' AS rule_hit
  FROM params p
  WHERE '__MARKET__' = 'SE'
    AND NOT EXISTS (SELECT 1 FROM buys_2_se)

  UNION ALL

  SELECT
    5 AS section_sort,
    'BUYS_3_SE' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    b.ticker,
    sp.state AS state_prev,
    st.state AS state_today,
    NULL AS from_state,
    'PASS' AS to_state,
    b.pass_date AS event_date,
    b.entry_window_date AS entry_window_date,
    CASE WHEN fe.first_ew_date = b.entry_window_date THEN 1 ELSE 0 END AS first_time_in_ew_ever,
    COALESCE(sc.days_in_stabilizing_before_ew, 0) AS days_in_stabilizing_before_ew,
    edc.days_in_current_episode,
    pws.days_in_ew_trading,
    b.fp_score AS ew_score_fastpass,
    b.fp_level AS ew_level_fastpass,
    b.ew_score_rolling_end AS ew_score_rolling,
    b.ew_level_rolling_end AS ew_level_rolling,
    'SE_BUY_3_PASS_ROLL_LVL1' AS rule_hit
  FROM buys_3_se b
  JOIN params p ON 1 = 1
  LEFT JOIN st_prev sp ON sp.ticker = b.ticker
  LEFT JOIN st_today st ON st.ticker = b.ticker
  LEFT JOIN first_ew fe ON fe.ticker = b.ticker
  LEFT JOIN episode_at_pass epx
    ON epx.ticker = b.ticker
   AND epx.entry_window_date = b.entry_window_date
  LEFT JOIN stab_counts sc
    ON sc.ticker = b.ticker
   AND sc.entry_window_date = b.entry_window_date
  LEFT JOIN episode_day_counts edc
    ON edc.ticker = b.ticker
   AND edc.entry_window_date = b.entry_window_date
  LEFT JOIN pass_window_stats pws
    ON pws.ticker = b.ticker
   AND pws.pass_date = b.pass_date
   AND pws.entry_window_date = b.entry_window_date

  UNION ALL

  SELECT
    5 AS section_sort,
    'BUYS_3_SE' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    '(none)' AS ticker,
    NULL AS state_prev,
    NULL AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    NULL AS event_date,
    NULL AS entry_window_date,
    NULL AS first_time_in_ew_ever,
    NULL AS days_in_stabilizing_before_ew,
    NULL AS days_in_current_episode,
    NULL AS days_in_ew_trading,
    NULL AS ew_score_fastpass,
    NULL AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    'EMPTY_SECTION' AS rule_hit
  FROM params p
  WHERE '__MARKET__' = 'SE'
    AND NOT EXISTS (SELECT 1 FROM buys_3_se)

  UNION ALL

  SELECT
    6 AS section_sort,
    'BUYS_4_SE' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    b.ticker,
    sp.state AS state_prev,
    st.state AS state_today,
    NULL AS from_state,
    'PASS' AS to_state,
    b.pass_date AS event_date,
    b.entry_window_date AS entry_window_date,
    CASE WHEN fe.first_ew_date = b.entry_window_date THEN 1 ELSE 0 END AS first_time_in_ew_ever,
    COALESCE(sc.days_in_stabilizing_before_ew, 0) AS days_in_stabilizing_before_ew,
    edc.days_in_current_episode,
    pws.days_in_ew_trading,
    b.fp_score AS ew_score_fastpass,
    b.fp_level AS ew_level_fastpass,
    b.ew_score_rolling_end AS ew_score_rolling,
    b.ew_level_rolling_end AS ew_level_rolling,
    'SE_BUY_4_ENTRY_FP80_AND_PASS_ROLL_LVL1' AS rule_hit
  FROM buys_4_se b
  JOIN params p ON 1 = 1
  LEFT JOIN st_prev sp ON sp.ticker = b.ticker
  LEFT JOIN st_today st ON st.ticker = b.ticker
  LEFT JOIN first_ew fe ON fe.ticker = b.ticker
  LEFT JOIN episode_at_pass epx
    ON epx.ticker = b.ticker
   AND epx.entry_window_date = b.entry_window_date
  LEFT JOIN stab_counts sc
    ON sc.ticker = b.ticker
   AND sc.entry_window_date = b.entry_window_date
  LEFT JOIN episode_day_counts edc
    ON edc.ticker = b.ticker
   AND edc.entry_window_date = b.entry_window_date
  LEFT JOIN pass_window_stats pws
    ON pws.ticker = b.ticker
   AND pws.pass_date = b.pass_date
   AND pws.entry_window_date = b.entry_window_date

  UNION ALL

  SELECT
    6 AS section_sort,
    'BUYS_4_SE' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    '(none)' AS ticker,
    NULL AS state_prev,
    NULL AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    NULL AS event_date,
    NULL AS entry_window_date,
    NULL AS first_time_in_ew_ever,
    NULL AS days_in_stabilizing_before_ew,
    NULL AS days_in_current_episode,
    NULL AS days_in_ew_trading,
    NULL AS ew_score_fastpass,
    NULL AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    'EMPTY_SECTION' AS rule_hit
  FROM params p
  WHERE '__MARKET__' = 'SE'
    AND NOT EXISTS (SELECT 1 FROM buys_4_se)

  UNION ALL

  SELECT
    7 AS section_sort,
    'EW_SNAPSHOT' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    s.ticker,
    sp.state AS state_prev,
    st.state AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    p.as_of_date AS event_date,
    s.entry_window_date AS entry_window_date,
    CASE WHEN fe.first_ew_date = s.entry_window_date THEN 1 ELSE 0 END AS first_time_in_ew_ever,
    s.days_in_stabilizing_before_ew,
    s.days_in_current_episode,
    NULL AS days_in_ew_trading,
    s.fp_score AS ew_score_fastpass,
    s.fp_level AS ew_level_fastpass,
    s.ew_score_rolling,
    s.ew_level_rolling,
    NULL AS rule_hit
  FROM ew_snapshot s
  JOIN params p ON 1 = 1
  LEFT JOIN st_prev sp ON sp.ticker = s.ticker
  LEFT JOIN st_today st ON st.ticker = s.ticker
  LEFT JOIN first_ew fe ON fe.ticker = s.ticker

  UNION ALL

  SELECT
    7 AS section_sort,
    'EW_SNAPSHOT' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    '(none)' AS ticker,
    NULL AS state_prev,
    NULL AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    NULL AS event_date,
    NULL AS entry_window_date,
    NULL AS first_time_in_ew_ever,
    NULL AS days_in_stabilizing_before_ew,
    NULL AS days_in_current_episode,
    NULL AS days_in_ew_trading,
    NULL AS ew_score_fastpass,
    NULL AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    'EMPTY_SECTION' AS rule_hit
  FROM params p
  WHERE NOT EXISTS (SELECT 1 FROM ew_snapshot)

  UNION ALL

  SELECT
    8 AS section_sort,
    'ALERTS' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    a.ticker,
    sp.state AS state_prev,
    st.state AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    p.as_of_date AS event_date,
    a.entry_window_date AS entry_window_date,
    CASE WHEN fe.first_ew_date = a.entry_window_date THEN 1 ELSE 0 END AS first_time_in_ew_ever,
    a.days_in_stabilizing_before_ew,
    a.days_in_current_episode,
    NULL AS days_in_ew_trading,
    a.fp_score AS ew_score_fastpass,
    a.fp_level AS ew_level_fastpass,
    a.ew_score_rolling,
    a.ew_level_rolling,
    a.alert_type AS rule_hit
  FROM alerts a
  JOIN params p ON 1 = 1
  LEFT JOIN st_prev sp ON sp.ticker = a.ticker
  LEFT JOIN st_today st ON st.ticker = a.ticker
  LEFT JOIN first_ew fe ON fe.ticker = a.ticker

  UNION ALL

  SELECT
    8 AS section_sort,
    'ALERTS' AS section,
    p.as_of_date AS as_of_date,
    '__MARKET__' AS market,
    '(none)' AS ticker,
    NULL AS state_prev,
    NULL AS state_today,
    NULL AS from_state,
    NULL AS to_state,
    NULL AS event_date,
    NULL AS entry_window_date,
    NULL AS first_time_in_ew_ever,
    NULL AS days_in_stabilizing_before_ew,
    NULL AS days_in_current_episode,
    NULL AS days_in_ew_trading,
    NULL AS ew_score_fastpass,
    NULL AS ew_level_fastpass,
    NULL AS ew_score_rolling,
    NULL AS ew_level_rolling,
    'EMPTY_SECTION' AS rule_hit
  FROM params p
  WHERE NOT EXISTS (SELECT 1 FROM alerts)
)
ORDER BY section_sort, market, ticker;

SELECT
  section,
  as_of_date,
  market,
  ticker,
  state_prev,
  state_today,
  from_state,
  to_state,
  event_date,
  entry_window_date,
  first_time_in_ew_ever,
  days_in_stabilizing_before_ew,
  days_in_current_episode,
  days_in_ew_trading,
  ew_score_fastpass,
  ew_level_fastpass,
  ew_score_rolling,
  ew_level_rolling,
  regime,
  entry_window_exit_state,
  fail10_prob,
  up20_prob,
  rule_hit
FROM report_raw
ORDER BY section_sort, market, ticker;

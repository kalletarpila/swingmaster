-- Deduplicate rc_transition on (ticker, date) and enforce uniqueness

CREATE TABLE IF NOT EXISTS rc_transition_new (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reasons_json TEXT,
    run_id TEXT,
    UNIQUE(ticker, date)
);

INSERT INTO rc_transition_new (ticker, date, from_state, to_state, reasons_json, run_id)
SELECT t.ticker, t.date, t.from_state, t.to_state, t.reasons_json, t.run_id
FROM rc_transition t
JOIN (
    SELECT ticker, date, MAX(rowid) AS max_rowid
    FROM rc_transition
    GROUP BY ticker, date
) x
ON t.ticker = x.ticker AND t.date = x.date AND t.rowid = x.max_rowid;

DROP TABLE rc_transition;
ALTER TABLE rc_transition_new RENAME TO rc_transition;

CREATE INDEX IF NOT EXISTS idx_rc_transition_ticker_date ON rc_transition(ticker, date);
-- Purpose: Add unique constraints for rc_transition records.

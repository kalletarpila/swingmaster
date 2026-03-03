-- Add deterministic buy_badges JSON array storage for simulated BUY transactions.

ALTER TABLE rc_transactions_simu
ADD COLUMN buy_badges TEXT NOT NULL DEFAULT '[]';

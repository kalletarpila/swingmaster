ALTER TABLE macro_source_daily
ADD COLUMN is_forward_filled INTEGER NOT NULL DEFAULT 0;

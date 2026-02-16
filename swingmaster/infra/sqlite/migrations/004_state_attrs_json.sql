-- Add state_attrs_json for versioned policy metadata persistence.

ALTER TABLE rc_state_daily
ADD COLUMN state_attrs_json TEXT NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS pws (
  id SERIAL PRIMARY KEY,
  pwsid TEXT UNIQUE,
  name  TEXT UNIQUE NOT NULL,
  state CHAR(2),
  notes TEXT
);
CREATE TABLE IF NOT EXISTS contaminant (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS measurement (
  id BIGSERIAL PRIMARY KEY,
  pws_id INT NOT NULL REFERENCES pws(id) ON DELETE CASCADE,
  contaminant_id INT NOT NULL REFERENCES contaminant(id) ON DELETE CASCADE,
  year INT,
  value_ppb DOUBLE PRECISION NOT NULL,
  basis TEXT,
  source_url TEXT,
  last_updated TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS zip_pws (
  zip CHAR(5) NOT NULL,
  pwsid TEXT,
  pws_name TEXT NOT NULL,
  coverage_fraction DOUBLE PRECISION,
  PRIMARY KEY (zip, pws_name)
);
ALTER TABLE measurement
  ADD CONSTRAINT value_ppb_nonneg CHECK (value_ppb >= 0) NOT VALID,
  ADD CONSTRAINT year_reasonable CHECK (year IS NULL OR year BETWEEN 2000 AND 2100) NOT VALID;
ALTER TABLE zip_pws
  ADD CONSTRAINT zip_format CHECK (zip ~ '^[0-9]{5}$') NOT VALID,
  ADD CONSTRAINT cov_range CHECK (coverage_fraction IS NULL OR (coverage_fraction >= 0 AND coverage_fraction <= 1)) NOT VALID;
CREATE INDEX IF NOT EXISTS ix_measurement_pws_cont ON measurement(pws_id, contaminant_id);
CREATE INDEX IF NOT EXISTS ix_measurement_year ON measurement(year);
CREATE INDEX IF NOT EXISTS ix_zip_pws_zip ON zip_pws(zip);


-- seed_quick.sql
-- Tiny demo dataset to prove the API works even before you ingest your real CSVs.

DROP TABLE IF EXISTS zip_pws;
DROP TABLE IF EXISTS measurement;
DROP TABLE IF EXISTS contaminant;
DROP TABLE IF EXISTS pws;

CREATE TABLE pws (
  id SERIAL PRIMARY KEY,
  pwsid TEXT UNIQUE,
  name TEXT UNIQUE NOT NULL,
  state CHAR(2),
  notes TEXT
);

CREATE TABLE contaminant (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE measurement (
  id BIGSERIAL PRIMARY KEY,
  pws_id INT REFERENCES pws(id),
  contaminant_id INT REFERENCES contaminant(id),
  year INT,
  value_ppb DOUBLE PRECISION,
  basis TEXT,
  source_url TEXT,
  last_updated TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE zip_pws (
  zip CHAR(5),
  pwsid TEXT,
  pws_name TEXT,
  coverage_fraction DOUBLE PRECISION,
  PRIMARY KEY (zip, pws_name)
);

-- Seed PWS
INSERT INTO pws (pwsid, name, state) VALUES
('MD0000000','Baltimore City DPW','MD'),
('FL0000000','Miami-Dade','FL')
ON CONFLICT DO NOTHING;

-- Seed contaminants
INSERT INTO contaminant (name) VALUES
('Total Trihalomethanes (TTHM)'),
('Haloacetic Acids (HAA5)'),
('Lead (90th percentile)')
ON CONFLICT DO NOTHING;

-- Seed measurements (Baltimore + Miami)
INSERT INTO measurement (pws_id, contaminant_id, year, value_ppb, basis, source_url)
SELECT p.id, c.id, 2024, 72.0, 'City distribution LRAA', 'local:/mnt/data/baltimore-2.png'
FROM pws p, contaminant c WHERE p.name='Baltimore City DPW' AND c.name='Total Trihalomethanes (TTHM)';
INSERT INTO measurement (pws_id, contaminant_id, year, value_ppb, basis, source_url)
SELECT p.id, c.id, 2024, 54.0, 'City distribution LRAA', 'local:/mnt/data/baltimore-2.png'
FROM pws p, contaminant c WHERE p.name='Baltimore City DPW' AND c.name='Haloacetic Acids (HAA5)';
INSERT INTO measurement (pws_id, contaminant_id, year, value_ppb, basis, source_url)
SELECT p.id, c.id, 2024, 2.74, '90% of tests less than', 'local:/mnt/data/baltimore-1.png'
FROM pws p, contaminant c WHERE p.name='Baltimore City DPW' AND c.name='Lead (90th percentile)';

INSERT INTO measurement (pws_id, contaminant_id, year, value_ppb, basis, source_url)
SELECT p.id, c.id, 2024, 51.0, 'LRAA', 'https://www.miamidade.gov/water/library/reports/water-quality-2024.pdf'
FROM pws p, contaminant c WHERE p.name='Miami-Dade' AND c.name='Total Trihalomethanes (TTHM)';
INSERT INTO measurement (pws_id, contaminant_id, year, value_ppb, basis, source_url)
SELECT p.id, c.id, 2024, 44.0, 'LRAA', 'https://www.miamidade.gov/water/library/reports/water-quality-2024.pdf'
FROM pws p, contaminant c WHERE p.name='Miami-Dade' AND c.name='Haloacetic Acids (HAA5)';

-- Seed zip crosswalk (coverage=1 for demo)
INSERT INTO zip_pws (zip, pwsid, pws_name, coverage_fraction) VALUES
('21201','MD0000000','Baltimore City DPW',0.99),
('33101','FL0000000','Miami-Dade',1.0)
ON CONFLICT DO NOTHING;

-- Done.

CREATE UNLOGGED TABLE IF NOT EXISTS vehicles_incoming (
  ts timestamptz NOT NULL,
  country_of_registration char(1) NOT NULL,
  license_plate text NOT NULL,
  vehicle_type text NOT NULL,
  colour text NOT NULL,
  brand text NOT NULL,
  location_of_crossing text NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS vehicles_outgoing (
  ts timestamptz NOT NULL,
  country_of_registration char(1) NOT NULL,
  license_plate text NOT NULL,
  vehicle_type text NOT NULL,
  colour text NOT NULL,
  brand text NOT NULL,
  location_of_crossing text NOT NULL
);

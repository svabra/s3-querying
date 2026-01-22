CREATE INDEX IF NOT EXISTS idx_in_ts ON vehicles_incoming (ts);
CREATE INDEX IF NOT EXISTS idx_out_ts ON vehicles_outgoing (ts);

CREATE INDEX IF NOT EXISTS idx_in_vehicle_ts
  ON vehicles_incoming (country_of_registration, license_plate, ts);
CREATE INDEX IF NOT EXISTS idx_out_vehicle_ts
  ON vehicles_outgoing (country_of_registration, license_plate, ts);

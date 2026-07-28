CREATE TABLE IF NOT EXISTS observer_registration_requests (
  id BIGSERIAL PRIMARY KEY,
  public_key TEXT NOT NULL UNIQUE,
  iata TEXT NOT NULL,
  display_name TEXT,
  contact TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

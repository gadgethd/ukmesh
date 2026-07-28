ALTER TABLE spam_message_incidents
  ADD COLUMN IF NOT EXISTS webhook_alerted_at TIMESTAMPTZ;

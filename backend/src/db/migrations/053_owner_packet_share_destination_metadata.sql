ALTER TABLE owner_packet_share_destinations
  ADD COLUMN IF NOT EXISTS website_url TEXT,
  ADD COLUMN IF NOT EXISTS description TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conrelid = 'owner_packet_share_destinations'::regclass
       AND conname = 'owner_packet_share_destinations_website_url_length'
  ) THEN
    ALTER TABLE owner_packet_share_destinations
      ADD CONSTRAINT owner_packet_share_destinations_website_url_length
      CHECK (website_url IS NULL OR char_length(website_url) BETWEEN 1 AND 2_048);
  END IF;

  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conrelid = 'owner_packet_share_destinations'::regclass
       AND conname = 'owner_packet_share_destinations_description_length'
  ) THEN
    ALTER TABLE owner_packet_share_destinations
      ADD CONSTRAINT owner_packet_share_destinations_description_length
      CHECK (description IS NULL OR char_length(description) BETWEEN 1 AND 160);
  END IF;
END $$;

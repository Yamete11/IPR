-- Reference data for the seed topology (seed/city-topology.json). Idempotent: re-runs insert nothing.

INSERT INTO public.repair_crews (crew_id, name, phone) VALUES
    ('crew-hv',    'High-voltage crew',      '+7-900-000-0101'),
    ('crew-grid',  'Distribution grid crew', '+7-900-000-0102'),
    ('crew-north', 'North district crew',    '+7-900-000-0103'),
    ('crew-south', 'South district crew',    '+7-900-000-0104')
ON CONFLICT (crew_id) DO NOTHING;

INSERT INTO public.crew_assignments (crew_id, asset_id) VALUES
    ('crew-hv',    'sub-1'),
    ('crew-grid',  'node-5'),
    ('crew-grid',  'node-6'),
    ('crew-north', 'bld-12'),
    ('crew-north', 'bld-19'),
    ('crew-south', 'bld-20')
ON CONFLICT (crew_id, asset_id) DO NOTHING;

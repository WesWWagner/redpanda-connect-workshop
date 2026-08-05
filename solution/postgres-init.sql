CREATE TABLE public.orders (
  id     SERIAL PRIMARY KEY,
  item   TEXT    NOT NULL,
  qty    INTEGER NOT NULL DEFAULT 1,
  status TEXT    NOT NULL DEFAULT 'pending'
);

INSERT INTO public.orders (item, qty, status) VALUES
  ('widget',        5,  'pending'),
  ('gadget',        3,  'shipped'),
  ('doohickey',    10,  'pending'),
  ('thingamajig',   2,  'delivered'),
  ('whatchamacallit', 7, 'pending'),
  ('gizmo',         1,  'cancelled'),
  ('doodad',        4,  'shipped'),
  ('contraption',   6,  'pending'),
  ('apparatus',     9,  'shipped'),
  ('device',        8,  'delivered');

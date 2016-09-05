CREATE TABLE Test
(
  ID serial PRIMARY KEY,
  ValueInt int4 DEFAULT 0,
  ValueFloat real DEFAULT 0.0,
  ValueString text DEFAULT ''
);

INSERT INTO Test (ID, ValueInt, ValueFloat, ValueString)
VALUES (generate_series(1,1000), (random() * 1000000)::int4,
  random() * 1000000, 'String ' || random());

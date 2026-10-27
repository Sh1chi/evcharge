CREATE TABLE stations (
	id        BIGSERIAL PRIMARY KEY,
	name      TEXT NOT NULL,
	lat       DOUBLE PRECISION NOT NULL,
	lon       DOUBLE PRECISION NOT NULL,
	connector TEXT NOT NULL,
	power_kw  DOUBLE PRECISION NOT NULL,
	status    TEXT NOT NULL
);

INSERT INTO stations (name, lat, lon, connector, power_kw, status) VALUES
	('Станция 1', 53.3560, 83.7700, 'CCS2',    50,  'free'),
	('Станция 2', 53.3200, 83.7000, 'CCS2',    150, 'free'),
	('Станция 3', 53.3500, 83.7750, 'Type2',   22,  'busy'),
	('Станция 4', 53.3700, 83.7500, 'Type2',   22,  'free'),
	('Станция 5', 53.3450, 83.7850, 'CHAdeMO', 50,  'free'),
	('Станция 6', 53.3300, 83.7300, 'CHAdeMO', 100, 'free');

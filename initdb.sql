CREATE TABLE IF NOT EXISTS districts (
    serial INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS assemblies (
    serial INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    district_id INTEGER NOT NULL,
    FOREIGN KEY (district_id) REFERENCES districts (serial)
);

CREATE TABLE IF NOT EXISTS polling_stations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    location TEXT NULL,
    assembly_serial INTEGER NOT NULL,
    FOREIGN KEY (assembly_serial) REFERENCES assemblies (serial)
);

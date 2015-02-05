DROP TABLE IF EXISTS IsamTable;

CREATE TABLE IsamTable (
	itemid INT NOT NULL,
	location POINT NOT NULL,
	SPATIAL INDEX(location),
	PRIMARY KEY(itemId)
) ENGINE = MYISAM;

SELECT I.id, POINT(L.latitude, L.longitude)
FROM Item I, Location L
WHERE I.location_id=L.id AND L.latitude IS NOT NULL AND L.longitude IS NOT NULL
INTO OUTFILE 'isam.csv';
DROP TABLE IF EXISTS IsamTable;

CREATE TABLE IsamTable (
	itemId INT NOT NULL,
	location POINT NOT NULL,
	SPATIAL INDEX(location),
	PRIMARY KEY(itemId)
) ENGINE = MYISAM;

SELECT I.id, L.latitude, L.longitude
FROM Item I, Location L
WHERE I.location_id=L.id AND L.latitude IS NOT NULL AND L.longitude IS NOT NULL
INTO OUTFILE '/tmp/isam.csv';

LOAD DATA INFILE '/tmp/isam.csv'
INTO TABLE IsamTable
FIELDS TERMINATED BY '\t'
(@id, @lat, @lon)
SET itemId=@id, location=POINT(@lat, @lon);

# SELECT *
# FROM Item I, Location L, IsamTable IT
# WHERE I.location_id=L.id AND I.id=IT.itemId
#       AND (L.latitude<>X(IT.location) OR L.longitude<>Y(IT.location));
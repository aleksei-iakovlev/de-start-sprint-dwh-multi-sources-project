CREATE TABLE stg.api_couriers (
	id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	object_id VARCHAR(255) UNIQUE ,
	object_value json
);

CREATE TABLE stg.api_deliveries (
	id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	object_id VARCHAR(255) UNIQUE ,
	object_value json
);
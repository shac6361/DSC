DO $$ DECLARE
rec RECORD;
BEGIN
	FOR rec IN (
	SELECT
		tablename
	FROM
		pg_tables
	WHERE
		schemaname = 'public') LOOP
EXECUTE 'DROP TABLE IF EXISTS ' || rec.tablename || ' CASCADE';
END LOOP;
END $$;

CREATE TABLE outcomes (
    "Animal ID" VARCHAR,
    "Name" VARCHAR,
    "Date of Outcome" DATE,
    "Date of Birth" DATE,
    "Outcome Type" VARCHAR,
    "Outcome Subtype" VARCHAR,
    "Animal Type" VARCHAR,
    "Sex upon Outcome" VARCHAR,
    "Years Age upon Outcome" NUMERIC(7),
    "Reproductive Status upon Outcome" VARCHAR,
    "Breed" VARCHAR,
    "Color" VARCHAR
);

CREATE TABLE dim_animal (
    "Animal ID" VARCHAR PRIMARY KEY,
    "Name" VARCHAR,
    "Date of Birth" DATE,
    "Animal Type" VARCHAR,
    "Breed" VARCHAR,
    "Color" VARCHAR
);

CREATE TABLE dim_age_at_outcome (
    "Age ID" SERIAL PRIMARY KEY,
    "Animal Type" VARCHAR,
    "Age Years" NUMERIC,
    "Animal Age Category" VARCHAR
);

CREATE TABLE dim_rp_status (
    "RP Status ID" SERIAL PRIMARY KEY,
    "Reproductive Status upon Outcome" VARCHAR,
    "Sex upon Outcome" VARCHAR
);

--CREATE TABLE dim_sex (
--    "Sex ID" SERIAL PRIMARY KEY,
--    "Sex upon Outcome" VARCHAR
--);

CREATE TABLE dim_outcome_type (
    "Outcome Type ID" SERIAL PRIMARY KEY,
    "Outcome Type" VARCHAR,
    "Outcome Subtype" VARCHAR
);

CREATE TABLE dim_outcome_date (
    "Outcome Date ID" SERIAL PRIMARY KEY,
    "Date of Outcome" DATE,
    "Outcome Day" NUMERIC(2),
    "Outcome Month" VARCHAR,
    "Outcome Year" NUMERIC(4),
    "Outcome Day of Week" VARCHAR
);

CREATE TABLE fct_outcome (
    "Outcome ID" SERIAL PRIMARY KEY,
    "Animal ID" VARCHAR REFERENCES dim_animal("Animal ID"),
    "Outcome Date ID" SERIAL REFERENCES dim_outcome_date("Outcome Date ID"),
    "Outcome Type ID" SERIAL REFERENCES dim_outcome_type("Outcome Type ID"),
    "Age at Outcome ID" SERIAL REFERENCES dim_age_at_outcome("Age ID"),  
    "Reproductive Status ID" SERIAL REFERENCES dim_rp_status("RP Status ID")
);
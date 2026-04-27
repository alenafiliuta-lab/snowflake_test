CREATE DATABASE IF NOT EXISTS MY_DB;
USE DATABASE MY_DB;
USE SCHEMA PUBLIC;
CREATE OR REPLACE STAGE my_internal_stage;

CREATE OR REPLACE TABLE Airline_Dataset (
anonymous_id INTEGER,
Passenger_ID VARCHAR(100),
First_Name VARCHAR(100),
Last_Name VARCHAR(100),
Gender VARCHAR(100),
Age INTEGER,
Nationality VARCHAR(100),
Airport_Name VARCHAR(100),
Airport_Country_Code VARCHAR(100),
Country_Name VARCHAR(100),
Airport_Continent VARCHAR(100),
Continents VARCHAR(100),
Departure_Date VARCHAR(100),
Arrival_Airport VARCHAR(100),
Pilot_Name VARCHAR(100),
Flight_Status VARCHAR(100),
Ticket_Ty VARCHAR(100),
Passenger_Status VARCHAR(100)
);


CREATE OR REPLACE TABLE silver_passenger (
Passenger_ID VARCHAR(100),
First_Name VARCHAR(100),
Last_Name VARCHAR(100),
Gender VARCHAR(100),
Age INTEGER,
Nationality VARCHAR(100)
);


CREATE OR REPLACE TABLE silver_airport (
Passenger_ID VARCHAR(100),
Airport_Name VARCHAR(100),
Airport_Country_Code VARCHAR(100),
Country_Name VARCHAR(100),
Airport_Continent VARCHAR(100),
Continents VARCHAR(100)
);


CREATE OR REPLACE TABLE silver_flights(
Passenger_ID VARCHAR(100),
Departure_Date VARCHAR(100),
Arrival_Airport VARCHAR(100),
Pilot_Name VARCHAR(100),
Flight_Status VARCHAR(100),
Ticket_Ty VARCHAR(100),
Passenger_Status VARCHAR(100)
);


CREATE OR REPLACE TABLE audit_log (
id NUMBER AUTOINCREMENT,
table_name VARCHAR(100),      
operation VARCHAR(50),        
rows_affected INTEGER,         
run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE load_all_silver_tables()
RETURNS STRING
LANGUAGE SQL
AS 
$$
BEGIN
    TRUNCATE TABLE silver_passenger;
    INSERT INTO silver_passenger(Passenger_ID,First_Name,Last_Name,Gender,Age,Nationality)
    SELECT
        Passenger_ID,
        INITCAP(TRIM(First_Name)) AS First_Name,
        INITCAP(TRIM(Last_Name)) AS Last_Name,
        TRIM(Gender) AS Gender,
        TRY_CAST(Age AS INTEGER) AS Age,
        TRIM(Nationality) AS Nationality
    FROM AIRLINE_DATASET
    WHERE Passenger_ID IS NOT NULL;
    
    INSERT INTO audit_log (table_name, operation, rows_affected)
        SELECT 'silver_passenger', 'TRUNCATE+INSERT', COUNT(*)
        FROM AIRLINE_DATASET
        WHERE Passenger_ID IS NOT NULL;
    
    TRUNCATE TABLE silver_airport;
    INSERT INTO silver_airport(Passenger_ID, Airport_Name, Airport_Country_Code, Country_Name, Airport_Continent, Continents)
    SELECT 
        Passenger_ID,
        INITCAP(TRIM(Airport_Name)) AS Airport_Name,
        UPPER(TRIM(Airport_Country_Code)) AS Airport_Country_Code,
        INITCAP(TRIM(Country_Name)) AS Country_Name ,
        INITCAP(TRIM(Airport_Continent)) AS Airport_Continent,
        INITCAP(TRIM(Continents)) AS Continents
    FROM AIRLINE_DATASET
    WHERE Passenger_ID IS NOT NULL;
    
    INSERT INTO audit_log (table_name, operation, rows_affected)
        SELECT 'silver_airport', 'TRUNCATE+INSERT', COUNT(*)
        FROM AIRLINE_DATASET
        WHERE Passenger_ID IS NOT NULL;

    TRUNCATE TABLE silver_flights;
    INSERT INTO silver_flights (Passenger_ID, Departure_Date, Arrival_Airport,Pilot_Name, Flight_Status, Ticket_Ty,Passenger_Status)
    SELECT
        Passenger_ID,
        TRY_CAST(Departure_Date AS DATE) AS Departure_Date,
        UPPER(TRIM(Arrival_Airport)) AS Arrival_Airport,
        INITCAP(TRIM(Pilot_Name)) AS Pilot_Name,
        INITCAP(TRIM(Flight_Status)) AS Flight_Status,
        INITCAP(TRIM(Ticket_Ty)) AS Ticket_Ty,
        INITCAP(TRIM(Passenger_Status)) AS Passenger_Status
    FROM AIRLINE_DATASET
    WHERE Passenger_ID IS NOT NULL
      AND Departure_Date IS NOT NULL;
    
    INSERT INTO audit_log (table_name, operation, rows_affected)
        SELECT 'silver_flights', 'TRUNCATE+INSERT', COUNT(*)
        FROM AIRLINE_DATASET
        WHERE Passenger_ID IS NOT NULL;
        
    RETURN 'All silver tables loaded successfully.';
END;
$$;


CREATE OR REPLACE PROCEDURE load_gold_layer()
RETURNS STRING
LANGUAGE SQL
AS $$
BEGIN
    CREATE OR REPLACE TABLE gold_flight_stats_by_status AS
    SELECT 
        f.flight_status,
        COUNT(DISTINCT f.passenger_id) AS unique_passengers,
        COUNT(*) AS total_tickets,
        ROUND(AVG(p.age), 0) AS avg_passenger_age
    FROM silver_flights f
    LEFT JOIN silver_passenger p ON f.passenger_id = p.passenger_id
    GROUP BY f.flight_status;
    INSERT INTO audit_log (table_name, operation, rows_affected)
        SELECT 'gold_flight_stats_by_status', 'CREATE+AGGREGATE', COUNT(*)
        FROM silver_flights f
        LEFT JOIN silver_passenger p ON f.passenger_id = p.passenger_id
        GROUP BY f.flight_status;
    RETURN 'Gold table refreshed';
END;
$$;


--DDL-query (use Time Travel)
ALTER TABLE gold_flight_stats_by_status SET DATA_RETENTION_TIME_IN_DAYS = 1;
DROP TABLE gold_flight_stats_by_status
UNDROP TABLE gold_flight_stats_by_status

--DML-query (table that was before)
SELECT *
FROM gold_flight_stats_by_status AT (OFFSET => -600);
--DML-query (insert old rows)
INSERT INTO silver_passenger (Passenger_ID, First_Name, Last_Name, Gender, Age, Nationality)
    SELECT Passenger_ID, First_Name, Last_Name, Gender, Age, Nationality
    FROM silver_passenger AT (OFFSET => -7200);

--Create Secure View
CREATE OR REPLACE SECURE VIEW secure_flight_stats AS
    SELECT flight_status,unique_passengers,total_tickets, avg_passenger_age
    FROM gold_flight_stats_by_status
WHERE 
    CASE 
        WHEN CURRENT_ROLE() = 'MANAGER' THEN TRUE
        WHEN CURRENT_ROLE() = 'ANALYST' AND flight_status = 'Cancelled' THEN TRUE
        ELSE FALSE
    END;
--CHECK   
CREATE ROLE IF NOT EXISTS ANALYST;
GRANT ROLE ANALYST TO ROLE ACCOUNTADMIN;
USE ROLE ANALYST;
SELECT CURRENT_ROLE();
SELECT * FROM secure_flight_stats; 
USE ROLE ACCOUNTADMIN


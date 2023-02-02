--------------------------------------
-- STAGE
--------------------------------------

DROP SCHEMA IF EXISTS stage CASCADE;

CREATE SCHEMA stage;

CREATE TABLE stage.raw_sales (
       invoice_id text
     , branch text
     , city text
     , customer_type text
     , gender text
     , product_line text
     , unit_price text
     , quantity text
     , tax text
     , total text
     , "date" text
     , "time" text
     , payment text
     , cogs text
     , gross_margin_percentage text
     , gross_income text
     , rating text
);

CREATE TABLE stage.valid_sales (
       invoice_id char(11) UNIQUE NOT NULL
     , branch char(1) NOT NULL
     , city varchar(20) NOT NULL
     , customer_type varchar(10) NOT NULL
     , gender varchar(10) NOT NULL
     , product_line varchar(255) NOT NULL
     , unit_price float NOT NULL
     , quantity int NOT NULL
     , tax float NOT NULL
     , total float NOT NULL
     , "date" date NOT NULL
     , "time" time NOT NULL
     , payment varchar(32) NOT NULL
     , cogs float NOT NULL
     , gross_margin_percentage float NOT NULL
     , gross_income float NOT NULL
     , rating float NOT NULL
);

CREATE TABLE stage.errors (
       id SERIAL
     , invoice_id TEXT
     , sql_state TEXT
     , message TEXT
     , detail TEXT
     , hint TEXT
     , context TEXT
);

CREATE OR REPLACE FUNCTION stage.validate_insert()
    RETURNS VOID AS
$BODY$
DECLARE
    r record;
    sql_state TEXT;
    message TEXT;
    detail TEXT;
    hint TEXT;
    context TEXT;
BEGIN
    FOR r IN 
    SELECT
       invoice_id
     , branch
     , city
     , customer_type
     , gender
     , product_line
     , unit_price
     , quantity
     , tax
     , total
     , "date"
     , "time"
     , payment
     , cogs
     , gross_margin_percentage
     , gross_income
     , rating
    FROM stage.raw_sales
    LOOP
        BEGIN
            INSERT INTO stage.valid_sales (
                  invoice_id
                , branch
                , city
                , customer_type
                , gender
                , product_line
                , unit_price
                , quantity
                , tax
                , total
                , "date"
                , "time"
                , payment
                , cogs
                , gross_margin_percentage
                , gross_income
                , rating
            ) VALUES (
                  r.invoice_id :: char(11)
                , r.branch :: char(1)
                , r.city :: varchar(20)
                , r.customer_type :: varchar(10)
                , r.gender :: varchar(10)
                , r.product_line :: varchar(255)
                , r.unit_price :: float
                , r.quantity :: int
                , r.tax :: float
                , r.total :: float
                , r."date" :: date
                , r."time" :: time
                , r.payment :: varchar(32)
                , r.cogs :: float
                , r.gross_margin_percentage :: float
                , r.gross_income :: float
                , r.rating :: float
            );
        EXCEPTION
        WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            sql_state := RETURNED_SQLSTATE,
            message := MESSAGE_TEXT,
            detail := PG_EXCEPTION_DETAIL,
            hint := PG_EXCEPTION_HINT,
            context := PG_EXCEPTION_CONTEXT;
        INSERT INTO stage.errors (invoice_id, sql_state, message, detail, hint, context)
        VALUES (r.invoice_id, sql_state, message, detail, hint, context);
        END;
    END LOOP;
END
$BODY$
LANGUAGE plpgsql;

--------------------------------------
-- NDS
--------------------------------------

DROP SCHEMA IF EXISTS nds CASCADE;

CREATE SCHEMA nds;

CREATE TABLE nds.city (
      id serial PRIMARY KEY
    , city varchar(20) NOT NULL
);

CREATE OR REPLACE FUNCTION nds.city_lookup_update(v text) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM nds.city
        WHERE city = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO nds.city (city)
        VALUES (v) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

CREATE TABLE nds.branch (
      id serial PRIMARY KEY
    , branch char(1) NOT NULL 
    , city_id int REFERENCES nds.city(id)
);

CREATE OR REPLACE FUNCTION nds.branch_lookup_update(br TEXT, ct TEXT) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT b.id 
        INTO res
        FROM nds.branch AS b
        JOIN nds.city AS c ON c.city = ct
        WHERE b.branch = br;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO nds.branch (branch, city_id)
        VALUES (br, nds.city_lookup_update(ct)) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

CREATE TABLE nds.customer_type (
      id serial PRIMARY KEY
    , customer_type varchar(10) NOT NULL
);

CREATE OR REPLACE FUNCTION nds.customer_type_lookup_update(v text) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM nds.customer_type
        WHERE customer_type = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO nds.customer_type (customer_type)
        VALUES (v) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

CREATE TABLE nds.gender (
      id serial PRIMARY KEY
    , gender varchar(10) NOT NULL 
);

CREATE OR REPLACE FUNCTION nds.gender_lookup_update(v text) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM nds.gender
        WHERE gender = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO nds.gender (gender)
        VALUES (v) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

CREATE TABLE nds.product_line (
      id serial PRIMARY KEY
    , product_line varchar(255) NOT NULL
);

CREATE OR REPLACE FUNCTION nds.product_line_lookup_update(v text) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM nds.product_line
        WHERE product_line = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO nds.product_line (product_line)
        VALUES (v) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

CREATE TABLE nds.payment (
      id serial PRIMARY KEY
    , payment varchar(32)
);

CREATE OR REPLACE FUNCTION nds.payment_lookup_update(v text) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM nds.payment
        WHERE payment = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO nds.payment (payment)
        VALUES (v) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

CREATE TABLE nds.sales (
      invoice_id char(11) PRIMARY KEY
    , branch_id int REFERENCES nds.branch(id)
    , customer_type int REFERENCES nds.customer_type(id)
    , gender int REFERENCES nds.gender(id)
    , product_line int REFERENCES nds.product_line(id)
    , payment int REFERENCES nds.payment(id)
    , datetime timestamp
    , unit_price float
    , quantity int
    , rating float
);

--------------------------------------
-- DWH
--------------------------------------

DROP SCHEMA IF EXISTS dwh CASCADE;

CREATE SCHEMA dwh;

-- dwh.dim_calendar

CREATE TABLE dwh.dim_calendar (
      id int PRIMARY KEY
    , "date" date NOT NULL
    , epoch bigint NOT NULL
    , day_suffix varchar(4) NOT NULL
    , day_name varchar(15) NOT NULL
    , day_of_week int NOT NULL
    , day_of_month int NOT NULL
    , day_of_quarter int NOT NULL
    , day_of_year int NOT NULL
    , week_of_month int NOT NULL
    , week_of_year int NOT NULL
    , month_actual int NOT NULL
    , month_name varchar(9) NOT NULL
    , month_name_short char(3) NOT NULL
    , quarter_actual int NOT NULL
    , quarter_name varchar(9) NOT NULL
    , year_actual int NOT NULL
    , first_day_of_week date NOT NULL
    , last_day_of_week date NOT NULL
    , first_day_of_month date NOT NULL
    , last_day_of_month date NOT NULL
    , first_day_of_quarter date NOT NULL
    , last_day_of_quarter date NOT NULL
    , first_day_of_year date NOT NULL
    , last_day_of_year date NOT NULL
    , mmyyyy char(6) NOT NULL
    , mmddyyyy char(8) NOT NULL
    , weekend bool NOT NULL
);

INSERT INTO dwh.dim_calendar
SELECT 
         TO_CHAR(ts, 'yyyymmdd')::INT AS id
       , ts AS date_actual
       , EXTRACT(EPOCH FROM ts) AS epoch
       , TO_CHAR(ts, 'fmDDth') AS day_suffix
       , TO_CHAR(ts, 'TMDay') AS day_name
       , EXTRACT(ISODOW FROM ts) AS day_of_week
       , EXTRACT(DAY FROM ts) AS day_of_month
       , ts - DATE_TRUNC('quarter', ts)::DATE + 1 AS day_of_quarter
       , EXTRACT(DOY FROM ts) AS day_of_year
       , TO_CHAR(ts, 'W')::INT AS week_of_month
       , EXTRACT(WEEK FROM ts) AS week_of_year
       , EXTRACT(MONTH FROM ts) AS month_actual
       , TO_CHAR(ts, 'TMMonth') AS month_name
       , TO_CHAR(ts, 'Mon') AS month_name_short
       , EXTRACT(QUARTER FROM ts) AS quarter_actual
       , CASE
           WHEN EXTRACT(QUARTER FROM ts) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM ts) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM ts) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM ts) = 4 THEN 'Fourth'
           END AS quarter_name
       , EXTRACT(YEAR FROM ts) AS year_actual
       , ts + (1 - EXTRACT(ISODOW FROM ts))::INT AS first_day_of_week
       , ts + (7 - EXTRACT(ISODOW FROM ts))::INT AS last_day_of_week
       , ts + (1 - EXTRACT(DAY FROM ts))::INT AS first_day_of_month
       , (DATE_TRUNC('MONTH', ts) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month
       , DATE_TRUNC('quarter', ts)::DATE AS first_day_of_quarter
       , (DATE_TRUNC('quarter', ts) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter
       , TO_DATE(EXTRACT(YEAR FROM ts) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year
       , TO_DATE(EXTRACT(YEAR FROM ts) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year
       , TO_CHAR(ts, 'mmyyyy') AS mmyyyy
       , replace(TO_CHAR(ts, 'mmddyyyy'), ' ', '') AS mmddyyyy
       , CASE
           WHEN EXTRACT(ISODOW FROM ts) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend
FROM (SELECT '2019-01-01'::DATE + SEQUENCE.DAY AS ts
      FROM GENERATE_SERIES(0, 18262) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

-- dwh.dim_branch

DROP SEQUENCE IF EXISTS dim_branch_null_sequence;

CREATE SEQUENCE dim_branch_null_sequence
    MINVALUE 0
    start 0
    increment 1;

CREATE TABLE dwh.dim_branch (
      id int UNIQUE NOT NULL DEFAULT nextval('dim_branch_null_sequence')
    , branch_id int
    , branch char(1)
    , city varchar(20)
    , start_ts date DEFAULT now()
    , end_ts date DEFAULT '2999-01-01'
    , is_current bool DEFAULT TRUE
);

INSERT INTO dwh.dim_branch (
      branch_id
    , branch
    , city
    , start_ts
    , end_ts
    , is_current
) VALUES (
      NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , NULL
);

CREATE OR REPLACE function dwh.dim_branch_scd2()
returns trigger 
AS $BODY$
    BEGIN
        IF exists(
            SELECT 1 
            FROM dwh.dim_branch 
            WHERE branch_id = NEW.branch_id 
            AND branch = NEW.branch
            AND city = NEW.city
            )
        THEN RETURN NULL;
        END IF;
        IF exists(
            SELECT 1
            FROM dwh.dim_branch
            WHERE branch_id = NEW.branch_id
            AND (
                branch <> NEW.branch
                OR city <> NEW.city
            )
        )
        THEN
            update dwh.dim_branch 
            set end_ts = now(),
            is_current = false
            where branch_id=new.branch_id
            AND end_ts = '2999-01-01';
            return new;
        END IF;
    RETURN NEW;
    END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER dim_branch_trigger before INSERT ON dwh.dim_branch
FOR EACH ROW EXECUTE PROCEDURE dwh.dim_branch_scd2();

CREATE OR REPLACE FUNCTION dwh.dim_branch_lookup_update(b_id int, b TEXT, c text) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM dwh.dim_branch 
        WHERE branch_id = b_id 
        AND branch = b
        AND city = c;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO dwh.dim_branch (
              branch_id
            , branch
            , city
        ) VALUES (
              b_id
            , b
            , c
        ) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

-- dwh.dim_customer_type

DROP SEQUENCE IF EXISTS dim_customer_type_null_sequence;

CREATE SEQUENCE dim_customer_type_null_sequence
    MINVALUE 0
    start 0
    increment 1;

CREATE TABLE dwh.dim_customer_type (
      id int UNIQUE NOT NULL DEFAULT nextval('dim_customer_type_null_sequence')
    , customer_type_id int
    , customer_type varchar(10)
    , start_ts date DEFAULT now()
    , end_ts date DEFAULT '2999-01-01'
    , is_current bool DEFAULT TRUE
);

INSERT INTO dwh.dim_customer_type (
      customer_type_id
    , customer_type
    , start_ts
    , end_ts
    , is_current
) VALUES (
      NULL
    , NULL
    , NULL
    , NULL
    , NULL
);

CREATE OR REPLACE function dwh.dim_customer_type_scd2()
returns trigger 
AS $BODY$
    BEGIN
        IF exists(
            SELECT 1 
            FROM dwh.dim_customer_type
            WHERE customer_type_id = NEW.customer_type_id
            AND customer_type = NEW.customer_type
            )
        THEN RETURN NULL;
        END IF;
        IF exists(
            SELECT 1
            FROM dwh.dim_customer_type
            WHERE customer_type_id = NEW.customer_type_id
            AND customer_type <> NEW.customer_type
        )
        THEN
            update dwh.dim_customer_type 
            set end_ts = now(),
            is_current = false
            where customer_type_id=new.customer_type_id
            AND end_ts = '2999-01-01';
            return new;
        END IF;
    RETURN NEW;
    END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER dim_customer_type_trigger before INSERT ON dwh.dim_customer_type
FOR EACH ROW EXECUTE PROCEDURE dwh.dim_customer_type_scd2();

CREATE OR REPLACE FUNCTION dwh.dim_customer_type_lookup_update(v_id int, v TEXT) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id
        INTO res
        FROM dwh.dim_customer_type 
        WHERE customer_type_id = v_id 
        AND customer_type = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO dwh.dim_customer_type (
              customer_type_id
            , customer_type
        ) VALUES (
              v_id
            , v
        ) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

-- dwh.dim_gender

DROP SEQUENCE IF EXISTS dim_gender_null_sequence;

CREATE SEQUENCE dim_gender_null_sequence
    MINVALUE 0
    start 0
    increment 1;

CREATE TABLE dwh.dim_gender (
      id int UNIQUE NOT NULL DEFAULT nextval('dim_gender_null_sequence')
    , gender_id int
    , gender varchar(10)
    , start_ts date DEFAULT now()
    , end_ts date DEFAULT '2999-01-01'
    , is_current bool DEFAULT TRUE
);

INSERT INTO dwh.dim_gender (
      gender_id
    , gender
    , start_ts
    , end_ts
    , is_current
) VALUES (
      NULL
    , NULL
    , NULL
    , NULL
    , NULL
);

CREATE OR REPLACE function dwh.dim_gender_scd2()
returns trigger 
AS $BODY$
    BEGIN
        IF exists(
            SELECT 1 
            FROM dwh.dim_gender
            WHERE gender_id = NEW.gender_id
            AND gender = NEW.gender
            )
        THEN RETURN NULL;
        END IF;
        IF exists(
            SELECT 1
            FROM dwh.dim_gender
            WHERE gender_id = NEW.gender_id
            AND gender <> NEW.gender
        )
        THEN
            update dwh.dim_gender 
            set end_ts = now(),
            is_current = false
            where gender_id=new.gender_id
            AND end_ts = '2999-01-01';
            return new;
        END IF;
    RETURN NEW;
    END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER dim_gender_trigger before INSERT ON dwh.dim_gender
FOR EACH ROW EXECUTE PROCEDURE dwh.dim_gender_scd2();

CREATE OR REPLACE FUNCTION dwh.dim_gender_lookup_update(v_id int, v TEXT) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM dwh.dim_gender 
        WHERE gender_id = v_id 
        AND gender = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO dwh.dim_gender (
              gender_id
            , gender
        ) VALUES (
              v_id
            , v
        ) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

-- dwh.dim_product_line

DROP SEQUENCE IF EXISTS dim_product_line_null_sequence;

CREATE SEQUENCE dim_product_line_null_sequence
    MINVALUE 0
    start 0
    increment 1;

CREATE TABLE dwh.dim_product_line (
      id int UNIQUE NOT NULL DEFAULT nextval('dim_product_line_null_sequence')
    , product_line_id int
    , product_line varchar(255)
    , start_ts date DEFAULT now()
    , end_ts date DEFAULT '2999-01-01'
    , is_current bool DEFAULT TRUE
);

INSERT INTO dwh.dim_product_line (
      product_line_id
    , product_line
    , start_ts
    , end_ts
    , is_current
) VALUES (
      NULL
    , NULL
    , NULL
    , NULL
    , NULL
);

CREATE OR REPLACE function dwh.dim_product_line_scd2()
returns trigger 
AS $BODY$
    BEGIN
        IF exists(
            SELECT 1 
            FROM dwh.dim_product_line
            WHERE product_line_id = NEW.product_line_id
            AND product_line = NEW.product_line
            )
        THEN RETURN NULL;
        END IF;
        IF exists(
            SELECT 1
            FROM dwh.dim_product_line
            WHERE product_line_id = NEW.product_line_id
            AND product_line <> NEW.product_line
        )
        THEN
            update dwh.dim_product_line 
            set end_ts = now(),
            is_current = false
            where product_line_id=new.product_line_id
            AND end_ts = '2999-01-01';
            return new;
        END IF;
    RETURN NEW;
    END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER dim_product_line_trigger before INSERT ON dwh.dim_product_line
FOR EACH ROW EXECUTE PROCEDURE dwh.dim_product_line_scd2();

CREATE OR REPLACE FUNCTION dwh.dim_product_line_lookup_update(v_id int, v TEXT) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM dwh.dim_product_line 
        WHERE product_line_id = v_id 
        AND product_line = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO dwh.dim_product_line (
              product_line_id
            , product_line
        ) VALUES (
              v_id
            , v
        ) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

-- dwh.dim_payment

DROP SEQUENCE IF EXISTS dim_payment_null_sequence;

CREATE SEQUENCE dim_payment_null_sequence
    MINVALUE 0
    start 0
    increment 1;

CREATE TABLE dwh.dim_payment (
      id int UNIQUE NOT NULL DEFAULT nextval('dim_payment_null_sequence')
    , payment_id int
    , payment varchar(32)
    , start_ts date DEFAULT now()
    , end_ts date DEFAULT '2999-01-01'
    , is_current bool DEFAULT TRUE
);

INSERT INTO dwh.dim_payment (
      payment_id
    , payment
    , start_ts
    , end_ts
    , is_current
) VALUES (
      NULL
    , NULL
    , NULL
    , NULL
    , NULL
);

CREATE OR REPLACE function dwh.dim_payment_scd2()
returns trigger 
AS $BODY$
    BEGIN
        IF EXISTS(
            SELECT 1 
            FROM dwh.dim_payment
            WHERE payment_id = NEW.payment_id
            AND payment = NEW.payment
            )
        THEN RETURN NULL;
        END IF;
        IF exists(
            SELECT 1
            FROM dwh.dim_payment
            WHERE payment_id = NEW.payment_id
            AND payment <> NEW.payment
        )
        THEN
            UPDATE dwh.dim_payment 
            SET end_ts = now(),
            is_current = false
            WHERE payment_id=new.payment_id
            AND end_ts = '2999-01-01';
            RETURN NEW;
        END IF;
    RETURN NEW;
    END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER dim_payment_trigger before INSERT ON dwh.dim_payment
FOR EACH ROW EXECUTE PROCEDURE dwh.dim_payment_scd2();

CREATE OR REPLACE FUNCTION dwh.dim_payment_lookup_update(v_id int, v TEXT) RETURNS int
AS $BODY$
DECLARE res int;
    BEGIN
        SELECT id 
        INTO res
        FROM dwh.dim_payment 
        WHERE payment_id = v_id 
        AND payment = v;
        IF res IS NOT NULL
        THEN RETURN res;
        END IF;
        INSERT INTO dwh.dim_payment (
              payment_id
            , payment
        ) VALUES (
              v_id
            , v
        ) RETURNING id INTO res;
        RETURN res;
    END
$BODY$
LANGUAGE plpgsql;

-- dwh.fact_sales

CREATE TABLE dwh.fact_sales (
      id serial PRIMARY KEY
    , "date" int REFERENCES dwh.dim_calendar(id)
    , "datetime" timestamp
    , invoice_id char(11)
    , branch_id int REFERENCES dwh.dim_branch(id)
    , customer_type_id int REFERENCES dwh.dim_customer_type(id)
    , gender_id int REFERENCES dwh.dim_gender(id)
    , product_line_id int REFERENCES dwh.dim_product_line(id)
    , payment_id int REFERENCES dwh.dim_payment(id)
    , unit_price float
    , quantity int
    , rating float
);

-- dwh.fact_sales_w

CREATE OR REPLACE VIEW dwh.fact_sales_w AS 
SELECT
      fs2.id
    , fs2.invoice_id
    , dc."date"
    , EXTRACT(epoch FROM fs2.datetime) :: int as "datetime"
    , EXTRACT(epoch FROM fs2.datetime) :: int as epoch
    , EXTRACT(HOUR FROM fs2.datetime) as "hour"
    , EXTRACT(MINUTE FROM fs2.datetime) as "minute"
    , dc.day_suffix 
    , dc.day_name 
    , dc.day_of_week 
    , dc.day_of_month 
    , dc.day_of_quarter 
    , dc.day_of_year 
    , dc.week_of_month 
    , dc.week_of_year 
    , dc.month_actual 
    , dc.month_name 
    , dc.month_name_short 
    , dc.quarter_actual 
    , dc.quarter_name 
    , dc.year_actual 
    , dc.first_day_of_week 
    , dc.last_day_of_week 
    , dc.first_day_of_month 
    , dc.last_day_of_month 
    , dc.first_day_of_quarter 
    , dc.last_day_of_quarter 
    , dc.first_day_of_year 
    , dc.last_day_of_year 
    , dc.mmyyyy 
    , dc.mmddyyyy 
    , dc.weekend 
    , db.branch 
    , db.city 
    , dct.customer_type
    , dg.gender 
    , dpl.product_line
    , dp.payment 
    , fs2.unit_price 
    , fs2.quantity
    , fs2.unit_price * fs2.quantity / 100 * 5 AS tax
    , fs2.unit_price * fs2.quantity + fs2.unit_price * fs2.quantity / 100 * 5 AS total
    , fs2.unit_price * fs2.quantity AS cogs
    , (fs2.unit_price * fs2.quantity + fs2.unit_price * fs2.quantity / 100 * 5 - fs2.unit_price * fs2.quantity)
        /(fs2.unit_price * fs2.quantity + fs2.unit_price * fs2.quantity / 100 * 5) * 100 AS gross_margine_percentage
    , fs2.unit_price * fs2.quantity / 100 * 5 AS gross_income
    , fs2.rating 
FROM dwh.fact_sales AS fs2 
JOIN dwh.dim_calendar AS dc ON dc.id = fs2."date" 
JOIN dwh.dim_branch AS db ON db.id = fs2.branch_id
JOIN dwh.dim_customer_type AS dct ON dct.id = fs2.customer_type_id 
JOIN dwh.dim_gender AS dg ON dg.id = fs2.gender_id 
JOIN dwh.dim_product_line AS dpl ON dpl.id = fs2.product_line_id 
JOIN dwh.dim_payment AS dp ON dp.id = fs2.payment_id;

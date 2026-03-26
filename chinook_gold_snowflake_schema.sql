-- =============================================================
-- Chinook Gold Layer — Snowflake Schema DDL
-- Compatible with: ER/Studio Data Architect (reverse engineer
-- via File > Reverse Engineer > From SQL Script)
-- Target dialect: ANSI SQL / SQL Server
-- Generated: 2026-03-26
--
-- Snowflake normalizations applied vs star schema:
--   dim_date        → dim_month → dim_quarter → dim_year
--   dim_track       → dim_album → dim_artist
--   dim_track       → dim_genre
--   dim_track       → dim_mediatype
--   dim_customer    → dim_geography (city/state/country/postal)
--   dim_employee    → dim_geography (shared geography dimension)
--   dim_employee    → self-referencing manager hierarchy
-- =============================================================


-- =============================================================
-- LEVEL 1 NORMALISED LOOKUP TABLES (leaf nodes)
-- =============================================================

CREATE TABLE dim_year (
    year_key            INT             NOT NULL,
    year                INT             NOT NULL,
    CONSTRAINT pk_dim_year PRIMARY KEY (year_key)
);

CREATE TABLE dim_quarter (
    quarter_key         INT             NOT NULL,
    year_key            INT             NOT NULL,
    quarter             INT             NOT NULL,     -- 1-4
    quarter_label       VARCHAR(10)     NOT NULL,     -- e.g. Q1-2013
    CONSTRAINT pk_dim_quarter PRIMARY KEY (quarter_key),
    CONSTRAINT fk_dim_quarter_year FOREIGN KEY (year_key)
        REFERENCES dim_year (year_key)
);

CREATE TABLE dim_month (
    month_key           INT             NOT NULL,
    quarter_key         INT             NOT NULL,
    month               INT             NOT NULL,     -- 1-12
    month_name          VARCHAR(20)     NOT NULL,
    CONSTRAINT pk_dim_month PRIMARY KEY (month_key),
    CONSTRAINT fk_dim_month_quarter FOREIGN KEY (quarter_key)
        REFERENCES dim_quarter (quarter_key)
);

CREATE TABLE dim_date (
    date_key            INT             NOT NULL,
    month_key           INT             NOT NULL,
    full_date           DATE            NOT NULL,
    day_of_month        INT             NOT NULL,
    day_of_week         INT             NOT NULL,
    day_name            VARCHAR(20)     NOT NULL,
    is_weekend          BIT             NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key),
    CONSTRAINT fk_dim_date_month FOREIGN KEY (month_key)
        REFERENCES dim_month (month_key)
);

-- =============================================================

CREATE TABLE dim_geography (
    geography_key       BIGINT          NOT NULL,
    address             VARCHAR(200)    NULL,
    city                VARCHAR(100)    NULL,
    state               VARCHAR(100)    NULL,
    country             VARCHAR(100)    NULL,
    postal_code         VARCHAR(20)     NULL,
    CONSTRAINT pk_dim_geography PRIMARY KEY (geography_key)
);

-- =============================================================

CREATE TABLE dim_artist (
    artist_key          BIGINT          NOT NULL,
    artist_id           INT             NOT NULL,
    artist_name         VARCHAR(200)    NULL,
    CONSTRAINT pk_dim_artist PRIMARY KEY (artist_key)
);

CREATE TABLE dim_album (
    album_key           BIGINT          NOT NULL,
    album_id            INT             NOT NULL,
    album_title         VARCHAR(300)    NOT NULL,
    artist_key          BIGINT          NOT NULL,
    CONSTRAINT pk_dim_album PRIMARY KEY (album_key),
    CONSTRAINT fk_dim_album_artist FOREIGN KEY (artist_key)
        REFERENCES dim_artist (artist_key)
);

CREATE TABLE dim_genre (
    genre_key           BIGINT          NOT NULL,
    genre_id            INT             NOT NULL,
    genre_name          VARCHAR(120)    NULL,
    CONSTRAINT pk_dim_genre PRIMARY KEY (genre_key)
);

CREATE TABLE dim_mediatype (
    media_type_key      BIGINT          NOT NULL,
    media_type_id       INT             NOT NULL,
    media_type_name     VARCHAR(120)    NULL,
    CONSTRAINT pk_dim_mediatype PRIMARY KEY (media_type_key)
);

CREATE TABLE dim_track (
    track_key           BIGINT          NOT NULL,
    track_id            INT             NOT NULL,
    track_name          VARCHAR(300)    NOT NULL,
    album_key           BIGINT          NULL,
    genre_key           BIGINT          NULL,
    media_type_key      BIGINT          NOT NULL,
    composer            VARCHAR(300)    NULL,
    duration_ms         INT             NULL,
    file_size_bytes     INT             NULL,
    unit_price          DECIMAL(10,2)   NOT NULL,
    CONSTRAINT pk_dim_track PRIMARY KEY (track_key),
    CONSTRAINT fk_dim_track_album      FOREIGN KEY (album_key)
        REFERENCES dim_album (album_key),
    CONSTRAINT fk_dim_track_genre      FOREIGN KEY (genre_key)
        REFERENCES dim_genre (genre_key),
    CONSTRAINT fk_dim_track_mediatype  FOREIGN KEY (media_type_key)
        REFERENCES dim_mediatype (media_type_key)
);

CREATE TABLE dim_playlist (
    playlist_key        BIGINT          NOT NULL,
    playlist_id         INT             NOT NULL,
    playlist_name       VARCHAR(200)    NULL,
    CONSTRAINT pk_dim_playlist PRIMARY KEY (playlist_key)
);

-- =============================================================
-- DIMENSION TABLES (normalised — reference lookup tables above)
-- =============================================================

CREATE TABLE dim_employee (
    employee_key            BIGINT          NOT NULL,
    employee_id             INT             NOT NULL,
    first_name              VARCHAR(100)    NOT NULL,
    last_name               VARCHAR(100)    NOT NULL,
    title                   VARCHAR(100)    NULL,
    manager_employee_key    BIGINT          NULL,     -- self-referencing hierarchy
    geography_key           BIGINT          NULL,
    birth_date              DATETIME        NULL,
    hire_date               DATETIME        NULL,
    phone                   VARCHAR(50)     NULL,
    fax                     VARCHAR(50)     NULL,
    email                   VARCHAR(200)    NULL,
    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_key),
    CONSTRAINT fk_dim_employee_geography FOREIGN KEY (geography_key)
        REFERENCES dim_geography (geography_key),
    CONSTRAINT fk_dim_employee_manager  FOREIGN KEY (manager_employee_key)
        REFERENCES dim_employee (employee_key)
);

CREATE TABLE dim_customer (
    customer_key            BIGINT          NOT NULL,
    customer_id             INT             NOT NULL,
    first_name              VARCHAR(100)    NOT NULL,
    last_name               VARCHAR(100)    NOT NULL,
    company                 VARCHAR(200)    NULL,
    phone                   VARCHAR(50)     NULL,
    fax                     VARCHAR(50)     NULL,
    email                   VARCHAR(200)    NOT NULL,
    geography_key           BIGINT          NULL,
    support_rep_key         BIGINT          NULL,
    effective_start_date    DATE            NOT NULL,
    effective_end_date      DATE            NULL,
    is_current              BIT             NOT NULL,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key),
    CONSTRAINT fk_dim_customer_geography   FOREIGN KEY (geography_key)
        REFERENCES dim_geography (geography_key),
    CONSTRAINT fk_dim_customer_support_rep FOREIGN KEY (support_rep_key)
        REFERENCES dim_employee (employee_key)
);


-- =============================================================
-- FACT TABLES
-- =============================================================

CREATE TABLE fact_sales (
    invoice_line_id     BIGINT          NOT NULL,
    invoice_id          BIGINT          NOT NULL,
    customer_key        BIGINT          NOT NULL,
    track_key           BIGINT          NOT NULL,
    date_key            INT             NOT NULL,
    support_rep_key     BIGINT          NULL,
    quantity            INT             NOT NULL,
    unit_price          DECIMAL(10,2)   NOT NULL,
    line_total          DECIMAL(10,2)   NOT NULL,
    billing_geography_key BIGINT        NULL,
    CONSTRAINT pk_fact_sales PRIMARY KEY (invoice_line_id),
    CONSTRAINT fk_fact_sales_customer   FOREIGN KEY (customer_key)
        REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_fact_sales_track      FOREIGN KEY (track_key)
        REFERENCES dim_track (track_key),
    CONSTRAINT fk_fact_sales_date       FOREIGN KEY (date_key)
        REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_sales_employee   FOREIGN KEY (support_rep_key)
        REFERENCES dim_employee (employee_key),
    CONSTRAINT fk_fact_sales_billing_geo FOREIGN KEY (billing_geography_key)
        REFERENCES dim_geography (geography_key)
);

CREATE TABLE fact_sales_customer_agg (
    customer_key            BIGINT          NOT NULL,
    total_invoices          BIGINT          NOT NULL,
    total_quantity          BIGINT          NOT NULL,
    total_revenue           DECIMAL(14,2)   NOT NULL,
    avg_line_total          DECIMAL(14,4)   NULL,
    last_purchase_date_key  INT             NULL,
    CONSTRAINT pk_fact_sales_customer_agg PRIMARY KEY (customer_key),
    CONSTRAINT fk_fact_agg_customer      FOREIGN KEY (customer_key)
        REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_fact_agg_last_date     FOREIGN KEY (last_purchase_date_key)
        REFERENCES dim_date (date_key)
);


-- =============================================================
-- END OF SCRIPT
-- =============================================================

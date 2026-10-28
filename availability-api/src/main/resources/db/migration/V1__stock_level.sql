CREATE TABLE stock_level (
    sku         VARCHAR(32)     NOT NULL,
    location    VARCHAR(16)     NOT NULL,
    on_hand     NUMERIC(12, 2)  NOT NULL,
    reserved    NUMERIC(12, 2)  NOT NULL,
    available   NUMERIC(12, 2)  NOT NULL,
    changed_at  TIMESTAMPTZ     NOT NULL,
    source_lsn  VARCHAR(32)     NOT NULL,
    updated_at  TIMESTAMPTZ     NOT NULL,
    PRIMARY KEY (sku, location)
);

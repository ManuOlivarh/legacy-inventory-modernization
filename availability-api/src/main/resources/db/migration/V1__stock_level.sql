-- One row per stock position, as the ERP last reported it. The primary key also
-- serves lookups by SKU, the only query the API makes.
CREATE TABLE stock_level (
    sku         VARCHAR(32)     NOT NULL,
    location    VARCHAR(16)     NOT NULL,
    on_hand     NUMERIC(12, 2)  NOT NULL,
    reserved    NUMERIC(12, 2)  NOT NULL,
    available   NUMERIC(12, 2)  NOT NULL,
    -- ERP time of this value, already converted to UTC by the translator.
    changed_at  TIMESTAMPTZ     NOT NULL,
    -- SQL Server commit LSN of this value; older events never overwrite newer ones.
    source_lsn  VARCHAR(32)     NOT NULL,
    updated_at  TIMESTAMPTZ     NOT NULL,
    PRIMARY KEY (sku, location)
);

-- Every applied fill, keyed by its Kafka coordinates. The primary key is the idempotency
-- boundary: a redelivered or retried record hits ORA-00001 instead of changing a position a
-- second time. signed_size carries the taker's signed quantity (+bought, -sold).
CREATE TABLE fills (
    source_topic     VARCHAR2(64 CHAR) NOT NULL,
    source_partition NUMBER(10)        NOT NULL,
    source_offset    NUMBER(19)        NOT NULL,
    symbol           VARCHAR2(32 CHAR) NOT NULL,
    price            NUMBER(27, 8)     NOT NULL,
    signed_size      NUMBER(19)        NOT NULL,
    maker_order_id   NUMBER(19)        NOT NULL,
    taker_order_id   NUMBER(19)        NOT NULL,
    time_millis      NUMBER(19)        NOT NULL,
    CONSTRAINT fills_pk PRIMARY KEY (source_topic, source_partition, source_offset)
);

-- Net position per symbol, booked from the taker side of each fill off orderbook.fills.
-- last_price keeps the egress's exact decimal form (orderbook prices carry 8 decimal places).
CREATE TABLE positions (
    symbol           VARCHAR2(32 CHAR) PRIMARY KEY,
    quantity         NUMBER(19)        NOT NULL,
    last_price       NUMBER(27, 8)     NOT NULL,
    last_time_millis NUMBER(19)        NOT NULL
);

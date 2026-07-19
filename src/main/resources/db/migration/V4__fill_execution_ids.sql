-- Execution identity from the producer. Stream coordinates identify a record; exec_id
-- identifies the execution — so the same economic fill republished at new coordinates (a
-- dead-letter replay, a redelivery through another topic) is still recognised as a duplicate.
-- Nullable: records published before the id existed dedupe by coordinates alone, and Oracle's
-- unique index ignores entirely-null entries.
ALTER TABLE fills ADD (exec_id VARCHAR2(64 CHAR));
CREATE UNIQUE INDEX fills_exec_id_uk ON fills (exec_id);

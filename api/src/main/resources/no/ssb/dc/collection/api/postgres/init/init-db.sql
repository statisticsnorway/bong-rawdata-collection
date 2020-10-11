DROP TABLE IF EXISTS "TOPIC_meta_item";
DROP TABLE IF EXISTS "TOPIC_record_item";

CREATE TABLE "TOPIC_meta_item"
(
    key   bytea                    NOT NULL,
    value bytea                    NOT NULL,
    ts    timestamp with time zone NOT NULL,
    PRIMARY KEY (key)
);

CREATE TABLE "TOPIC_record_item"
(
    key   bytea                    NOT NULL,
    value bytea                    NOT NULL,
    ts    timestamp with time zone NOT NULL,
    PRIMARY KEY (key)
);

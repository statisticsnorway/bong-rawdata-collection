DROP TABLE IF EXISTS "TOPIC_bong_item";

CREATE TABLE "TOPIC_bong_item"
(
    key   bytea                    NOT NULL,
    value bytea                    NOT NULL,
    ts    timestamp with time zone NOT NULL,
    PRIMARY KEY (key)
);


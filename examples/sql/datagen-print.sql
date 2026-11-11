CREATE TABLE datagen_source (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TABLE print_sink (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'print'
);

INSERT INTO print_sink
SELECT id, name
FROM datagen_source;

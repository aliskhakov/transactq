CREATE TABLE queue
(
    id bigserial PRIMARY KEY NOT NULL,
    name varchar(255) NOT NULL
);
CREATE UNIQUE INDEX queue_name_uindex ON queue (name);

CREATE TABLE public.message
(
    id bigserial PRIMARY KEY NOT NULL,
    queue_id bigint NOT NULL,
    payload text,
    created_at timestamp DEFAULT current_timestamp NOT NULL,
    CONSTRAINT message_queue_id_fk FOREIGN KEY (queue_id) REFERENCES queue (id)
);
CREATE INDEX message_queue_id_index ON message (queue_id);
CREATE INDEX message_created_at_index ON message (created_at);

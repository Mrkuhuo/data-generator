ALTER TABLE target_connection
    ADD COLUMN config_json LONGTEXT NULL AFTER jdbc_params;

ALTER TABLE write_task
    ADD COLUMN target_config_json LONGTEXT NULL AFTER description;

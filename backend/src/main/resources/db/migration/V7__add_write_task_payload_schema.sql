ALTER TABLE write_task
    ADD COLUMN payload_schema_json LONGTEXT NULL AFTER target_config_json;

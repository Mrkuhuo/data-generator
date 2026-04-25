ALTER TABLE write_task_relation
    ADD COLUMN relation_mode VARCHAR(40) NOT NULL DEFAULT 'DATABASE_COLUMNS',
    ADD COLUMN mapping_config_json LONGTEXT NULL;

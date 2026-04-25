CREATE TABLE target_connection (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(120) NOT NULL,
    db_type VARCHAR(20) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INT NOT NULL,
    database_name VARCHAR(120) NOT NULL,
    schema_name VARCHAR(120) NULL,
    username VARCHAR(120) NOT NULL,
    password_value VARCHAR(255) NOT NULL,
    jdbc_params VARCHAR(1000) NULL,
    status VARCHAR(20) NOT NULL,
    description VARCHAR(1000) NULL,
    last_test_status VARCHAR(120) NULL,
    last_test_message VARCHAR(1000) NULL,
    last_test_details_json LONGTEXT NULL,
    last_tested_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE write_task (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(120) NOT NULL,
    connection_id BIGINT NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    table_mode VARCHAR(30) NOT NULL,
    write_mode VARCHAR(30) NOT NULL,
    row_count INT NOT NULL,
    batch_size INT NOT NULL,
    seed BIGINT NULL,
    status VARCHAR(20) NOT NULL,
    schedule_type VARCHAR(20) NOT NULL,
    cron_expression VARCHAR(120) NULL,
    description VARCHAR(1000) NULL,
    last_triggered_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_write_task_connection FOREIGN KEY (connection_id) REFERENCES target_connection (id)
);

CREATE TABLE write_task_column (
    id BIGINT NOT NULL AUTO_INCREMENT,
    task_id BIGINT NOT NULL,
    column_name VARCHAR(120) NOT NULL,
    db_type VARCHAR(80) NOT NULL,
    length_value INT NULL,
    precision_value INT NULL,
    scale_value INT NULL,
    nullable_flag BIT NOT NULL,
    primary_key_flag BIT NOT NULL,
    generator_type VARCHAR(40) NOT NULL,
    generator_config_json LONGTEXT NOT NULL,
    sort_order INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_write_task_column_task FOREIGN KEY (task_id) REFERENCES write_task (id)
);

CREATE INDEX idx_target_connection_db_type ON target_connection (db_type);
CREATE INDEX idx_target_connection_status ON target_connection (status);
CREATE INDEX idx_write_task_connection_id ON write_task (connection_id);
CREATE INDEX idx_write_task_status ON write_task (status);
CREATE INDEX idx_write_task_column_task_id ON write_task_column (task_id);

ALTER TABLE write_task
    ADD COLUMN group_id BIGINT NULL,
    ADD COLUMN task_key VARCHAR(120) NULL,
    ADD COLUMN row_plan_json LONGTEXT NULL;

ALTER TABLE write_task_column
    ADD COLUMN foreign_key_flag TINYINT(1) NOT NULL DEFAULT 0,
    ADD COLUMN reference_config_json LONGTEXT NULL;

CREATE TABLE write_task_group (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    name VARCHAR(120) NOT NULL,
    connection_id BIGINT NOT NULL,
    description VARCHAR(1000) NULL,
    seed BIGINT NULL,
    status VARCHAR(20) NOT NULL,
    CONSTRAINT fk_write_task_group_connection
        FOREIGN KEY (connection_id) REFERENCES target_connection(id)
);

CREATE TABLE write_task_relation (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    group_id BIGINT NOT NULL,
    relation_name VARCHAR(160) NOT NULL,
    parent_task_id BIGINT NOT NULL,
    child_task_id BIGINT NOT NULL,
    relation_type VARCHAR(40) NOT NULL,
    source_mode VARCHAR(30) NOT NULL,
    selection_strategy VARCHAR(30) NOT NULL,
    reuse_policy VARCHAR(30) NOT NULL,
    parent_columns_json LONGTEXT NOT NULL,
    child_columns_json LONGTEXT NOT NULL,
    null_rate DECIMAL(10,4) NOT NULL DEFAULT 0,
    mixed_existing_ratio DECIMAL(10,4) NULL,
    min_children_per_parent INT NULL,
    max_children_per_parent INT NULL,
    sort_order INT NOT NULL DEFAULT 0,
    CONSTRAINT fk_write_task_relation_group
        FOREIGN KEY (group_id) REFERENCES write_task_group(id) ON DELETE CASCADE,
    CONSTRAINT fk_write_task_relation_parent_task
        FOREIGN KEY (parent_task_id) REFERENCES write_task(id) ON DELETE CASCADE,
    CONSTRAINT fk_write_task_relation_child_task
        FOREIGN KEY (child_task_id) REFERENCES write_task(id) ON DELETE CASCADE
);

CREATE TABLE write_task_group_execution (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    write_task_group_id BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    planned_table_count INT NOT NULL DEFAULT 0,
    completed_table_count INT NOT NULL DEFAULT 0,
    success_table_count INT NOT NULL DEFAULT 0,
    failure_table_count INT NOT NULL DEFAULT 0,
    error_summary VARCHAR(1000) NULL,
    summary_json LONGTEXT NULL,
    CONSTRAINT fk_write_task_group_execution_group
        FOREIGN KEY (write_task_group_id) REFERENCES write_task_group(id) ON DELETE CASCADE
);

CREATE TABLE write_task_group_table_execution (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    write_task_group_execution_id BIGINT NOT NULL,
    write_task_id BIGINT NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL,
    before_write_row_count BIGINT NULL,
    after_write_row_count BIGINT NULL,
    inserted_count BIGINT NOT NULL DEFAULT 0,
    null_violation_count BIGINT NOT NULL DEFAULT 0,
    blank_string_count BIGINT NOT NULL DEFAULT 0,
    fk_miss_count BIGINT NOT NULL DEFAULT 0,
    pk_duplicate_count BIGINT NOT NULL DEFAULT 0,
    summary_json LONGTEXT NULL,
    error_summary VARCHAR(1000) NULL,
    CONSTRAINT fk_write_task_group_table_execution_execution
        FOREIGN KEY (write_task_group_execution_id) REFERENCES write_task_group_execution(id) ON DELETE CASCADE,
    CONSTRAINT fk_write_task_group_table_execution_task
        FOREIGN KEY (write_task_id) REFERENCES write_task(id) ON DELETE CASCADE
);

CREATE INDEX idx_write_task_group_connection ON write_task_group(connection_id);
CREATE INDEX idx_write_task_group_task_group_id ON write_task(group_id);
CREATE UNIQUE INDEX idx_write_task_group_task_key ON write_task(group_id, task_key);
CREATE INDEX idx_write_task_relation_group_id ON write_task_relation(group_id);
CREATE INDEX idx_write_task_group_execution_group_id ON write_task_group_execution(write_task_group_id);
CREATE INDEX idx_write_task_group_table_execution_execution_id ON write_task_group_table_execution(write_task_group_execution_id);

ALTER TABLE write_task
    ADD CONSTRAINT fk_write_task_group
        FOREIGN KEY (group_id) REFERENCES write_task_group(id) ON DELETE CASCADE;

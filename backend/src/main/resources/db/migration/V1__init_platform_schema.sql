CREATE TABLE connector_instance (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(120) NOT NULL,
    connector_type VARCHAR(40) NOT NULL,
    connector_role VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    description VARCHAR(1000) NULL,
    config_json LONGTEXT NOT NULL,
    last_test_status VARCHAR(120) NULL,
    last_test_message VARCHAR(1000) NULL,
    last_test_details_json LONGTEXT NULL,
    last_tested_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE dataset_definition (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(120) NOT NULL,
    category VARCHAR(80) NULL,
    version VARCHAR(30) NOT NULL,
    status VARCHAR(20) NOT NULL,
    description VARCHAR(1000) NULL,
    schema_json LONGTEXT NOT NULL,
    sample_config_json LONGTEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE job_definition (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(120) NOT NULL,
    dataset_definition_id BIGINT NOT NULL,
    target_connector_id BIGINT NOT NULL,
    write_strategy VARCHAR(20) NOT NULL,
    schedule_type VARCHAR(20) NOT NULL,
    cron_expression VARCHAR(120) NULL,
    status VARCHAR(20) NOT NULL,
    runtime_config_json LONGTEXT NOT NULL,
    last_triggered_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_job_dataset FOREIGN KEY (dataset_definition_id) REFERENCES dataset_definition (id),
    CONSTRAINT fk_job_target_connector FOREIGN KEY (target_connector_id) REFERENCES connector_instance (id)
);

CREATE TABLE job_execution (
    id BIGINT NOT NULL AUTO_INCREMENT,
    job_definition_id BIGINT NOT NULL,
    trigger_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NULL,
    generated_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    error_count BIGINT NOT NULL DEFAULT 0,
    error_summary VARCHAR(1000) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_execution_job FOREIGN KEY (job_definition_id) REFERENCES job_definition (id)
);

CREATE TABLE job_execution_log (
    id BIGINT NOT NULL AUTO_INCREMENT,
    job_execution_id BIGINT NOT NULL,
    log_level VARCHAR(10) NOT NULL,
    message VARCHAR(1000) NOT NULL,
    detail_json LONGTEXT NULL,
    logged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_execution_log_execution FOREIGN KEY (job_execution_id) REFERENCES job_execution (id)
);

CREATE INDEX idx_connector_instance_type ON connector_instance (connector_type);
CREATE INDEX idx_dataset_definition_status ON dataset_definition (status);
CREATE INDEX idx_job_definition_status ON job_definition (status);
CREATE INDEX idx_job_execution_status ON job_execution (status);
CREATE INDEX idx_job_execution_job_id ON job_execution (job_definition_id);
CREATE INDEX idx_job_execution_log_execution_id ON job_execution_log (job_execution_id);

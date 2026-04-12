CREATE DATABASE IF NOT EXISTS demo_sink;

USE demo_sink;

CREATE TABLE IF NOT EXISTS synthetic_user_activity (
    userId BIGINT NULL,
    city VARCHAR(120) NULL,
    score DECIMAL(10, 2) NULL,
    active BOOLEAN NULL,
    createdAt VARCHAR(64) NULL,
    profile JSON NULL,
    tags JSON NULL,
    email VARCHAR(255) NULL
);

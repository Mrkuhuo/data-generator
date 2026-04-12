CREATE TABLE IF NOT EXISTS synthetic_user_activity (
    "userId" BIGINT NULL,
    city VARCHAR(120) NULL,
    score NUMERIC(10, 2) NULL,
    active BOOLEAN NULL,
    "createdAt" VARCHAR(64) NULL,
    profile JSONB NULL,
    tags JSONB NULL,
    email VARCHAR(255) NULL
);

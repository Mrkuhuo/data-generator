IF DB_ID(N'mdg_demo') IS NULL
BEGIN
    CREATE DATABASE [mdg_demo];
END
GO

USE [mdg_demo];
GO

IF OBJECT_ID(N'dbo.synthetic_user_activity', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.synthetic_user_activity (
        id BIGINT NOT NULL PRIMARY KEY,
        user_code NVARCHAR(32) NOT NULL,
        city NVARCHAR(32) NOT NULL,
        created_at DATETIME2 NOT NULL
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM dbo.synthetic_user_activity
    WHERE id = 1
)
BEGIN
    INSERT INTO dbo.synthetic_user_activity (id, user_code, city, created_at)
    VALUES
        (1, N'DEMO-001', N'上海', SYSUTCDATETIME()),
        (2, N'DEMO-002', N'北京', SYSUTCDATETIME());
END
GO

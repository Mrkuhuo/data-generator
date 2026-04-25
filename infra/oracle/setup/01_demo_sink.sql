WHENEVER SQLERROR EXIT SQL.SQLCODE
SET DEFINE OFF
PROMPT [mdg-oracle] prepare demo schema in FREEPDB1

ALTER SESSION SET CONTAINER = FREEPDB1;

DECLARE
    user_count NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO user_count
      FROM dba_users
     WHERE username = 'MDG_DEMO';

    IF user_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER MDG_DEMO IDENTIFIED BY "MdgDemo123!" QUOTA UNLIMITED ON USERS';
        EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE TRIGGER TO MDG_DEMO';
    END IF;
END;
/

DECLARE
    table_count NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO table_count
      FROM all_tables
     WHERE owner = 'MDG_DEMO'
       AND table_name = 'SYNTHETIC_USER_ACTIVITY';

    IF table_count = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE TABLE MDG_DEMO.SYNTHETIC_USER_ACTIVITY (
                ID NUMBER(19) NOT NULL PRIMARY KEY,
                USER_CODE VARCHAR2(32) NOT NULL,
                CITY VARCHAR2(32) NOT NULL,
                CREATED_AT TIMESTAMP NOT NULL
            )
        ]';
    END IF;
END;
/

MERGE INTO MDG_DEMO.SYNTHETIC_USER_ACTIVITY target
USING (
    SELECT 1 AS id, 'DEMO-001' AS user_code, 'Shanghai' AS city, SYSTIMESTAMP AS created_at FROM dual
    UNION ALL
    SELECT 2 AS id, 'DEMO-002' AS user_code, 'Beijing' AS city, SYSTIMESTAMP AS created_at FROM dual
) source
ON (target.ID = source.id)
WHEN NOT MATCHED THEN
    INSERT (ID, USER_CODE, CITY, CREATED_AT)
    VALUES (source.id, source.user_code, source.city, source.created_at);

COMMIT;
PROMPT [mdg-oracle] demo schema ready

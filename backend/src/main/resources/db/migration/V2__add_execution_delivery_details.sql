ALTER TABLE job_execution
    ADD COLUMN delivery_details_json LONGTEXT NULL AFTER error_summary;

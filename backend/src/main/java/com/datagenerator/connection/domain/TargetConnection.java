package com.datagenerator.connection.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "target_connection")
public class TargetConnection extends BaseEntity {

    @Column(nullable = false, length = 120)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "db_type", nullable = false, length = 20)
    private DatabaseType dbType;

    @Column(nullable = false, length = 255)
    private String host;

    @Column(nullable = false)
    private Integer port;

    @Column(name = "database_name", nullable = false, length = 120)
    private String databaseName;

    @Column(name = "schema_name", length = 120)
    private String schemaName;

    @Column(nullable = false, length = 120)
    private String username;

    @Column(name = "password_value", nullable = false, length = 255)
    private String passwordValue;

    @Column(name = "jdbc_params", length = 1000)
    private String jdbcParams;

    @Lob
    @Column(name = "config_json", columnDefinition = "LONGTEXT")
    private String configJson;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TargetConnectionStatus status = TargetConnectionStatus.DRAFT;

    @Column(length = 1000)
    private String description;

    @Column(name = "last_test_status", length = 120)
    private String lastTestStatus;

    @Column(name = "last_test_message", length = 1000)
    private String lastTestMessage;

    @Lob
    @Column(name = "last_test_details_json", columnDefinition = "LONGTEXT")
    private String lastTestDetailsJson;

    @Column(name = "last_tested_at")
    private Instant lastTestedAt;
}

package com.datagenerator.connector.domain;

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
@Table(name = "connector_instance")
public class ConnectorInstance extends BaseEntity {

    @Column(nullable = false, length = 120)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "connector_type", nullable = false, length = 40)
    private ConnectorType connectorType;

    @Enumerated(EnumType.STRING)
    @Column(name = "connector_role", nullable = false, length = 20)
    private ConnectorRole connectorRole = ConnectorRole.TARGET;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ConnectorStatus status = ConnectorStatus.DRAFT;

    @Column(length = 1000)
    private String description;

    @Lob
    @Column(name = "config_json", nullable = false, columnDefinition = "LONGTEXT")
    private String configJson = "{}";

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

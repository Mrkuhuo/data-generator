package com.datagenerator.dataset.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "dataset_definition")
public class DatasetDefinition extends BaseEntity {

    @Column(nullable = false, length = 120)
    private String name;

    @Column(length = 80)
    private String category;

    @Column(nullable = false, length = 30)
    private String version = "v1";

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private DatasetStatus status = DatasetStatus.DRAFT;

    @Column(length = 1000)
    private String description;

    @Lob
    @Column(name = "schema_json", nullable = false, columnDefinition = "LONGTEXT")
    private String schemaJson = "{}";

    @Lob
    @Column(name = "sample_config_json", nullable = false, columnDefinition = "LONGTEXT")
    private String sampleConfigJson = "{}";
}

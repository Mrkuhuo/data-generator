package com.datagenerator.task.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "write_task_column")
public class WriteTaskColumn extends BaseEntity {

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "task_id", nullable = false)
    private WriteTask task;

    @Column(name = "column_name", nullable = false, length = 120)
    private String columnName;

    @Column(name = "db_type", nullable = false, length = 80)
    private String dbType;

    @Column(name = "length_value")
    private Integer lengthValue;

    @Column(name = "precision_value")
    private Integer precisionValue;

    @Column(name = "scale_value")
    private Integer scaleValue;

    @Column(name = "nullable_flag", nullable = false)
    private boolean nullableFlag = true;

    @Column(name = "primary_key_flag", nullable = false)
    private boolean primaryKeyFlag;

    @Column(name = "foreign_key_flag", nullable = false)
    private boolean foreignKeyFlag;

    @Enumerated(EnumType.STRING)
    @Column(name = "generator_type", nullable = false, length = 40)
    private ColumnGeneratorType generatorType;

    @Lob
    @Column(name = "generator_config_json", nullable = false, columnDefinition = "LONGTEXT")
    private String generatorConfigJson = "{}";

    @Lob
    @Column(name = "reference_config_json", columnDefinition = "LONGTEXT")
    private String referenceConfigJson;

    @Column(name = "sort_order", nullable = false)
    private Integer sortOrder = 0;
}

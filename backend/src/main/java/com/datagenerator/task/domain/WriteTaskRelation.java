package com.datagenerator.task.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "write_task_relation")
public class WriteTaskRelation extends BaseEntity {

    @Column(name = "group_id", nullable = false)
    private Long groupId;

    @Column(name = "relation_name", nullable = false, length = 160)
    private String relationName;

    @Column(name = "parent_task_id", nullable = false)
    private Long parentTaskId;

    @Column(name = "child_task_id", nullable = false)
    private Long childTaskId;

    @Enumerated(EnumType.STRING)
    @Column(name = "relation_mode", nullable = false, length = 40)
    private WriteTaskRelationMode relationMode = WriteTaskRelationMode.DATABASE_COLUMNS;

    @Enumerated(EnumType.STRING)
    @Column(name = "relation_type", nullable = false, length = 40)
    private WriteTaskRelationType relationType = WriteTaskRelationType.ONE_TO_MANY;

    @Enumerated(EnumType.STRING)
    @Column(name = "source_mode", nullable = false, length = 30)
    private ReferenceSourceMode sourceMode = ReferenceSourceMode.CURRENT_BATCH;

    @Enumerated(EnumType.STRING)
    @Column(name = "selection_strategy", nullable = false, length = 30)
    private RelationSelectionStrategy selectionStrategy = RelationSelectionStrategy.RANDOM_UNIFORM;

    @Enumerated(EnumType.STRING)
    @Column(name = "reuse_policy", nullable = false, length = 30)
    private RelationReusePolicy reusePolicy = RelationReusePolicy.ALLOW_REPEAT;

    @Lob
    @Column(name = "parent_columns_json", nullable = false, columnDefinition = "LONGTEXT")
    private String parentColumnsJson;

    @Lob
    @Column(name = "child_columns_json", nullable = false, columnDefinition = "LONGTEXT")
    private String childColumnsJson;

    @Column(name = "null_rate", nullable = false, precision = 10, scale = 4)
    private BigDecimal nullRate = BigDecimal.ZERO;

    @Column(name = "mixed_existing_ratio", precision = 10, scale = 4)
    private BigDecimal mixedExistingRatio;

    @Column(name = "min_children_per_parent")
    private Integer minChildrenPerParent;

    @Column(name = "max_children_per_parent")
    private Integer maxChildrenPerParent;

    @Lob
    @Column(name = "mapping_config_json", columnDefinition = "LONGTEXT")
    private String mappingConfigJson;

    @Column(name = "sort_order", nullable = false)
    private Integer sortOrder = 0;
}

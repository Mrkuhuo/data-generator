package com.datagenerator.task.repository;

import com.datagenerator.task.domain.WriteTaskRelation;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WriteTaskRelationRepository extends JpaRepository<WriteTaskRelation, Long> {

    List<WriteTaskRelation> findByGroupIdOrderBySortOrderAscIdAsc(Long groupId);
}

package com.datagenerator.dataset.application;

import com.datagenerator.dataset.api.DatasetUpsertRequest;
import com.datagenerator.dataset.domain.DatasetDefinition;
import com.datagenerator.dataset.domain.DatasetStatus;
import com.datagenerator.dataset.repository.DatasetDefinitionRepository;
import java.util.List;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DatasetService {

    private final DatasetDefinitionRepository datasetRepository;

    public DatasetService(DatasetDefinitionRepository datasetRepository) {
        this.datasetRepository = datasetRepository;
    }

    public List<DatasetDefinition> findAll() {
        return datasetRepository.findAll(Sort.by(Sort.Direction.DESC, "updatedAt"));
    }

    public DatasetDefinition findById(Long id) {
        return datasetRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("未找到数据集：" + id));
    }

    @Transactional
    public DatasetDefinition create(DatasetUpsertRequest request) {
        DatasetDefinition dataset = new DatasetDefinition();
        apply(dataset, request);
        return datasetRepository.save(dataset);
    }

    @Transactional
    public DatasetDefinition update(Long id, DatasetUpsertRequest request) {
        DatasetDefinition dataset = findById(id);
        apply(dataset, request);
        return datasetRepository.save(dataset);
    }

    @Transactional
    public void delete(Long id) {
        datasetRepository.deleteById(id);
    }

    public long count() {
        return datasetRepository.count();
    }

    @Transactional
    public DatasetDefinition createExample() {
        DatasetDefinition dataset = new DatasetDefinition();
        dataset.setName("示例用户行为数据");
        dataset.setCategory("快速开始");
        dataset.setVersion("v1");
        dataset.setStatus(DatasetStatus.READY);
        dataset.setDescription("覆盖 sequence、enum、template、object、array、datetime 等规则的入门示例数据集。");
        dataset.setSchemaJson("""
                {
                  "type": "object",
                  "fields": {
                    "userId": { "rule": "sequence", "start": 10001, "step": 1 },
                    "city": { "rule": "weighted_enum", "options": [
                      { "value": "Shanghai", "weight": 4 },
                      { "value": "Beijing", "weight": 3 },
                      { "value": "Shenzhen", "weight": 2 },
                      { "value": "Hangzhou", "weight": 1 }
                    ]},
                    "score": { "rule": "random_decimal", "min": 60, "max": 99.99, "scale": 2 },
                    "active": { "rule": "boolean", "trueRate": 0.82 },
                    "createdAt": { "rule": "datetime", "from": "2025-01-01T00:00:00Z", "to": "2025-12-31T23:59:59Z" },
                    "profile": {
                      "rule": "object",
                      "fields": {
                        "deviceId": { "rule": "string", "prefix": "dev-", "length": 10 },
                        "channel": { "rule": "enum", "values": ["app", "web", "mini-program"] }
                      }
                    },
                    "tags": {
                      "rule": "array",
                      "sizeMin": 1,
                      "sizeMax": 3,
                      "item": { "rule": "enum", "values": ["new", "vip", "trial", "returning"] }
                    },
                    "email": { "rule": "template", "template": "user-${userId}@demo.local" }
                  }
                }
                """);
        dataset.setSampleConfigJson("""
                {
                  "count": 5,
                  "seed": 20260412
                }
                """);
        return datasetRepository.save(dataset);
    }

    private void apply(DatasetDefinition dataset, DatasetUpsertRequest request) {
        dataset.setName(request.name());
        dataset.setCategory(request.category());
        dataset.setVersion(request.version());
        dataset.setStatus(request.status());
        dataset.setDescription(request.description());
        dataset.setSchemaJson(request.schemaJson());
        dataset.setSampleConfigJson(request.sampleConfigJson());
    }
}

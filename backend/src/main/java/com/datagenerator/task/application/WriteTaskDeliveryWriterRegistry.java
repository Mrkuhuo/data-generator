package com.datagenerator.task.application;

import com.datagenerator.connection.domain.DatabaseType;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class WriteTaskDeliveryWriterRegistry {

    private final List<WriteTaskDeliveryWriter> writers;

    public WriteTaskDeliveryWriterRegistry(List<WriteTaskDeliveryWriter> writers) {
        this.writers = writers;
    }

    public WriteTaskDeliveryWriter get(DatabaseType databaseType) {
        return writers.stream()
                .filter(writer -> writer.supports(databaseType))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("暂未实现该目标类型的写入器: " + databaseType));
    }
}

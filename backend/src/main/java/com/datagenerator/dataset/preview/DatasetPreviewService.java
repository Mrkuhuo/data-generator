package com.datagenerator.dataset.preview;

import com.datagenerator.dataset.api.DatasetPreviewRequest;
import com.datagenerator.dataset.api.DatasetPreviewResponse;
import com.datagenerator.dataset.domain.DatasetDefinition;
import com.datagenerator.dataset.domain.DatasetStatus;
import com.datagenerator.dataset.repository.DatasetDefinitionRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;

@Service
public class DatasetPreviewService {

    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\$\\{([^}]+)}");

    private final DatasetDefinitionRepository datasetRepository;
    private final ObjectMapper objectMapper;

    public DatasetPreviewService(DatasetDefinitionRepository datasetRepository, ObjectMapper objectMapper) {
        this.datasetRepository = datasetRepository;
        this.objectMapper = objectMapper;
    }

    public DatasetPreviewResponse preview(Long datasetId, DatasetPreviewRequest request) {
        GeneratedDatasetBatch batch = generate(datasetId, request != null ? request.count() : null, request != null ? request.seed() : null);
        return new DatasetPreviewResponse(batch.count(), batch.seed(), batch.rows());
    }

    public GeneratedDatasetBatch generate(Long datasetId, Integer requestedCount, Long requestedSeed) {
        DatasetDefinition dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + datasetId));
        return generate(dataset, requestedCount, requestedSeed);
    }

    public GeneratedDatasetBatch generate(DatasetDefinition dataset, Integer requestedCount, Long requestedSeed) {
        if (dataset.getStatus() == DatasetStatus.ARCHIVED) {
            throw new IllegalArgumentException("Archived datasets cannot be previewed");
        }

        Map<String, Object> sampleConfig = readJsonObject(dataset.getSampleConfigJson(), "sampleConfigJson");
        int count = requestedCount != null
                ? sanitizeCount(requestedCount)
                : sanitizeCount(asInteger(sampleConfig.getOrDefault("count", 5), 5));
        long seed = requestedSeed != null
                ? requestedSeed
                : asLong(sampleConfig.getOrDefault("seed", System.currentTimeMillis()), System.currentTimeMillis());

        Map<String, Object> schema = readJsonObject(dataset.getSchemaJson(), "schemaJson");
        Object rootType = schema.getOrDefault("type", "object");
        if (!Objects.equals(rootType, "object")) {
            throw new IllegalArgumentException("Dataset schema root must be an object");
        }

        PreviewOptions options = new PreviewOptions(count, seed);
        PreviewState state = new PreviewState(seed);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int index = 0; index < options.count(); index++) {
            Map<String, Object> rootContext = new LinkedHashMap<>();
            rows.add(generateObjectNode("", schema, rootContext, rootContext, state));
        }

        return new GeneratedDatasetBatch(dataset, options.count(), options.seed(), rows);
    }

    private Map<String, Object> generateObjectNode(
            String path,
            Map<String, Object> node,
            Map<String, Object> rootContext,
            Map<String, Object> currentContext,
            PreviewState state
    ) {
        Object fieldsObject = node.get("fields");
        if (!(fieldsObject instanceof Map<?, ?> rawFields)) {
            throw new IllegalArgumentException("Object node must declare fields");
        }

        Map<String, Object> output = currentContext;
        for (Map.Entry<?, ?> entry : rawFields.entrySet()) {
            String fieldName = String.valueOf(entry.getKey());
            if (!(entry.getValue() instanceof Map<?, ?> rawChild)) {
                throw new IllegalArgumentException("Field " + fieldName + " must be a JSON object");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> childNode = (Map<String, Object>) rawChild;
            String childPath = path.isBlank() ? fieldName : path + "." + fieldName;
            Object value = generateValue(childPath, childNode, rootContext, output, state);
            output.put(fieldName, value);
        }
        return output;
    }

    private Object generateValue(
            String path,
            Map<String, Object> node,
            Map<String, Object> rootContext,
            Map<String, Object> currentContext,
            PreviewState state
    ) {
        String rule = String.valueOf(node.getOrDefault("rule", node.get("type")));
        if (rule == null || rule.isBlank() || "null".equals(rule)) {
            throw new IllegalArgumentException("Node at " + path + " is missing a rule");
        }

        return switch (rule) {
            case "fixed" -> node.get("value");
            case "sequence" -> state.nextSequence(path, asLong(node.getOrDefault("start", 1), 1), asLong(node.getOrDefault("step", 1), 1));
            case "random_int" -> generateRandomInt(node, state.random());
            case "random_decimal" -> generateRandomDecimal(node, state.random());
            case "string" -> generateString(node, state.random());
            case "enum" -> chooseEnum(node, state.random());
            case "weighted_enum" -> chooseWeightedEnum(node, state.random());
            case "boolean" -> generateBoolean(node, state.random());
            case "datetime" -> generateDatetime(node, state.random());
            case "object" -> generateObjectNode(path, node, rootContext, new LinkedHashMap<>(), state);
            case "array" -> generateArray(path, node, rootContext, currentContext, state);
            case "reference" -> resolveReference(node, currentContext, rootContext);
            case "template" -> renderTemplate(String.valueOf(node.getOrDefault("template", "")), currentContext, rootContext);
            default -> throw new IllegalArgumentException("Unsupported rule at " + path + ": " + rule);
        };
    }

    private List<Object> generateArray(
            String path,
            Map<String, Object> node,
            Map<String, Object> rootContext,
            Map<String, Object> currentContext,
            PreviewState state
    ) {
        Object itemObject = node.get("item");
        if (!(itemObject instanceof Map<?, ?> rawItem)) {
            throw new IllegalArgumentException("Array node at " + path + " must declare an item object");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> item = (Map<String, Object>) rawItem;

        int size;
        if (node.containsKey("size")) {
            size = asInteger(node.get("size"), 1);
        } else {
            int min = asInteger(node.getOrDefault("sizeMin", 1), 1);
            int max = asInteger(node.getOrDefault("sizeMax", min), min);
            size = min + state.random().nextInt(Math.max(1, max - min + 1));
        }

        List<Object> items = new ArrayList<>();
        for (int index = 0; index < size; index++) {
            items.add(generateValue(path + "[" + index + "]", item, rootContext, currentContext, state));
        }
        return items;
    }

    private long generateRandomInt(Map<String, Object> node, Random random) {
        long min = asLong(node.getOrDefault("min", 0), 0);
        long max = asLong(node.getOrDefault("max", 1000), 1000);
        if (max < min) {
            throw new IllegalArgumentException("random_int rule has max < min");
        }
        return min + (long) Math.floor(random.nextDouble() * (max - min + 1));
    }

    private BigDecimal generateRandomDecimal(Map<String, Object> node, Random random) {
        double min = asDouble(node.getOrDefault("min", 0), 0);
        double max = asDouble(node.getOrDefault("max", 1000), 1000);
        int scale = asInteger(node.getOrDefault("scale", 2), 2);
        if (max < min) {
            throw new IllegalArgumentException("random_decimal rule has max < min");
        }
        double generated = min + (random.nextDouble() * (max - min));
        return BigDecimal.valueOf(generated).setScale(scale, RoundingMode.HALF_UP);
    }

    private String generateString(Map<String, Object> node, Random random) {
        String prefix = String.valueOf(node.getOrDefault("prefix", ""));
        String suffix = String.valueOf(node.getOrDefault("suffix", ""));
        int length = asInteger(node.getOrDefault("length", 12), 12);
        String charset = String.valueOf(node.getOrDefault("charset", "abcdefghijklmnopqrstuvwxyz0123456789"));

        if (charset.isBlank()) {
            throw new IllegalArgumentException("string rule requires a non-empty charset");
        }

        int payloadLength = Math.max(0, length);
        StringBuilder builder = new StringBuilder(prefix);
        for (int index = 0; index < payloadLength; index++) {
            builder.append(charset.charAt(random.nextInt(charset.length())));
        }
        builder.append(suffix);
        return builder.toString();
    }

    private Object chooseEnum(Map<String, Object> node, Random random) {
        List<?> values = asList(node.get("values"));
        if (values.isEmpty()) {
            throw new IllegalArgumentException("enum rule requires a non-empty values array");
        }
        return values.get(random.nextInt(values.size()));
    }

    private Object chooseWeightedEnum(Map<String, Object> node, Random random) {
        List<?> options = asList(node.get("options"));
        if (options.isEmpty()) {
            throw new IllegalArgumentException("weighted_enum rule requires a non-empty options array");
        }

        int totalWeight = 0;
        List<Map<String, Object>> parsedOptions = new ArrayList<>();
        for (Object option : options) {
            if (!(option instanceof Map<?, ?> rawOption)) {
                throw new IllegalArgumentException("weighted_enum option must be an object");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> typedOption = (Map<String, Object>) rawOption;
            int weight = asInteger(typedOption.getOrDefault("weight", 1), 1);
            totalWeight += weight;
            parsedOptions.add(typedOption);
        }

        int cursor = random.nextInt(totalWeight);
        int current = 0;
        for (Map<String, Object> option : parsedOptions) {
            current += asInteger(option.getOrDefault("weight", 1), 1);
            if (cursor < current) {
                return option.get("value");
            }
        }
        return parsedOptions.getLast().get("value");
    }

    private boolean generateBoolean(Map<String, Object> node, Random random) {
        double trueRate = asDouble(node.getOrDefault("trueRate", 0.5), 0.5);
        return random.nextDouble() <= trueRate;
    }

    private String generateDatetime(Map<String, Object> node, Random random) {
        Instant from = Instant.parse(String.valueOf(node.getOrDefault("from", Instant.now().minus(30, ChronoUnit.DAYS))));
        Instant to = Instant.parse(String.valueOf(node.getOrDefault("to", Instant.now())));
        if (to.isBefore(from)) {
            throw new IllegalArgumentException("datetime rule has to < from");
        }
        long offset = (long) (random.nextDouble() * (to.toEpochMilli() - from.toEpochMilli() + 1));
        return Instant.ofEpochMilli(from.toEpochMilli() + offset).toString();
    }

    private Object resolveReference(Map<String, Object> node, Map<String, Object> currentContext, Map<String, Object> rootContext) {
        String path = String.valueOf(node.getOrDefault("path", ""));
        if (path.isBlank()) {
            throw new IllegalArgumentException("reference rule requires a path");
        }

        Object currentValue = resolvePath(path, currentContext);
        if (currentValue != null) {
            return currentValue;
        }
        return resolvePath(path, rootContext);
    }

    private Object resolvePath(String path, Map<String, Object> context) {
        String[] segments = path.split("\\.");
        Object current = context;
        for (String segment : segments) {
            if (!(current instanceof Map<?, ?> currentMap)) {
                return null;
            }
            current = currentMap.get(segment);
        }
        return current;
    }

    private String renderTemplate(String template, Map<String, Object> currentContext, Map<String, Object> rootContext) {
        Matcher matcher = TEMPLATE_PATTERN.matcher(template);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String key = matcher.group(1);
            Object replacement = resolveReference(Map.of("path", key), currentContext, rootContext);
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement == null ? "" : replacement.toString()));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private Map<String, Object> readJsonObject(String json, String fieldName) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid " + fieldName + ": " + exception.getMessage());
        }
    }

    private int sanitizeCount(int count) {
        if (count < 1) {
            return 1;
        }
        return Math.min(count, 100);
    }

    private List<?> asList(Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        return List.of();
    }

    private int asInteger(Object value, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private long asLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private double asDouble(Object value, double fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(value.toString());
    }
}

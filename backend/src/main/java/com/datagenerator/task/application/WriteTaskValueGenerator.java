package com.datagenerator.task.application;

import com.datagenerator.task.domain.ColumnGeneratorType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class WriteTaskValueGenerator {

    public Object generateValue(
            String sequenceKey,
            ColumnGeneratorType generatorType,
            Map<String, Object> generatorConfig,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        Map<String, Object> config = generatorConfig == null ? Map.of() : generatorConfig;
        return switch (generatorType) {
            case SEQUENCE -> nextSequence(sequenceKey, config, sequenceState);
            case RANDOM_INT -> generateRandomInt(config, random);
            case RANDOM_DECIMAL -> generateRandomDecimal(config, random);
            case STRING -> generateString(config, random, rowIndex);
            case ENUM -> chooseEnum(config, random);
            case BOOLEAN -> generateBoolean(config, random);
            case DATETIME -> generateDatetime(config, random);
            case UUID -> UUID.randomUUID().toString();
        };
    }

    private long nextSequence(String sequenceKey, Map<String, Object> config, Map<String, Long> sequenceState) {
        long start = asLong(config.get("start"), 1L);
        long step = asLong(config.get("step"), 1L);
        long current = sequenceState.getOrDefault(sequenceKey, start);
        sequenceState.put(sequenceKey, current + step);
        return current;
    }

    private long generateRandomInt(Map<String, Object> config, Random random) {
        long min = asLong(config.get("min"), 0L);
        long max = asLong(config.get("max"), 1000L);
        if (max < min) {
            throw new IllegalArgumentException("随机整数生成规则中 max 不能小于 min");
        }
        return min + (long) Math.floor(random.nextDouble() * (max - min + 1));
    }

    private BigDecimal generateRandomDecimal(Map<String, Object> config, Random random) {
        double min = asDouble(config.get("min"), 0D);
        double max = asDouble(config.get("max"), 1000D);
        int scale = asInt(config.get("scale"), 2);
        if (max < min) {
            throw new IllegalArgumentException("随机小数生成规则中 max 不能小于 min");
        }
        double value = min + random.nextDouble() * (max - min);
        return BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP);
    }

    private String generateString(Map<String, Object> config, Random random, int rowIndex) {
        String prefix = String.valueOf(config.getOrDefault("prefix", ""));
        String suffix = String.valueOf(config.getOrDefault("suffix", ""));
        int length = Math.max(1, asInt(config.get("length"), 12));
        String charset = String.valueOf(config.getOrDefault("charset", "abcdefghijklmnopqrstuvwxyz0123456789"));
        if (charset.isBlank()) {
            throw new IllegalArgumentException("字符串生成规则要求 charset 不能为空");
        }

        String mode = String.valueOf(config.getOrDefault("mode", "random"));
        if ("email".equalsIgnoreCase(mode)) {
            String domain = String.valueOf(config.getOrDefault("domain", "demo.local"));
            return prefix + "user" + (rowIndex + 1) + "@" + domain + suffix;
        }

        StringBuilder builder = new StringBuilder(prefix);
        for (int index = 0; index < length; index++) {
            builder.append(charset.charAt(random.nextInt(charset.length())));
        }
        builder.append(suffix);
        return builder.toString();
    }

    private Object chooseEnum(Map<String, Object> config, Random random) {
        Object values = config.get("values");
        if (!(values instanceof java.util.List<?> options) || options.isEmpty()) {
            throw new IllegalArgumentException("枚举生成规则要求 values 至少包含一个选项");
        }
        return options.get(random.nextInt(options.size()));
    }

    private boolean generateBoolean(Map<String, Object> config, Random random) {
        double trueRate = asDouble(config.get("trueRate"), 0.5D);
        return random.nextDouble() <= trueRate;
    }

    private String generateDatetime(Map<String, Object> config, Random random) {
        Instant from = parseInstant(config.get("from"), Instant.now().minusSeconds(30L * 24 * 3600));
        Instant to = parseInstant(config.get("to"), Instant.now());
        if (to.isBefore(from)) {
            throw new IllegalArgumentException("时间生成规则中 to 不能早于 from");
        }
        long offset = (long) (random.nextDouble() * (to.toEpochMilli() - from.toEpochMilli() + 1));
        Instant generated = Instant.ofEpochMilli(from.toEpochMilli() + offset);
        if (asBoolean(config.get("dateOnly"), false)) {
            return generated.atZone(ZoneOffset.UTC).toLocalDate().toString();
        }
        return generated.toString();
    }

    private Instant parseInstant(Object value, Instant fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime.toInstant(ZoneOffset.UTC);
        }
        if (value instanceof LocalDate localDate) {
            return localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
        }
        return Instant.parse(value.toString());
    }

    private int asInt(Object value, int fallback) {
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

    private boolean asBoolean(Object value, boolean fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        return Boolean.parseBoolean(value.toString());
    }
}

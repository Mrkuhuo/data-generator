package com.datagenerator.dataset.preview;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class PreviewState {

    private final Random random;
    private final Map<String, Long> sequences = new HashMap<>();

    public PreviewState(long seed) {
        this.random = new Random(seed);
    }

    public Random random() {
        return random;
    }

    public long nextSequence(String path, long start, long step) {
        long nextValue = sequences.getOrDefault(path, start);
        sequences.put(path, nextValue + step);
        return nextValue;
    }
}


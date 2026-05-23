package kafka.project;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public final class SentimentLabelConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String[] FALLBACK_LABELS = {"Negative", "Neutral", "Positive"};

    private final String[] labels;
    private final int negativeIndex;
    private final int neutralIndex;
    private final int positiveIndex;

    private SentimentLabelConfig(String[] labels) {
        this.labels = labels;
        this.negativeIndex = indexOf(labels, "negative").orElse(0);
        this.neutralIndex = indexOf(labels, "neutral").orElse(1);
        this.positiveIndex = indexOf(labels, "positive").orElse(2);
    }

    public static SentimentLabelConfig load(Path configPath) {
        if (configPath == null || !Files.isRegularFile(configPath)) {
            return new SentimentLabelConfig(FALLBACK_LABELS);
        }
        try {
            JsonNode id2Label = OBJECT_MAPPER.readTree(configPath.toFile()).get("id2label");
            if (id2Label == null || !id2Label.isObject()) {
                return new SentimentLabelConfig(FALLBACK_LABELS);
            }

            TreeMap<Integer, String> orderedLabels = OBJECT_MAPPER.convertValue(
                    id2Label,
                    new TypeReference<>() {
                    }
            );
            if (orderedLabels.isEmpty()) {
                return new SentimentLabelConfig(FALLBACK_LABELS);
            }

            String[] labels = new String[orderedLabels.lastKey() + 1];
            orderedLabels.forEach((index, label) -> labels[index] = normalizeLabel(label));
            return new SentimentLabelConfig(labels);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to read sentiment label config: " + configPath, e);
        }
    }

    public String label(int index) {
        if (index < 0 || index >= labels.length || labels[index] == null || labels[index].isBlank()) {
            return "Unknown";
        }
        return labels[index];
    }

    public double negativeScore(double[] probabilities) {
        return score(probabilities, negativeIndex);
    }

    public double neutralScore(double[] probabilities) {
        return score(probabilities, neutralIndex);
    }

    public double positiveScore(double[] probabilities) {
        return score(probabilities, positiveIndex);
    }

    public int strongerBinaryIndex(double[] probabilities) {
        return negativeScore(probabilities) >= positiveScore(probabilities) ? negativeIndex : positiveIndex;
    }

    public int neutralIndex() {
        return neutralIndex;
    }

    public String labelsForLog() {
        return String.join(",", labels);
    }

    private static Optional<Integer> indexOf(String[] labels, String expected) {
        for (int i = 0; i < labels.length; i++) {
            if (labels[i] != null && expected.equals(labels[i].toLowerCase(Locale.ROOT))) {
                return Optional.of(i);
            }
        }
        return Optional.empty();
    }

    private static String normalizeLabel(String label) {
        if (label == null || label.isBlank()) {
            return "Unknown";
        }
        String normalized = label.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "label_0" -> "Negative";
            case "label_1" -> "Neutral";
            case "label_2" -> "Positive";
            default -> Character.toUpperCase(normalized.charAt(0)) + normalized.substring(1);
        };
    }

    private double score(double[] probabilities, int index) {
        if (index < 0 || index >= probabilities.length) {
            return 0.0;
        }
        return probabilities[index];
    }
}

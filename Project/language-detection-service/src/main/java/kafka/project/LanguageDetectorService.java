package kafka.project;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class LanguageDetectorService {

    private static final Map<String, String> ISO_639_3_TO_1 = Map.ofEntries(
            Map.entry("ces", "cs"),
            Map.entry("deu", "de"),
            Map.entry("ell", "el"),
            Map.entry("eng", "en"),
            Map.entry("est", "et"),
            Map.entry("isl", "is"),
            Map.entry("ita", "it"),
            Map.entry("lav", "lv"),
            Map.entry("slv", "sl"),
            Map.entry("swe", "sv"),
            Map.entry("ukr", "uk")
    );

    private final LanguageDetectorME languageDetector;
    private final Set<String> allowedLanguages;
    private final double minimumConfidence;
    private final double reliableMinimumConfidence;
    private final double reliableMinimumMargin;

    public LanguageDetectorService(
            @Value("${app.opennlp.language-model-path}") Path languageModelPath,
            @Value("${app.opennlp.minimum-confidence:0.0}") double minimumConfidence,
            @Value("${app.opennlp.allowed-languages:en,de,cs,it,et,sl,el,lv,uk,sv,is}") String allowedLanguages,
            @Value("${app.opennlp.reliable-minimum-confidence:0.10}") double reliableMinimumConfidence,
            @Value("${app.opennlp.reliable-minimum-margin:0.02}") double reliableMinimumMargin
    ) {
        this.languageDetector = createDetector(languageModelPath);
        this.allowedLanguages = parseAllowedLanguages(allowedLanguages);
        this.minimumConfidence = minimumConfidence;
        this.reliableMinimumConfidence = reliableMinimumConfidence;
        this.reliableMinimumMargin = reliableMinimumMargin;
    }

    public LanguagePrediction detect(String text) {
        Language[] languages = languageDetector.predictLanguages(text);
        List<LanguageScore> allCandidates = Arrays.stream(languages)
                .map(language -> new LanguageScore(
                        normalizeLanguageCode(language.getLang()),
                        language.getLang(),
                        language.getConfidence()
                ))
                .toList();
        List<LanguageScore> candidates = allCandidates.stream()
                .filter(candidate -> allowedLanguages.isEmpty() || allowedLanguages.contains(candidate.language()))
                .sorted(Comparator.comparingDouble(LanguageScore::confidence).reversed())
                .toList();

        if (candidates.isEmpty()) {
            double confidence = allCandidates.isEmpty() ? 0.0 : allCandidates.getFirst().confidence();
            return new LanguagePrediction("unknown", confidence, false, allCandidates);
        }

        LanguageScore top = candidates.getFirst();
        double secondConfidence = candidates.size() > 1 ? candidates.get(1).confidence() : 0.0;
        boolean reliable = top.confidence() >= reliableMinimumConfidence
                || top.confidence() - secondConfidence >= reliableMinimumMargin;

        if (top.confidence() < minimumConfidence) {
            return new LanguagePrediction("unknown", top.confidence(), reliable, candidates);
        }

        return new LanguagePrediction(
                top.language(),
                top.confidence(),
                reliable,
                candidates
        );
    }

    private Set<String> parseAllowedLanguages(String configuredLanguages) {
        if (configuredLanguages == null || configuredLanguages.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(configuredLanguages.split(","))
                .map(String::trim)
                .filter(language -> !language.isBlank())
                .map(this::normalizeLanguageCode)
                .collect(Collectors.toUnmodifiableSet());
    }

    private String normalizeLanguageCode(String languageCode) {
        if (languageCode == null || languageCode.isBlank()) {
            return "unknown";
        }
        String normalized = languageCode.toLowerCase(Locale.ROOT);
        return ISO_639_3_TO_1.getOrDefault(normalized, normalized);
    }

    private LanguageDetectorME createDetector(Path modelPath) {
        if (!Files.isRegularFile(modelPath)) {
            throw new IllegalStateException("OpenNLP language model file does not exist: " + modelPath);
        }
        try (InputStream inputStream = Files.newInputStream(modelPath)) {
            return new LanguageDetectorME(new LanguageDetectorModel(inputStream));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load OpenNLP language model: " + modelPath, e);
        }
    }
}

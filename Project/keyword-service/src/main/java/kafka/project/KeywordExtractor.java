package kafka.project;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.et.EstonianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.uk.UkrainianMorfologikAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class KeywordExtractor {

    private static final Pattern WORD_PATTERN = Pattern.compile("[\\p{L}][\\p{L}\\p{M}'-]*");
    private static final String UNKNOWN_LEMMA = "O";
    private static final CharArraySet ICELANDIC_STOP_WORDS = CharArraySet.unmodifiableSet(new CharArraySet(List.of(
            "að", "af", "á", "aðeins", "allar", "allt", "annars", "ekki", "eða", "ef", "ég", "en", "er", "eru",
            "frá", "fyrir", "hafa", "hann", "hún", "hvað", "hvar", "hvernig", "í", "með", "mig", "mín", "og",
            "sem", "sér", "sína", "sitt", "það", "þar", "þau", "þegar", "þeir", "þetta", "til", "um", "var", "við"
    ), true));
    private static final CharArraySet SLOVENIAN_STOP_WORDS = CharArraySet.unmodifiableSet(new CharArraySet(List.of(
            "a", "ali", "bi", "bil", "bila", "bili", "bilo", "bo", "bodo", "bova", "boš", "brez", "da", "do",
            "ga", "in", "iz", "jaz", "je", "jih", "jo", "kaj", "kako", "ki", "ko", "kot", "me", "med", "mi",
            "na", "ne", "ni", "njegov", "njih", "njihov", "ona", "oni", "pa", "po", "pri", "sem", "si", "so",
            "smo", "ste", "ta", "te", "tega", "ti", "to", "tu", "tudi", "v", "vas", "ve", "za"
    ), true));
    private static final Logger log = LoggerFactory.getLogger(KeywordExtractor.class);

    private final OpenNlpKeywordToolService toolService;

    public KeywordExtractor(OpenNlpKeywordToolService toolService) {
        this.toolService = toolService;
    }

    public List<String> extract(String text, String language) {
        KeywordLanguageTools tools = toolService.toolsFor(language);
        String[] tokens = tools.tokenize(text);
        String[] tags = tools.tag(tokens);
        String[] lemmas = tools.lemmatize(tokens, tags);
        CharArraySet languageStopWords = stopSetFor(language);

        Map<String, Long> counts = new LinkedHashMap<>();
        for (int i = 0; i < tokens.length; i++) {
            String candidate = chooseLemma(tokens[i], lemmas[i]).toLowerCase(Locale.ROOT);
            if (candidate.length() < 3 || !WORD_PATTERN.matcher(candidate).matches() || isStopWord(languageStopWords, candidate)) {
                continue;
            }
            counts.merge(candidate, 1L, Long::sum);
        }

        return counts.keySet().stream().toList();
    }

    private String chooseLemma(String token, String lemma) {
        if (lemma == null || lemma.isBlank() || UNKNOWN_LEMMA.equals(lemma)) {
            return token;
        }
        return lemma;
    }

    private boolean isStopWord(CharArraySet stopWords, String token) {
        return !stopWords.isEmpty() && stopWords.contains(token);
    }

    private CharArraySet stopSetFor(String language) {
        return switch (language) {
            case "cs" -> CzechAnalyzer.getDefaultStopSet();
            case "de" -> GermanAnalyzer.getDefaultStopSet();
            case "el" -> GreekAnalyzer.getDefaultStopSet();
            case "en" -> EnglishAnalyzer.getDefaultStopSet();
            case "et" -> EstonianAnalyzer.getDefaultStopSet();
            case "is" -> ICELANDIC_STOP_WORDS;
            case "it" -> ItalianAnalyzer.getDefaultStopSet();
            case "lv" -> LatvianAnalyzer.getDefaultStopSet();
            case "sl" -> SLOVENIAN_STOP_WORDS;
            case "uk" -> UkrainianMorfologikAnalyzer.getDefaultStopwords();
            case "sv" -> SwedishAnalyzer.getDefaultStopSet();
            default -> {
                log.info("No stop words for language {}", language);
                yield CharArraySet.EMPTY_SET;
            }
        };
    }

}

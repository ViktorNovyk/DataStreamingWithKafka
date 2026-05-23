package kafka.project;

import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.lemmatizer.LemmatizerModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

@Service
public class OpenNlpKeywordToolService {

    private static final Logger LOG = LoggerFactory.getLogger(OpenNlpKeywordToolService.class);
    private static final KeywordLanguageTools FALLBACK_TOOLS =
            new KeywordLanguageTools(SimpleTokenizer.INSTANCE, null, null);

    private final Map<String, KeywordLanguageTools> toolsByLanguage;

    public OpenNlpKeywordToolService(
            @Value("${app.keyword.models.tokens-dir:/models/opennlp/tokens}") Path tokensDirectory,
            @Value("${app.keyword.models.pos-dir:/models/opennlp/pos}") Path posDirectory,
            @Value("${app.keyword.models.lemmas-dir:/models/opennlp/lemmas}") Path lemmasDirectory
    ) {
        Map<String, TokenizerModel> tokenizerModels = loadModels(tokensDirectory, "tokenizer", this::loadTokenizerModel);
        Map<String, POSModel> posModels = loadModels(posDirectory, "POS", this::loadPosModel);
        Map<String, LemmatizerModel> lemmatizerModels = loadModels(lemmasDirectory, "lemmatizer", this::loadLemmatizerModel);

        Map<String, KeywordLanguageTools> tools = new HashMap<>();
        Set<String> languages = new HashSet<>();
        languages.addAll(tokenizerModels.keySet());
        languages.addAll(posModels.keySet());
        languages.addAll(lemmatizerModels.keySet());

        languages.forEach(language -> tools.put(language, new KeywordLanguageTools(
                tokenizerModels.containsKey(language) ? new TokenizerME(tokenizerModels.get(language)) : SimpleTokenizer.INSTANCE,
                posModels.containsKey(language) ? new POSTaggerME(posModels.get(language)) : null,
                lemmatizerModels.containsKey(language) ? new LemmatizerME(lemmatizerModels.get(language)) : null
        )));

        this.toolsByLanguage = Map.copyOf(tools);
        LOG.info("Initialized OpenNLP keyword tools for languages={}", this.toolsByLanguage.keySet());
    }

    public KeywordLanguageTools toolsFor(String language) {
        return toolsByLanguage.getOrDefault(language, FALLBACK_TOOLS);
    }

    private <T> Map<String, T> loadModels(Path directory, String modelType, Function<Path, T> loader) {
        Map<String, T> models = new HashMap<>();
        if (!Files.exists(directory)) {
            LOG.info("OpenNLP {} model directory does not exist, skipping: {}", modelType, directory);
            return Map.copyOf(models);
        }
        if (!Files.isDirectory(directory)) {
            throw new IllegalStateException("OpenNLP " + modelType + " model path is not a directory: " + directory);
        }

        try (Stream<Path> paths = Files.list(directory)) {
            paths.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".bin"))
                    .forEach(path -> {
                        String language = languageFromFileName(path);
                        if (models.containsKey(language)) {
                            LOG.warn("Replacing OpenNLP {} model for language={} with file={}", modelType, language, path);
                        }
                        models.put(language, loader.apply(path));
                    });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to scan OpenNLP " + modelType + " model directory: " + directory, e);
        }

        LOG.info("Loaded {} OpenNLP {} models from {}. {}", models.size(), modelType, directory, models.keySet());
        return Map.copyOf(models);
    }

    private String languageFromFileName(Path path) {
        String fileName = path.getFileName().toString();
        String[] parts = fileName.split("-");
        if (parts.length < 3 || !"opennlp".equals(parts[0]) || parts[1].isBlank()) {
            throw new IllegalArgumentException("OpenNLP model file name must look like opennlp-en-...-tokens-...bin: " + fileName);
        }
        return parts[1].toLowerCase(Locale.ROOT);
    }

    private TokenizerModel loadTokenizerModel(Path path) {
        try (InputStream inputStream = Files.newInputStream(path)) {
            return new TokenizerModel(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load OpenNLP tokenizer model: " + path, e);
        }
    }

    private POSModel loadPosModel(Path path) {
        try (InputStream inputStream = Files.newInputStream(path)) {
            return new POSModel(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load OpenNLP POS model: " + path, e);
        }
    }

    private LemmatizerModel loadLemmatizerModel(Path path) {
        try (InputStream inputStream = Files.newInputStream(path)) {
            return new LemmatizerModel(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load OpenNLP lemmatizer model: " + path, e);
        }
    }

}

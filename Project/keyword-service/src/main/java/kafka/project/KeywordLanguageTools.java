package kafka.project;

import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;

import java.util.Arrays;

public record KeywordLanguageTools(
        Tokenizer tokenizer,
        POSTaggerME posTagger,
        LemmatizerME lemmatizer
) {

    synchronized String[] tokenize(String text) {
        return tokenizer.tokenize(text);
    }

    synchronized String[] tag(String[] tokens) {
        if (posTagger == null) {
            String[] fallback = new String[tokens.length];
            Arrays.fill(fallback, "NN");
            return fallback;
        }
        return posTagger.tag(tokens);
    }

    synchronized String[] lemmatize(String[] tokens, String[] tags) {
        if (lemmatizer == null) {
            return tokens;
        }
        return lemmatizer.lemmatize(tokens, tags);
    }
}

package kafka.project;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OnnxSentimentAnalyzer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OnnxSentimentAnalyzer.class);

    private final OrtEnvironment environment;
    private final OrtSession session;
    private final HuggingFaceTokenizer tokenizer;
    private final SentimentLabelConfig labelConfig;
    private final int maxSequenceLength;
    private final long padTokenId;
    private final boolean collapseNeutralToBinary;
    private final boolean usesTokenTypeIds;

    public OnnxSentimentAnalyzer(
            @Value("${app.sentiment.onnx-model-path}") Path onnxModelPath,
            @Value("${app.sentiment.tokenizer-path}") Path tokenizerPath,
            @Value("${app.sentiment.config-path:}") Path configPath,
            @Value("${app.sentiment.max-sequence-length:128}") int maxSequenceLength,
            @Value("${app.sentiment.pad-token-id:1}") long padTokenId,
            @Value("${app.sentiment.collapse-neutral-to-binary:false}") boolean collapseNeutralToBinary,
            @Value("${app.sentiment.ort-intra-op-threads:2}") int intraOpThreads,
            @Value("${app.sentiment.ort-inter-op-threads:1}") int interOpThreads
    ) {
        if (!Files.isRegularFile(onnxModelPath)) {
            throw new IllegalStateException("ONNX sentiment model file does not exist: " + onnxModelPath);
        }
        if (!Files.isRegularFile(tokenizerPath)) {
            throw new IllegalStateException("Hugging Face tokenizer JSON does not exist: " + tokenizerPath);
        }
        this.maxSequenceLength = maxSequenceLength;
        this.padTokenId = padTokenId;
        this.collapseNeutralToBinary = collapseNeutralToBinary;
        this.labelConfig = SentimentLabelConfig.load(resolveConfigPath(configPath, onnxModelPath));
        this.environment = OrtEnvironment.getEnvironment();
        try {
            OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions();
            sessionOptions.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
            sessionOptions.setExecutionMode(OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL);
            sessionOptions.setIntraOpNumThreads(intraOpThreads);
            sessionOptions.setInterOpNumThreads(interOpThreads);
            this.session = environment.createSession(onnxModelPath.toString(), sessionOptions);
            this.usesTokenTypeIds = session.getInputNames().contains("token_type_ids");
            this.tokenizer = HuggingFaceTokenizer.newInstance(tokenizerPath);
            LOG.info("Initialized ONNX sentiment model={} labels={} maxSequenceLength={} intraOpThreads={} interOpThreads={}",
                    onnxModelPath, labelConfig.labelsForLog(), maxSequenceLength, intraOpThreads, interOpThreads);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize ONNX sentiment analyzer", e);
        }
    }

    public SentimentPrediction analyze(String text) {
        return analyzeBatch(List.of(text)).getFirst();
    }

    public List<SentimentPrediction> analyzeBatch(List<String> texts) {
        if (texts == null || texts.isEmpty()) {
            return List.of();
        }

        try {
            long startedAt = System.nanoTime();

            // Tokenize the whole Kafka poll at once, so ONNX Runtime can process one batch
            // instead of running one expensive model invocation per message.
            Encoding[] encodings = tokenizer.batchEncode(texts);
            int batchSize = encodings.length;

            // Use the longest encoded message in this batch, capped by maxSequenceLength.
            // Shorter messages are padded to this length so the tensors are rectangular:
            // [batchSize, sequenceLength].
            int sequenceLength = sequenceLength(encodings);

            long[][] inputIds = new long[batchSize][sequenceLength];
            long[][] attentionMask = new long[batchSize][sequenceLength];
            long[][] tokenTypeIds = usesTokenTypeIds ? new long[batchSize][sequenceLength] : null;

            for (int i = 0; i < batchSize; i++) {
                // input_ids contains tokenizer vocabulary ids. Padding fills unused positions.
                if (padTokenId != 0) {
                    fill(inputIds[i], padTokenId);
                }

                // attention_mask marks real tokens as 1 and padding as 0. copyTruncated
                // also enforces maxSequenceLength for very long comments.
                copyTruncated(encodings[i].getIds(), inputIds[i], sequenceLength);
                copyTruncated(encodings[i].getAttentionMask(), attentionMask[i], sequenceLength);
            }

            // ONNX input names must match the exported model. DistilBERT needs input_ids
            // and attention_mask; some BERT-style models also require token_type_ids.
            Map<String, OnnxTensor> inputs = new HashMap<>();
            inputs.put("input_ids", OnnxTensor.createTensor(environment, inputIds));
            inputs.put("attention_mask", OnnxTensor.createTensor(environment, attentionMask));
            if (usesTokenTypeIds) {
                inputs.put("token_type_ids", OnnxTensor.createTensor(environment, tokenTypeIds));
            }

            try (OrtSession.Result result = session.run(inputs)) {
                // The model returns one logits row per input text. Logits are raw scores,
                // so convert them to probabilities before choosing the sentiment label.
                float[][] logits = (float[][]) result.get(0).getValue();
                List<SentimentPrediction> predictions = new ArrayList<>(logits.length);
                for (float[] row : logits) {
                    predictions.add(toPrediction(softmax(row)));
                }
                long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;
                if (elapsedMs > 1000) {
                    LOG.warn("Slow sentiment batch inference elapsedMs={} batchSize={} sequenceLength={}",
                            elapsedMs, batchSize, sequenceLength);
                }
                return predictions;
            } finally {
                // OnnxTensor wraps native memory. Close every tensor after session.run
                // finishes to avoid leaking memory outside the JVM heap.
                for (OnnxTensor tensor : inputs.values()) {
                    tensor.close();
                }
            }
        } catch (OrtException e) {
            throw new IllegalStateException("Failed to run ONNX sentiment model", e);
        }
    }

    private int sequenceLength(Encoding[] encodings) {
        int sequenceLength = 0;
        for (Encoding encoding : encodings) {
            sequenceLength = Math.max(sequenceLength, Math.min(encoding.getIds().length, maxSequenceLength));
        }
        return Math.max(sequenceLength, 1);
    }

    private void copyTruncated(long[] source, long[] target, int sequenceLength) {
        System.arraycopy(source, 0, target, 0, Math.min(source.length, sequenceLength));
    }

    private void fill(long[] values, long value) {
        for (int i = 0; i < values.length; i++) {
            values[i] = value;
        }
    }

    private SentimentPrediction toPrediction(double[] probabilities) {
        int labelIndex = argmax(probabilities);
        if (collapseNeutralToBinary && labelIndex == labelConfig.neutralIndex()) {
            labelIndex = labelConfig.strongerBinaryIndex(probabilities);
        }
        return new SentimentPrediction(
                labelConfig.label(labelIndex),
                probabilities[labelIndex],
                labelConfig.negativeScore(probabilities),
                labelConfig.neutralScore(probabilities),
                labelConfig.positiveScore(probabilities)
        );
    }

    private Path resolveConfigPath(Path configuredPath, Path onnxModelPath) {
        if (configuredPath != null && !configuredPath.toString().isBlank()) {
            return configuredPath;
        }
        Path parent = onnxModelPath.getParent();
        return parent == null ? null : parent.resolve("config.json");
    }

    private double[] softmax(float[] logits) {
        double max = Double.NEGATIVE_INFINITY;
        for (float logit : logits) {
            max = Math.max(max, logit);
        }
        double total = 0.0;
        double[] exp = new double[logits.length];
        for (int i = 0; i < logits.length; i++) {
            exp[i] = Math.exp(logits[i] - max);
            total += exp[i];
        }
        for (int i = 0; i < exp.length; i++) {
            exp[i] = exp[i] / total;
        }
        return exp;
    }

    private int argmax(double[] values) {
        int index = 0;
        for (int i = 1; i < values.length; i++) {
            if (values[i] > values[index]) {
                index = i;
            }
        }
        return index;
    }

    @Override
    public void close() throws Exception {
        session.close();
        tokenizer.close();
    }
}

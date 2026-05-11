package kafka.hw2.reporting;

import java.util.List;

public record ReportGenerationResponse(
        long generatedAtMs,
        String reportFilePath,
        int rowsCount,
        List<ExperimentSummaryRow> rows
) {
}

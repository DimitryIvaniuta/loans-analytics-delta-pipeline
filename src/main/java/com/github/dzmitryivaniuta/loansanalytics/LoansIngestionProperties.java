package com.github.dzmitryivaniuta.loansanalytics;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "loans.ingestion")
public record LoansIngestionProperties(
        Path inputDir,
        boolean cliEnabled,
        LocalDate asOf,
        List<String> enabledFeeds
) {
    public LoansIngestionProperties {
        if (enabledFeeds == null || enabledFeeds.isEmpty()) {
            enabledFeeds = List.of("LOAN_MASTER", "PAYMENT_TRANSACTION");
        }
    }
}

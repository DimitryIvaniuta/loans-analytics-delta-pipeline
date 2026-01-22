package com.github.dzmitryivaniuta.loansanalytics;

import com.github.dzmitryivaniuta.loansanalytics.ingest.IngestionOrchestrator;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(LoansIngestionProperties.class)
@RequiredArgsConstructor
public class LoansAnalyticsApplication implements CommandLineRunner {

    private final IngestionOrchestrator orchestrator;
    private final LoansIngestionProperties props;

    public static void main(String[] args) {
        SpringApplication.run(LoansAnalyticsApplication.class, args);
    }

    /**
     * Optional CLI entrypoint.
     *
     * <p>Example:
     * <pre>
     *   ./gradlew bootRun --args='--loans.ingestion.cli-enabled=true --loans.ingestion.as-of=2026-01-17'
     * </pre>
     */
    @Override
    public void run(String... args) {
        if (!props.cliEnabled()) {
            return;
        }
        LocalDate asOf = props.asOf();
        if (asOf == null) {
            throw new IllegalArgumentException("loans.ingestion.as-of must be provided when CLI is enabled");
        }
        orchestrator.ingestAndGenerateDelta(asOf);
    }
}

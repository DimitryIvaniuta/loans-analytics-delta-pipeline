package com.github.dzmitryivaniuta.loansanalytics.ingest;

import com.github.dzmitryivaniuta.loansanalytics.LoansIngestionProperties;
import com.github.dzmitryivaniuta.loansanalytics.ingest.feed.FeedDefinition;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class FeedFileLocator {

    private final LoansIngestionProperties props;

    public Path locate(FeedDefinition feed, LocalDate asOf) {
        Path dir = props.inputDir();
        if (dir == null) {
            throw new IllegalStateException("loans.ingestion.input-dir is not configured");
        }
        Path p = dir.resolve(feed.expectedFileName(asOf));
        if (!Files.exists(p)) {
            throw new IllegalArgumentException("Missing feed file for " + feed.name() + " at " + p.toAbsolutePath());
        }
        return p;
    }
}

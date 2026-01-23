package com.github.dimitryivaniuta.loansanalytics.ingest;

import com.github.dimitryivaniuta.loansanalytics.LoansIngestionProperties;
import com.github.dimitryivaniuta.loansanalytics.ingest.feed.FeedDefinition;
import com.github.dimitryivaniuta.loansanalytics.ingest.feed.FeedName;
import com.github.dimitryivaniuta.loansanalytics.ingest.feed.FeedRegistry;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Multi-feed ingestion orchestrator.
 *
 * <p>For each enabled feed:
 * <ul>
 *   <li>locate the feed file for the day</li>
 *   <li>COPY-load into feed-specific staging table</li>
 *   <li>upsert into feed-specific snapshot table</li>
 *   <li>generate feed delta vs previous day into unified delta_event table</li>
 * </ul>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionOrchestrator {

    private final LoansIngestionProperties props;
    private final FeedRegistry registry;
    private final FeedFileLocator fileLocator;
    private final PostgresCopyService copyService;
    private final SnapshotRepository snapshotRepository;
    private final DeltaRepository deltaRepository;
    private final JobRunAuditService audit;

    @Transactional
    public UUID ingestAndGenerateDelta(LocalDate asOf) {
        Set<FeedName> enabled = parseEnabledFeeds(props.enabledFeeds());
        return ingestAndGenerateDelta(asOf, enabled);
    }

    @Transactional
    public UUID ingestAndGenerateDelta(LocalDate asOf, Set<FeedName> feeds) {
        UUID runId = UUID.randomUUID();
        Instant startedAt = Instant.now();
        audit.startRun(runId, asOf, startedAt);

        try {
            LocalDate prev = asOf.minusDays(1);

            for (FeedName fn : feeds) {
                FeedDefinition feed = registry.get(fn);
                Path file = fileLocator.locate(feed, asOf);
                audit.startFeed(runId, fn, Instant.now(), file.getFileName().toString());

                try {
                    snapshotRepository.truncateStaging(runId, feed, asOf);
                    long copied = copyService.copyIntoStaging(runId, asOf, feed, file);
                    long staged = snapshotRepository.countStagedRows(runId, feed, asOf);
                    log.info("Run {} feed {} copied={} staged={}", runId, fn, copied, staged);

                    snapshotRepository.upsertSnapshotFromStaging(runId, feed, asOf);
                    long snap = snapshotRepository.countSnapshot(feed, asOf);

                    int delta = deltaRepository.generateDelta(runId, feed, asOf, prev);

                    snapshotRepository.deleteStaging(runId, feed);
                    audit.finishFeed(runId, fn, Instant.now(), "SUCCESS", staged, snap, delta, null);
                } catch (Exception e) {
                    log.error("Run {} feed {} failed", runId, fn, e);
                    audit.finishFeed(runId, fn, Instant.now(), "FAILED", null, null, null, e.getMessage());
                    throw e;
                }
            }

            audit.finishRun(runId, Instant.now(), "SUCCESS", null);
            return runId;
        } catch (Exception e) {
            audit.finishRun(runId, Instant.now(), "FAILED", e.getMessage());
            throw e;
        }
    }

    private static Set<FeedName> parseEnabledFeeds(List<String> configured) {
        Set<FeedName> out = new HashSet<>();
        if (configured == null || configured.isEmpty()) {
            return out;
        }
        for (String s : configured) {
            if (s == null || s.isBlank()) continue;
            out.add(FeedName.valueOf(s.trim()));
        }
        return out;
    }
}

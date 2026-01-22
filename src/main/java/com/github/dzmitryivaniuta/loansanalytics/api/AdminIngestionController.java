package com.github.dzmitryivaniuta.loansanalytics.api;

import com.github.dzmitryivaniuta.loansanalytics.ingest.IngestionOrchestrator;
import com.github.dzmitryivaniuta.loansanalytics.ingest.feed.FeedName;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
public class AdminIngestionController {

    private final IngestionOrchestrator orchestrator;

    @PostMapping("/ingest")
    public ResponseEntity<Map<String, Object>> ingest(
            @RequestParam("asOf")
            @NotNull
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
            LocalDate asOf,
            @RequestParam(value = "feeds", required = false) String feeds
    ) {
        UUID runId = (feeds == null || feeds.isBlank())
                ? orchestrator.ingestAndGenerateDelta(asOf)
                : orchestrator.ingestAndGenerateDelta(asOf, parseFeeds(feeds));
        return ResponseEntity.ok(Map.of("runId", runId.toString(), "asOf", asOf.toString()));
    }

    private static java.util.Set<FeedName> parseFeeds(String csv) {
        java.util.Set<FeedName> set = new java.util.HashSet<>();
        for (String p : csv.split(",")) {
            if (p.isBlank()) continue;
            set.add(FeedName.valueOf(p.trim()));
        }
        return set;
    }
}

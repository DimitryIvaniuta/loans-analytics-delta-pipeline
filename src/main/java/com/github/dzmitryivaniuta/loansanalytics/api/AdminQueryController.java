package com.github.dzmitryivaniuta.loansanalytics.api;

import com.github.dzmitryivaniuta.loansanalytics.ingest.JobRunRepository;
import com.github.dzmitryivaniuta.loansanalytics.ingest.feed.FeedRegistry;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
public class AdminQueryController {

    private final FeedRegistry feedRegistry;
    private final JobRunRepository jobRunRepository;

    @GetMapping("/feeds")
    public ResponseEntity<?> listFeeds() {
        return ResponseEntity.ok(
                feedRegistry.all().stream().map(fd -> Map.of(
                        "name", fd.name().name(),
                        "filePattern", fd.filePattern(),
                        "stagingTable", fd.stagingTable(),
                        "snapshotTable", fd.snapshotTable(),
                        "pkColumns", fd.primaryKeyColumns(),
                        "businessColumns", fd.dataColumns()
                )).toList()
        );
    }

    @GetMapping("/runs")
    public ResponseEntity<?> listRuns(
            @RequestParam(value = "from", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(value = "to", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
            @RequestParam(value = "limit", required = false, defaultValue = "50") @Min(1) @Max(500) int limit
    ) {
        return ResponseEntity.ok(jobRunRepository.listRuns(from, to, limit));
    }

    @GetMapping("/runs/{runId}")
    public ResponseEntity<?> getRun(@PathVariable("runId") UUID runId) {
        Map<String,Object> run = jobRunRepository.getRun(runId);
        return run == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(run);
    }

    @GetMapping("/runs/{runId}/feeds")
    public ResponseEntity<?> getRunFeeds(@PathVariable("runId") UUID runId) {
        return ResponseEntity.ok(jobRunRepository.listRunFeeds(runId));
    }
}

package com.github.dimitryivaniuta.loansanalytics.ingest;

import com.github.dimitryivaniuta.loansanalytics.ingest.feed.FeedName;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class JobRunAuditService {
    private final JobRunRepository repo;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void startRun(UUID id, LocalDate asOf, Instant startedAt) {
        repo.startRun(id, asOf, startedAt);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void finishRun(UUID id, Instant finishedAt, String status, String error) {
        repo.finishRun(id, finishedAt, status, error);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void startFeed(UUID runId, FeedName feed, Instant startedAt, String file) {
        repo.startFeed(runId, feed, startedAt, file);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void finishFeed(UUID runId, FeedName feed, Instant finishedAt, String status,
                           Long staged, Long snap, Integer delta, String error) {
        repo.finishFeed(runId, feed, finishedAt, status, staged, snap, delta, error);
    }
}

package com.github.dzmitryivaniuta.loansanalytics.ingest;

import com.github.dzmitryivaniuta.loansanalytics.ingest.feed.FeedName;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class JobRunRepository {

    private final NamedParameterJdbcTemplate jdbc;

    private static OffsetDateTime utc(Instant i) {
        return i == null ? null : OffsetDateTime.ofInstant(i, ZoneOffset.UTC);
    }

    public void startRun(UUID id, LocalDate asOf, Instant startedAt) {
        jdbc.update(
                "INSERT INTO job_run (id, as_of_date, started_at, status) " +
                        "VALUES (:id, :asOf, :startedAt, :status)",
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("asOf", asOf)
                        .addValue("startedAt", utc(startedAt))
                        .addValue("status", "STARTED")
        );
    }

    public void finishRun(UUID id, Instant finishedAt, String status, @Nullable String errorMessage) {
        jdbc.update(
                "UPDATE job_run SET finished_at=:finishedAt, status=:status, error_message=:msg WHERE id=:id",
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("finishedAt", utc(finishedAt))
                        .addValue("status", status)
                        .addValue("msg", errorMessage)
        );
    }

    public void startFeed(UUID runId, FeedName feed, Instant startedAt, @Nullable String sourceFile) {
        jdbc.update(
                "INSERT INTO job_run_feed (job_run_id, feed_name, source_file, started_at, status) " +
                        "VALUES (:runId, :feed, :file, :startedAt, :status)",
                new MapSqlParameterSource()
                        .addValue("runId", runId)
                        .addValue("feed", feed.name())
                        .addValue("file", sourceFile)
                        .addValue("startedAt", utc(startedAt))
                        .addValue("status", "STARTED")
        );
    }

    public void finishFeed(
            UUID runId,
            FeedName feed,
            Instant finishedAt,
            String status,
            @Nullable Long stagedRows,
            @Nullable Long snapshotRows,
            @Nullable Integer deltaRows,
            @Nullable String errorMessage
    ) {
        jdbc.update(
                "UPDATE job_run_feed SET finished_at=:finishedAt, status=:status, staged_rows=:staged, " +
                        "snapshot_rows=:snap, delta_rows=:delta, error_message=:msg " +
                        "WHERE job_run_id=:runId AND feed_name=:feed",
                new MapSqlParameterSource()
                        .addValue("runId", runId)
                        .addValue("feed", feed.name())
                        .addValue("finishedAt", utc(finishedAt))
                        .addValue("status", status)
                        .addValue("staged", stagedRows)
                        .addValue("snap", snapshotRows)
                        .addValue("delta", deltaRows)
                        .addValue("msg", errorMessage)
        );
    }

    @Nullable
    public UUID findLatestSuccessfulRunId(LocalDate asOf) {
        List<UUID> ids = jdbc.query(
                "SELECT id FROM job_run WHERE as_of_date=:asOf AND status='SUCCESS' ORDER BY started_at DESC LIMIT 1",
                new MapSqlParameterSource("asOf", asOf),
                (rs, rowNum) -> UUID.fromString(rs.getString(1))
        );
        return ids.isEmpty() ? null : ids.get(0);
    }

    public List<Map<String, Object>> listRuns(@Nullable LocalDate from, @Nullable LocalDate to, int limit) {
        return jdbc.queryForList(
                "SELECT id, as_of_date, status, started_at, finished_at, error_message " +
                        "FROM job_run " +
                        "WHERE (:from IS NULL OR as_of_date >= :from) AND (:to IS NULL OR as_of_date <= :to) " +
                        "ORDER BY started_at DESC LIMIT :limit",
                new MapSqlParameterSource()
                        .addValue("from", from)
                        .addValue("to", to)
                        .addValue("limit", limit)
        );
    }

    @Nullable
    public Map<String, Object> getRun(UUID id) {
        var list = jdbc.queryForList(
                "SELECT id, as_of_date, status, started_at, finished_at, error_message FROM job_run WHERE id=:id",
                new MapSqlParameterSource("id", id)
        );
        return list.isEmpty() ? null : list.get(0);
    }

    public List<Map<String, Object>> listRunFeeds(UUID runId) {
        return jdbc.queryForList(
                "SELECT feed_name, status, source_file, staged_rows, snapshot_rows, delta_rows, started_at, finished_at, error_message " +
                        "FROM job_run_feed WHERE job_run_id=:runId ORDER BY feed_name",
                new MapSqlParameterSource("runId", runId)
        );
    }
}

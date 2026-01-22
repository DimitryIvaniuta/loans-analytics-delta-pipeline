package com.github.dzmitryivaniuta.loansanalytics.ingest;

import com.github.dzmitryivaniuta.loansanalytics.ingest.feed.FeedDefinition;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SnapshotRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public long countStagedRows(UUID runId, FeedDefinition feed, LocalDate asOf) {
        String sql = "SELECT COUNT(*) FROM " + feed.stagingTable() + " WHERE job_run_id=:id AND as_of_date=:asOf";
        return jdbc.query(sql, Map.of("id", runId, "asOf", asOf), rs -> {
            rs.next();
            return rs.getLong(1);
        });
    }

    public long countSnapshot(FeedDefinition feed, LocalDate asOf) {
        String sql = "SELECT COUNT(*) FROM " + feed.snapshotTable() + " WHERE as_of_date=:asOf";
        return jdbc.query(sql, Map.of("asOf", asOf), rs -> {
            rs.next();
            return rs.getLong(1);
        });
    }

    /**
     * Upserts staging rows into the daily snapshot table.
     *
     * <p>Row hash is computed in SQL using SHA-256 over the canonical JSON representation
     * of the row (excluding ingest metadata columns).</p>
     */
    public void upsertSnapshotFromStaging(UUID runId, FeedDefinition feed, LocalDate asOf) {
        String insertCols = String.join(",", feed.dataColumns());
        String selectCols = "s." + String.join(",s.", feed.dataColumns());

        // Exclude staging metadata from hash input.
        String hashExpr = "encode(digest((to_jsonb(s) - 'job_run_id' - 'as_of_date' - 'source_file' - 'loaded_at')::text, 'sha256'), 'hex')";

        String conflictCols = "as_of_date," + String.join(",", feed.primaryKeyColumns());

        // Update all non-PK data columns.
        StringBuilder update = new StringBuilder();
        boolean first = true;
        for (String col : feed.dataColumns()) {
            if (feed.primaryKeyColumns().contains(col)) {
                continue;
            }
            if (!first) update.append(", ");
            first = false;
            update.append(col).append(" = EXCLUDED.").append(col);
        }
        if (!update.isEmpty()) {
            update.append(", ");
        }
        update.append("row_hash = EXCLUDED.row_hash, source_file = EXCLUDED.source_file, ingested_at = EXCLUDED.ingested_at");

        String sql = """
                INSERT INTO %s (as_of_date,%s,row_hash,source_file,ingested_at)
                SELECT
                  s.as_of_date,%s,
                  %s AS row_hash,
                  s.source_file,
                  now() AS ingested_at
                FROM %s s
                WHERE s.job_run_id=:id AND s.as_of_date=:asOf
                ON CONFLICT (%s) DO UPDATE SET
                  %s
                """.formatted(
                feed.snapshotTable(),
                insertCols,
                selectCols,
                hashExpr,
                feed.stagingTable(),
                conflictCols,
                update
        );

        jdbc.update(sql, Map.of("id", runId, "asOf", asOf));
    }

    public void truncateStaging(UUID runId, FeedDefinition feed, LocalDate asOf) {
        jdbc.update("DELETE FROM " + feed.stagingTable() + " WHERE job_run_id=:id AND as_of_date=:asOf",
                Map.of("id", runId, "asOf", asOf));
    }

    public void deleteStaging(UUID runId, FeedDefinition feed) {
        jdbc.update("DELETE FROM " + feed.stagingTable() + " WHERE job_run_id=:id", Map.of("id", runId));
    }
}

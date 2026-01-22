package com.github.dzmitryivaniuta.loansanalytics.ingest;

import com.github.dzmitryivaniuta.loansanalytics.ingest.feed.FeedDefinition;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DeltaRepository {

    private final NamedParameterJdbcTemplate jdbc;

    /**
     * Generates a delta between current snapshot ({@code asOf}) and previous snapshot ({@code prevAsOf}) for a feed.
     *
     * <p>Change types:
     * <ul>
     *   <li>I - present in current, absent in previous</li>
     *   <li>U - present in both, row_hash changed</li>
     *   <li>D - absent in current, present in previous</li>
     * </ul>
     *
     * <p>For updates (U), {@code changed_fields} is computed as JSONB diff with before/after values per field.
     */
    public int generateDelta(UUID runId, FeedDefinition feed, LocalDate asOf, LocalDate prevAsOf) {
        jdbc.update("DELETE FROM delta_event WHERE job_run_id=:id AND feed_name=:feed",
                Map.of("id", runId, "feed", feed.name().name()));

        String join = feed.pkJoinCondition("c", "p");
        String entityKey = feed.entityKeyJsonExpr("c", "p");

        // remove snapshot metadata from payloads
        String currPayload = "(to_jsonb(c) - 'as_of_date' - 'row_hash' - 'source_file' - 'ingested_at')";
        String prevPayload = "(to_jsonb(p) - 'as_of_date' - 'row_hash' - 'source_file' - 'ingested_at')";

        String anyPkNullInPrev = "p." + feed.primaryKeyColumns().get(0) + " IS NULL";
        String anyPkNullInCurr = "c." + feed.primaryKeyColumns().get(0) + " IS NULL";

        String sql = """
                INSERT INTO delta_event(
                  job_run_id, feed_name, as_of_date, op,
                  entity_key, old_row_hash, new_row_hash,
                  before_row, after_row, changed_fields, created_at
                )
                SELECT
                  :id as job_run_id,
                  :feed as feed_name,
                  :asOf as as_of_date,
                  CASE
                    WHEN %s THEN 'I'
                    WHEN %s THEN 'D'
                    WHEN p.row_hash <> c.row_hash THEN 'U'
                  END as op,
                  %s as entity_key,
                  p.row_hash as old_row_hash,
                  c.row_hash as new_row_hash,
                  CASE WHEN %s THEN NULL ELSE %s END as before_row,
                  CASE WHEN %s THEN NULL ELSE %s END as after_row,
                  CASE
                    WHEN (p.row_hash <> c.row_hash) THEN jsonb_diff(%s, %s)
                    ELSE '{}'::jsonb
                  END as changed_fields,
                  now() as created_at
                FROM
                  (SELECT * FROM %s WHERE as_of_date=:asOf) c
                  FULL OUTER JOIN (SELECT * FROM %s WHERE as_of_date=:prev) p
                    ON %s
                WHERE
                  (%s) OR (%s) OR (p.row_hash <> c.row_hash)
                """.formatted(
                anyPkNullInPrev,
                anyPkNullInCurr,
                entityKey,
                anyPkNullInPrev,
                prevPayload,
                anyPkNullInCurr,
                currPayload,
                prevPayload,
                currPayload,
                feed.snapshotTable(),
                feed.snapshotTable(),
                join,
                anyPkNullInPrev,
                anyPkNullInCurr
        );

        return jdbc.update(sql, Map.of(
                "id", runId,
                "feed", feed.name().name(),
                "asOf", asOf,
                "prev", prevAsOf
        ));
    }

    public List<DeltaEventRow> findDeltaRows(UUID runId, String feedName) {
        return jdbc.query(
                """
                SELECT op, entity_key::text, before_row::text, after_row::text, changed_fields::text
                FROM delta_event
                WHERE job_run_id=:id AND feed_name=:feed
                ORDER BY entity_key
                """,
                Map.of("id", runId, "feed", feedName),
                (rs, rowNum) -> new DeltaEventRow(
                        rs.getString("op"),
                        rs.getString("entity_key"),
                        rs.getString("before_row"),
                        rs.getString("after_row"),
                        rs.getString("changed_fields")
                )
        );
    }

    public record DeltaEventRow(String op, String entityKeyJson, String beforeRowJson, String afterRowJson, String changedFieldsJson) {}
}

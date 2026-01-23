package com.github.dimitryivaniuta.loansanalytics.ingest;

import com.github.dimitryivaniuta.loansanalytics.ingest.feed.FeedDefinition;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.springframework.stereotype.Service;

/**
 * High-throughput CSV ingest using PostgreSQL COPY.
 *
 * <p>We parse the CSV header to build the COPY column list in the same order as the file.
 * The header is skipped by COPY (HEADER true). We still prefix our own metadata columns
 * (job_run_id, as_of_date, source_file) to each data row using a streaming wrapper.</p>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PostgresCopyService {

    private final DataSource dataSource;

    public long copyIntoStaging(UUID runId, LocalDate asOf, FeedDefinition feed, Path csvFile) {
        String sourceFile = csvFile.getFileName().toString();
        try {
            List<String> headers = readHeader(csvFile);
            List<String> mappedCols = feed.mapHeadersToDbColumns(headers);

            String copySql = buildCopySql(feed.stagingTable(), mappedCols);
            log.info("COPY {} -> {} ({} columns)", sourceFile, feed.stagingTable(), mappedCols.size());

            try (var conn = dataSource.getConnection()) {
                PGConnection pg = conn.unwrap(PGConnection.class);
                CopyManager cm = pg.getCopyAPI();

                try (InputStream in = Files.newInputStream(csvFile);
                     InputStream prefixed = new PrefixingCsvInputStream(in, runId, asOf, sourceFile)) {
                    return cm.copyIn(copySql, prefixed);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("COPY into staging failed for feed " + feed.name() + " file=" + csvFile, e);
        }
    }

    private static String buildCopySql(String stagingTable, List<String> mappedCols) {
        String cols = String.join(",", mappedCols);
        // We always prefix run metadata columns.
        return "COPY " + stagingTable + " (job_run_id,as_of_date,source_file," + cols + ") " +
                "FROM STDIN WITH (FORMAT csv, HEADER true, QUOTE '\"', ESCAPE '\"')";
    }

    private static List<String> readHeader(Path csvFile) throws IOException {
        try (var in = Files.newInputStream(csvFile);
             var br = new BufferedReader(new java.io.InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line = br.readLine();
            if (line == null || line.isBlank()) {
                throw new IllegalArgumentException("Empty CSV file: " + csvFile);
            }
            return parseCsvLine(line);
        }
    }

    /**
     * Minimal CSV line parser for header (supports quotes and commas inside quotes).
     */
    static List<String> parseCsvLine(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // escaped quote
                    cur.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                out.add(cur.toString().trim());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString().trim());
        return out;
    }
}

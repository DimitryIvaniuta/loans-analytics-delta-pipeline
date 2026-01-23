package com.github.dimitryivaniuta.loansanalytics.ingest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.UUID;

/**
 * Streams a CSV file while prefixing each line with run metadata columns:
 * job_run_id, as_of_date, source_file.
 *
 * <p>This keeps the ingest path COPY-friendly (no full file rewrite) and avoids row-by-row JDBC inserts.
 */
final class PrefixingCsvInputStream extends InputStream {

    private final BufferedReader reader;
    private final UUID runId;
    private final LocalDate asOf;
    private final String sourceFile;

    private byte[] buffer = new byte[0];
    private int pos = 0;
    private boolean firstLine = true;
    private boolean closed = false;

    PrefixingCsvInputStream(InputStream original, UUID runId, LocalDate asOf, String sourceFile) {
        this.reader = new BufferedReader(new InputStreamReader(original, StandardCharsets.UTF_8));
        this.runId = runId;
        this.asOf = asOf;
        this.sourceFile = sourceFile;
    }

    @Override
    public int read() throws IOException {
        if (closed) {
            return -1;
        }

        if (pos >= buffer.length) {
            if (!fillBuffer()) {
                closed = true;
                return -1;
            }
        }
        return buffer[pos++] & 0xFF;
    }

    private boolean fillBuffer() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return false;
        }

        String prefix = runId + "," + asOf + "," + csvQuote(sourceFile) + ",";
        String out;
        if (firstLine) {
            // Header
            out = "job_run_id,as_of_date,source_file," + line;
            firstLine = false;
        } else {
            out = prefix + line;
        }

        // COPY expects newline-separated records.
        out = out + "\n";
        buffer = out.getBytes(StandardCharsets.UTF_8);
        pos = 0;
        return true;
    }

    private static String csvQuote(String v) {
        String s = v == null ? "" : v;
        // If the value contains comma, quote, or newline - quote and escape.
        boolean mustQuote = s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
        if (!mustQuote) {
            return s;
        }
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    @Override
    public void close() throws IOException {
        reader.close();
        closed = true;
    }
}

package com.github.dzmitryivaniuta.loansanalytics.ingest.feed;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Declarative feed definition:
 * - file naming pattern
 * - staging/snapshot tables
 * - PK columns
 * - canonical DB data columns
 * - mapping from input CSV header -> DB column
 */
public record FeedDefinition(
        FeedName name,
        String filePattern,
        String stagingTable,
        String snapshotTable,
        List<String> primaryKeyColumns,
        List<String> dataColumns,
        Map<String, String> headerAliases
) {
    private static final DateTimeFormatter BASIC = DateTimeFormatter.BASIC_ISO_DATE;

    public FeedDefinition {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(filePattern, "filePattern");
        Objects.requireNonNull(stagingTable, "stagingTable");
        Objects.requireNonNull(snapshotTable, "snapshotTable");
        primaryKeyColumns = List.copyOf(primaryKeyColumns);
        dataColumns = List.copyOf(dataColumns);
        headerAliases = Map.copyOf(headerAliases == null ? Map.of() : headerAliases);
    }

    public String expectedFileName(LocalDate asOf) {
        String date = BASIC.format(asOf);
        return filePattern.formatted(date);
    }

    /**
     * Maps CSV header names into DB columns (in the same order as in the file).
     *
     * <p>Rules:
     * <ul>
     *   <li>Exact alias match (normalized) wins</li>
     *   <li>Otherwise, normalize header -> snake_case and use it directly</li>
     * </ul>
     */
    public List<String> mapHeadersToDbColumns(List<String> rawHeaders) {
        List<String> mapped = new ArrayList<>(rawHeaders.size());
        for (String h : rawHeaders) {
            String norm = normalizeHeader(h);
            String col = headerAliases.getOrDefault(norm, norm);
            if (!dataColumns.contains(col)) {
                throw new IllegalArgumentException("Unsupported column in feed " + name + ": '" + h + "' -> '" + col + "'. " +
                        "Add it to the schema or provide a header alias mapping.");
            }
            mapped.add(col);
        }
        return mapped;
    }

    public String pkJoinCondition(String leftAlias, String rightAlias) {
        List<String> parts = new ArrayList<>();
        for (String pk : primaryKeyColumns) {
            parts.add(leftAlias + "." + pk + " = " + rightAlias + "." + pk);
        }
        return String.join(" AND ", parts);
    }

    public String entityKeyJsonExpr(String currentAlias, String prevAlias) {
        // jsonb_build_object('loan_id', coalesce(c.loan_id, p.loan_id), ...)
        Map<String, String> kv = new LinkedHashMap<>();
        for (String pk : primaryKeyColumns) {
            kv.put(pk, "coalesce(" + currentAlias + "." + pk + ", " + prevAlias + "." + pk + ")");
        }
        StringBuilder sb = new StringBuilder("jsonb_build_object(");
        boolean first = true;
        for (var e : kv.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append("'").append(e.getKey()).append("', ").append(e.getValue());
        }
        sb.append(")");
        return sb.toString();
    }

    public static String normalizeHeader(String header) {
        if (header == null) return "";
        String h = header.trim();
        // strip surrounding quotes
        if (h.length() >= 2 && h.startsWith("\"") && h.endsWith("\"")) {
            h = h.substring(1, h.length() - 1);
        }
        h = h.trim().toLowerCase(Locale.ROOT);
        // replace separators with underscore
        h = h.replaceAll("[\\s/\\-]+", "_");
        // drop punctuation
        h = h.replaceAll("[^a-z0-9_]+", "");
        // collapse underscores
        h = h.replaceAll("_+", "_");
        // trim underscores
        h = h.replaceAll("^_+|_+$", "");
        return h;
    }
}

package com.github.dimitryivaniuta.loansanalytics.api;

import com.github.dimitryivaniuta.loansanalytics.ingest.DeltaRepository;
import com.github.dimitryivaniuta.loansanalytics.ingest.JobRunRepository;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class DeltaExportController {

    private final JobRunRepository jobRunRepository;
    private final DeltaRepository deltaRepository;

    /**
     * Streams the delta feed for the latest successful run for the given day.
     *
     * Downstream contract is unified across feeds:
     * feed_name, op(I/U/D), entity_key, changed_fields, before_row, after_row.
     */
    @GetMapping("/delta")
    public void downloadDelta(
            @RequestParam("asOf")
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
            LocalDate asOf,
            @RequestParam(value = "feed", required = false, defaultValue = "LOAN_MASTER") String feed,
            HttpServletResponse response
    ) throws IOException {
        UUID runId = jobRunRepository.findLatestSuccessfulRunId(asOf);
        if (runId == null) {
            response.sendError(404, "No successful run found for asOf=" + asOf);
            return;
        }

        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType("text/csv");
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=delta_" + feed + "_" + asOf + ".csv");

        var writer = response.getWriter();
        writer.write("feed_name,op,entity_key,changed_fields,before_row,after_row\n");

        for (var row : deltaRepository.findDeltaRows(runId, feed)) {
            writer.write(csv(feed));
            writer.write(',');
            writer.write(csv(row.op()));
            writer.write(',');
            writer.write(csv(row.entityKeyJson()));
            writer.write(',');
            writer.write(csv(row.changedFieldsJson()));
            writer.write(',');
            writer.write(csv(row.beforeRowJson()));
            writer.write(',');
            writer.write(csv(row.afterRowJson()));
            writer.write('\n');
        }
        writer.flush();
    }

    private static String csv(String v) {
        if (v == null) return "";
        boolean mustQuote = v.indexOf(',') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\n') >= 0 || v.indexOf('\r') >= 0;
        if (!mustQuote) return v;
        return '"' + v.replace("\"", "\"\"") + '"';
    }
}

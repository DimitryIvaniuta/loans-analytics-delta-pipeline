package com.github.dimitryivaniuta.loansanalytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dimitryivaniuta.loansanalytics.ingest.DeltaRepository;
import com.github.dimitryivaniuta.loansanalytics.ingest.IngestionOrchestrator;
import com.github.dimitryivaniuta.loansanalytics.ingest.feed.FeedName;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
class IngestionOrchestratorIT {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("loans")
            .withUsername("loans")
            .withPassword("loans");

    static Path inputDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws IOException {
        if (inputDir == null) {
            inputDir = Files.createTempDirectory("loans-input-");
        }
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add("loans.ingestion.input-dir", () -> inputDir.toAbsolutePath().toString());
        registry.add("loans.ingestion.enabled-feeds", () -> "LOAN_MASTER,PAYMENT_TRANSACTION");
    }

    @Autowired
    IngestionOrchestrator orchestrator;

    @Autowired
    DeltaRepository deltaRepository;

    @Autowired
    ObjectMapper objectMapper;

    @AfterEach
    void cleanup() throws IOException {
        if (inputDir != null) {
            try (var s = Files.list(inputDir)) {
                s.forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }

    @Test
    void shouldGenerateInsertUpdateDeleteDeltaAndChangedFieldsForLoanMasterAndPaymentTransaction() throws Exception {
        LocalDate day1 = LocalDate.of(2026, 1, 17);
        LocalDate day2 = LocalDate.of(2026, 1, 18);

        // Day 1 files
        writeLoanMaster(day1,
                loanMasterRow("L1", "B1", "HOME", "OPEN", "2024-01-01", "2034-01-01", "100.00", "EUR", "0.035000", "0.8000", "BR1", "NORTH", "2026-01-17T00:00:00Z"),
                loanMasterRow("L2", "B2", "AUTO", "OPEN", "2024-01-01", "2029-01-01", "200.00", "EUR", "0.055000", "0.9000", "BR2", "SOUTH", "2026-01-17T00:00:00Z")
        );
        writePaymentTransaction(day1,
                paymentTxRow("T1", "L1", "2026-01-17", "2026-01-17", "ACH", "10.00", "EUR", "ONLINE", "R1"),
                paymentTxRow("T2", "L2", "2026-01-17", "2026-01-17", "CASH", "20.00", "EUR", "BRANCH", "R2")
        );

        orchestrator.ingestAndGenerateDelta(day1, Set.of(FeedName.LOAN_MASTER, FeedName.PAYMENT_TRANSACTION));

        // Day 2 files: L1 updated, L2 deleted, L3 inserted. T1 updated, T2 deleted, T3 inserted.
        writeLoanMaster(day2,
                loanMasterRow("L1", "B1", "HOME", "OPEN", "2024-01-01", "2034-01-01", "110.00", "EUR", "0.035000", "0.8000", "BR1", "NORTH", "2026-01-18T00:00:00Z"),
                loanMasterRow("L3", "B3", "CASH", "OPEN", "2026-01-18", "2027-01-18", "999.99", "EUR", "0.075000", "0.7000", "BR3", "EAST", "2026-01-18T00:00:00Z")
        );
        writePaymentTransaction(day2,
                paymentTxRow("T1", "L1", "2026-01-18", "2026-01-18", "ACH", "11.00", "EUR", "ONLINE", "R1"),
                paymentTxRow("T3", "L3", "2026-01-18", "2026-01-18", "ACH", "33.00", "EUR", "ONLINE", "R3")
        );

        var runId = orchestrator.ingestAndGenerateDelta(day2, Set.of(FeedName.LOAN_MASTER, FeedName.PAYMENT_TRANSACTION));

        // =========================
        // LOAN_MASTER assertions
        // =========================
        var loanDelta = deltaRepository.findDeltaRows(runId, "LOAN_MASTER");
        assertThat(loanDelta).hasSize(3);
        assertThat(loanDelta).extracting(DeltaRepository.DeltaEventRow::op)
                .containsExactlyInAnyOrder("I", "U", "D");

        var loanUpdate = loanDelta.stream().filter(r -> r.op().equals("U") && r.entityKeyJson().contains("L1")).findFirst().orElseThrow();
        JsonNode loanChanged = objectMapper.readTree(loanUpdate.changedFieldsJson());
        assertThat(loanChanged.has("principal_balance")).isTrue();
        assertThat(new BigDecimal(loanChanged.get("principal_balance").get("before").asText())
                .setScale(2, RoundingMode.UNNECESSARY))
                .isEqualTo(new BigDecimal("100.00").setScale(2, RoundingMode.UNNECESSARY));

        assertThat(new BigDecimal(loanChanged.get("principal_balance").get("after").asText())
                .setScale(2, RoundingMode.UNNECESSARY))
                .isEqualTo(new BigDecimal("110.00").setScale(2, RoundingMode.UNNECESSARY));

        var loanInsert = loanDelta.stream().filter(r -> r.op().equals("I") && r.entityKeyJson().contains("L3")).findFirst().orElseThrow();
        assertThat(loanInsert.beforeRowJson()).isNull();
        assertThat(loanInsert.afterRowJson()).contains("\"loan_id\": \"L3\"");

        var loanDelete = loanDelta.stream().filter(r -> r.op().equals("D") && r.entityKeyJson().contains("L2")).findFirst().orElseThrow();
        assertThat(loanDelete.afterRowJson()).isNull();
        assertThat(loanDelete.beforeRowJson()).contains("\"loan_id\": \"L2\"");

        // =========================
        // PAYMENT_TRANSACTION assertions
        // =========================
        var txDelta = deltaRepository.findDeltaRows(runId, "PAYMENT_TRANSACTION");
        assertThat(txDelta).hasSize(3);
        assertThat(txDelta).extracting(DeltaRepository.DeltaEventRow::op)
                .containsExactlyInAnyOrder("I", "U", "D");

        var txUpdate = txDelta.stream().filter(r -> r.op().equals("U") && r.entityKeyJson().contains("T1")).findFirst().orElseThrow();
        JsonNode txChanged = objectMapper.readTree(txUpdate.changedFieldsJson());
        assertThat(txChanged.has("amount")).isTrue();
        assertThat(new BigDecimal(txChanged.get("amount").get("before").asText())
                .setScale(2, RoundingMode.UNNECESSARY))
                .isEqualTo(new BigDecimal("10.00").setScale(2, RoundingMode.UNNECESSARY));
        assertThat(new BigDecimal(txChanged.get("amount").get("after").asText())
                .setScale(2, RoundingMode.UNNECESSARY))
                .isEqualTo(new BigDecimal("11.00").setScale(2, RoundingMode.UNNECESSARY));

        var txInsert = txDelta.stream().filter(r -> r.op().equals("I") && r.entityKeyJson().contains("T3")).findFirst().orElseThrow();
        assertThat(txInsert.beforeRowJson()).isNull();
        assertThat(txInsert.afterRowJson()).contains("\"transaction_id\": \"T3\"");

        var txDelete = txDelta.stream().filter(r -> r.op().equals("D") && r.entityKeyJson().contains("T2")).findFirst().orElseThrow();
        assertThat(txDelete.afterRowJson()).isNull();
        assertThat(txDelete.beforeRowJson()).contains("\"transaction_id\": \"T2\"");
    }

    private static void writeLoanMaster(LocalDate asOf, String... rows) throws IOException {
        String fn = "loan_master_%s.csv".formatted(asOf.format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE));
        Path p = inputDir.resolve(fn);
        String header = String.join(",",
                "loan_id", "borrower_id", "product_code", "status", "origination_date", "maturity_date", "principal_balance", "currency", "interest_rate", "ltv", "branch_id", "region", "last_modified_at");
        var sb = new StringBuilder();
        sb.append(header).append("\n");
        for (String r : rows) sb.append(r).append("\n");
        Files.writeString(p, sb.toString());
    }

    private static void writePaymentTransaction(LocalDate asOf, String... rows) throws IOException {
        String fn = "payment_transaction_%s.csv".formatted(asOf.format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE));
        Path p = inputDir.resolve(fn);
        String header = String.join(",",
                "transaction_id", "loan_id", "transaction_date", "posting_date", "transaction_type", "amount", "currency", "channel", "reference");
        var sb = new StringBuilder();
        sb.append(header).append("\n");
        for (String r : rows) sb.append(r).append("\n");
        Files.writeString(p, sb.toString());
    }

    private static String loanMasterRow(String loanId, String borrowerId, String productCode, String status,
                                        String originationDate, String maturityDate, String principalBalance,
                                        String currency, String interestRate, String ltv, String branchId,
                                        String region, String lastModifiedAt) {
        return String.join(",",
                loanId, borrowerId, productCode, status, originationDate, maturityDate,
                principalBalance, currency, interestRate, ltv, branchId, region, lastModifiedAt);
    }

    private static String paymentTxRow(String transactionId, String loanId, String transactionDate, String postingDate,
                                       String transactionType, String amount, String currency, String channel,
                                       String reference) {
        return String.join(",",
                transactionId, loanId, transactionDate, postingDate, transactionType, amount,
                currency, channel, reference);
    }
}

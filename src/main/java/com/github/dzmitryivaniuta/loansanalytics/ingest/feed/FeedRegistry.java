package com.github.dzmitryivaniuta.loansanalytics.ingest.feed;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Central registry of all supported feeds (11 distinct schemas).
 */
@Component
public class FeedRegistry {

    private final Map<FeedName, FeedDefinition> byName;

    public FeedRegistry() {
        Map<FeedName, FeedDefinition> m = new LinkedHashMap<>();

        // 1) LOAN_MASTER
        m.put(FeedName.LOAN_MASTER, new FeedDefinition(
                FeedName.LOAN_MASTER,
                "loan_master_%s.csv",
                "stg_loan_master",
                "snap_loan_master",
                List.of("loan_id"),
                List.of(
                        "loan_id",
                        "borrower_id",
                        "product_code",
                        "status",
                        "origination_date",
                        "maturity_date",
                        "principal_balance",
                        "currency",
                        "interest_rate",
                        "ltv",
                        "branch_id",
                        "region",
                        "last_modified_at"
                ),
                Map.ofEntries(
                        Map.entry("loan_id", "loan_id"),
                        Map.entry("loanid", "loan_id"),
                        Map.entry("loan", "loan_id"),
                        Map.entry("borrower_id", "borrower_id"),
                        Map.entry("borrowerid", "borrower_id"),
                        Map.entry("product_code", "product_code"),
                        Map.entry("productcode", "product_code"),
                        Map.entry("principal_balance", "principal_balance"),
                        Map.entry("principalbalance", "principal_balance"),
                        Map.entry("interest_rate", "interest_rate"),
                        Map.entry("interestrate", "interest_rate"),
                        Map.entry("last_modified_date", "last_modified_at"),
                        Map.entry("lastmodifieddate", "last_modified_at"),
                        Map.entry("last_modified_at", "last_modified_at")
                )
        ));

        // 6) PAYMENT_TRANSACTION
        m.put(FeedName.PAYMENT_TRANSACTION, new FeedDefinition(
                FeedName.PAYMENT_TRANSACTION,
                "payment_transaction_%s.csv",
                "stg_payment_transaction",
                "snap_payment_transaction",
                List.of("transaction_id"),
                List.of(
                        "transaction_id",
                        "loan_id",
                        "transaction_date",
                        "posting_date",
                        "transaction_type",
                        "amount",
                        "currency",
                        "channel",
                        "reference"
                ),
                Map.of(
                        "transaction_id", "transaction_id",
                        "transactionid", "transaction_id",
                        "posting_date", "posting_date",
                        "postingdate", "posting_date",
                        "transaction_date", "transaction_date",
                        "transactiondate", "transaction_date",
                        "transaction_type", "transaction_type",
                        "transactiontype", "transaction_type"
                )
        ));

        // 2) BORROWER
        m.put(FeedName.BORROWER, new FeedDefinition(
                FeedName.BORROWER,
                "borrower_%s.csv",
                "stg_borrower",
                "snap_borrower",
                List.of("borrower_id"),
                List.of("borrower_id","first_name","last_name","date_of_birth","national_id_hash","email","phone","employer","annual_income","created_date","modified_date"),
                Map.of(
                        "borrowerid","borrower_id",
                        "dateofbirth","date_of_birth",
                        "nationalidhash","national_id_hash",
                        "annualincome","annual_income",
                        "createddate","created_date",
                        "modifieddate","modified_date"
                )
        ));

        // 3) COBORROWER
        m.put(FeedName.COBORROWER, new FeedDefinition(
                FeedName.COBORROWER,
                "coborrower_%s.csv",
                "stg_coborrower",
                "snap_coborrower",
                List.of("loan_id","coborrower_id"),
                List.of("loan_id","coborrower_id","first_name","last_name","date_of_birth","relationship","email","phone"),
                Map.of(
                        "coborrowerid","coborrower_id",
                        "dateofbirth","date_of_birth"
                )
        ));

        // 4) COLLATERAL
        m.put(FeedName.COLLATERAL, new FeedDefinition(
                FeedName.COLLATERAL,
                "collateral_%s.csv",
                "stg_collateral",
                "snap_collateral",
                List.of("collateral_id"),
                List.of("collateral_id","loan_id","property_type","street","city","state","postal_code","country","valuation_amount","valuation_date","occupancy","year_built"),
                Map.of(
                        "collateralid","collateral_id",
                        "propertytype","property_type",
                        "postalcode","postal_code",
                        "valuationamount","valuation_amount",
                        "valuationdate","valuation_date",
                        "yearbuilt","year_built"
                )
        ));

        // 5) PAYMENT_SCHEDULE
        m.put(FeedName.PAYMENT_SCHEDULE, new FeedDefinition(
                FeedName.PAYMENT_SCHEDULE,
                "payment_schedule_%s.csv",
                "stg_payment_schedule",
                "snap_payment_schedule",
                List.of("loan_id","installment_no"),
                List.of("loan_id","installment_no","due_date","due_amount","principal_due","interest_due","escrow_due","status"),
                Map.of(
                        "installmentno","installment_no",
                        "duedate","due_date",
                        "dueamount","due_amount",
                        "principaldue","principal_due",
                        "interestdue","interest_due",
                        "escrowdue","escrow_due"
                )
        ));

        // 7) DELINQUENCY
        m.put(FeedName.DELINQUENCY, new FeedDefinition(
                FeedName.DELINQUENCY,
                "delinquency_%s.csv",
                "stg_delinquency",
                "snap_delinquency",
                List.of("loan_id"),
                List.of("loan_id","days_past_due","delinquency_bucket","next_action","next_action_date","hardship_flag"),
                Map.of(
                        "dayspastdue","days_past_due",
                        "delinquencybucket","delinquency_bucket",
                        "nextaction","next_action",
                        "nextactiondate","next_action_date",
                        "hardshipflag","hardship_flag"
                )
        ));

        // 8) RATE
        m.put(FeedName.RATE, new FeedDefinition(
                FeedName.RATE,
                "rate_%s.csv",
                "stg_rate",
                "snap_rate",
                List.of("loan_id"),
                List.of("loan_id","rate_type","index_name","margin","current_rate","next_reset_date","cap","floor"),
                Map.of(
                        "ratetype","rate_type",
                        "indexname","index_name",
                        "currentrate","current_rate",
                        "nextresetdate","next_reset_date"
                )
        ));

        // 9) ESCROW
        m.put(FeedName.ESCROW, new FeedDefinition(
                FeedName.ESCROW,
                "escrow_%s.csv",
                "stg_escrow",
                "snap_escrow",
                List.of("loan_id"),
                List.of("loan_id","escrow_balance","tax_reserve","insurance_reserve","hazard_policy_no","hazard_premium","flood_policy_no","flood_premium"),
                Map.of(
                        "escrowbalance","escrow_balance",
                        "taxreserve","tax_reserve",
                        "insurancereserve","insurance_reserve",
                        "hazardpolicyno","hazard_policy_no",
                        "hazardpremium","hazard_premium",
                        "floodpolicyno","flood_policy_no",
                        "floodpremium","flood_premium"
                )
        ));

        // 10) MODIFICATION
        m.put(FeedName.MODIFICATION, new FeedDefinition(
                FeedName.MODIFICATION,
                "modification_%s.csv",
                "stg_modification",
                "snap_modification",
                List.of("modification_id"),
                List.of("modification_id","loan_id","modification_type","effective_date","new_interest_rate","new_term_months","reason","status"),
                Map.of(
                        "modificationid","modification_id",
                        "modificationtype","modification_type",
                        "effectivedate","effective_date",
                        "newinterestrate","new_interest_rate",
                        "newtermmonths","new_term_months"
                )
        ));

        // 11) CONTACT_CRM (curated subset + robust header aliases based on user's example)
        m.put(FeedName.CONTACT_CRM, new FeedDefinition(
                FeedName.CONTACT_CRM,
                "contact_crm_%s.csv",
                "stg_contact_crm",
                "snap_contact_crm",
                List.of("contact_id"),
                List.of(
                        "contact_id",
                        "email",
                        "secondary_email",
                        "office_phone",
                        "home_phone",
                        "cell_phone",
                        "first_name",
                        "last_name",
                        "company",
                        "position",
                        "city",
                        "state_province",
                        "zip_postal_code",
                        "country",
                        "created_date",
                        "modified_date"
                ),
                Map.ofEntries(
                        // e.g. "ZIP/Postal Code" -> zip_postal_code
                        Map.entry("zip_postal_code", "zip_postal_code"),
                        Map.entry("zippostalcode", "zip_postal_code"),
                        Map.entry("postal_code", "zip_postal_code"),
                        Map.entry("state_province", "state_province"),
                        Map.entry("state_province_territory", "state_province"),
                        Map.entry("office_phone", "office_phone"),
                        Map.entry("home_phone", "home_phone"),
                        Map.entry("cell_phone", "cell_phone"),
                        Map.entry("secondary_email", "secondary_email"),
                        Map.entry("created_date", "created_date"),
                        Map.entry("createddate", "created_date"),
                        Map.entry("modified_date", "modified_date"),
                        Map.entry("modifieddate", "modified_date")
                )
        ));

        this.byName = Map.copyOf(m);
    }

    public FeedDefinition get(FeedName name) {
        FeedDefinition d = byName.get(name);
        if (d == null) {
            throw new IllegalArgumentException("Unknown feed: " + name);
        }
        return d;
    }

    public List<FeedDefinition> all() {
        return new ArrayList<>(byName.values());
    }
}

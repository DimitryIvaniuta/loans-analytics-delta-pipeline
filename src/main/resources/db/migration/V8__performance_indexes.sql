CREATE INDEX IF NOT EXISTS ix_snap_loan_master_asof ON snap_loan_master(as_of_date);
CREATE INDEX IF NOT EXISTS ix_snap_payment_tx_asof ON snap_payment_transaction(as_of_date);


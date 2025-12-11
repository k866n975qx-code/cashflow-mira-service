-- =====================================================================
-- Unified schema for Cashflow service (one file to apply on any server)
-- =====================================================================
BEGIN;

-- ===== RAW MIRROR FROM LUNCH MONEY =====
CREATE TABLE IF NOT EXISTS lm_transactions (
  lm_id            BIGINT PRIMARY KEY,
  date_posted      DATE NOT NULL,
  amount           NUMERIC(14,4) NOT NULL,        -- - outflow, + inflow
  currency         CHAR(3) NOT NULL,
  payee            TEXT,
  note             TEXT,
  account_id       TEXT,                          -- LM asset id/name if present
  plaid_account_id TEXT                           -- LM's plaid account id if present
);
CREATE INDEX IF NOT EXISTS idx_lm_txn_date ON lm_transactions(date_posted);

-- ===== LOCAL WORKING COPY (INSERT-ONLY FROM MIRROR) =====
CREATE TABLE IF NOT EXISTS transactions (
  lm_id        BIGINT PRIMARY KEY REFERENCES lm_transactions(lm_id) ON DELETE CASCADE,
  date_posted  DATE NOT NULL,
  amount       NUMERIC(14,4) NOT NULL,
  currency     CHAR(3) NOT NULL,
  payee        TEXT,
  note         TEXT,
  category     TEXT,                               -- your tag
  ignored      BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_txn_date ON transactions(date_posted);
CREATE INDEX IF NOT EXISTS idx_txn_category ON transactions(category);

-- minimal change log for edits
CREATE TABLE IF NOT EXISTS transaction_changes (
  id         BIGSERIAL PRIMARY KEY,
  lm_id      BIGINT NOT NULL REFERENCES transactions(lm_id) ON DELETE CASCADE,
  field      TEXT   NOT NULL,      -- 'category' | 'ignored' | 'note' | 'payee'
  old_value  TEXT,
  new_value  TEXT,
  changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_txn_changes_lm_id ON transaction_changes(lm_id);

-- ===== CATEGORIES (YOUR FLAGS) =====
CREATE TABLE IF NOT EXISTS categories (
  id               TEXT PRIMARY KEY,               -- you provide (e.g., 'groceries') or UUID/text
  name             TEXT NOT NULL UNIQUE,
  affects_cashflow BOOLEAN NOT NULL DEFAULT true,  -- false => excluded from cashflow, no budgets allowed
  budgetable       BOOLEAN NOT NULL DEFAULT true,  -- budget rows only allowed if true and affects_cashflow=true
  is_income        BOOLEAN NOT NULL DEFAULT false  -- marks income categories (used for EF/inflow calc)
);

-- ===== MONTHLY BUDGETS (NO CARRYOVER) =====
CREATE TABLE IF NOT EXISTS budgets (
  category_id TEXT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
  year        INT  NOT NULL,
  month       INT  NOT NULL CHECK (month BETWEEN 1 AND 12),
  amount      NUMERIC(14,4) NOT NULL,
  PRIMARY KEY (category_id, year, month)
);

-- ===== RECURRING BILLS (SIMPLE) + LEDGER FOR PROGRESS/PAID =====
CREATE TABLE IF NOT EXISTS bills (
  id           TEXT PRIMARY KEY,                   -- you provide (e.g., 'rent', 'car_ins')
  name         TEXT NOT NULL,
  amount       NUMERIC(14,4) NOT NULL,
  currency     CHAR(3) NOT NULL,
  frequency    TEXT NOT NULL CHECK (frequency IN ('weekly','monthly','yearly')),
  weekday      INT  CHECK (weekday BETWEEN 0 AND 6),         -- for weekly (0=Mon .. 6=Sun)
  day_of_month INT  CHECK (day_of_month BETWEEN 1 AND 31),   -- for monthly/yearly
  start_date   DATE,
  end_date     DATE
);

CREATE TABLE IF NOT EXISTS bill_ledger (
  id          BIGSERIAL PRIMARY KEY,
  bill_id     TEXT NOT NULL REFERENCES bills(id) ON DELETE CASCADE,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  amount      NUMERIC(14,4) NOT NULL,             -- + contribution; - payment
  note        TEXT
);
CREATE INDEX IF NOT EXISTS idx_bill_ledger_bill ON bill_ledger(bill_id, occurred_at DESC);

-- ===== ACCOUNTS & BALANCE SNAPSHOTS =====
CREATE TABLE IF NOT EXISTS accounts (
  id         TEXT PRIMARY KEY,                     -- LM asset id or Plaid account id
  name       TEXT,
  provider   TEXT,                                 -- 'plaid' | 'asset'
  type       TEXT,                                 -- e.g., 'depository','credit','investment','loan'
  subtype    TEXT,                                 -- e.g., 'checking','savings'
  is_liquid  BOOLEAN NOT NULL DEFAULT false        -- toggle via API; EF uses only liquid=true
);

CREATE TABLE IF NOT EXISTS account_balances (
  id         BIGSERIAL PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  balance    NUMERIC(14,4) NOT NULL,
  currency   CHAR(3) NOT NULL,
  as_of      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_acc_balances ON account_balances(account_id, as_of DESC);

COMMIT;

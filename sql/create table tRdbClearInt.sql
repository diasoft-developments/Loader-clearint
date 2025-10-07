-- if OBJECT_ID('tRdbClearint') is not null
--   drop table tRdbClearint
go
create table tRdbClearint
(
    ID INT IDENTITY(1,1) PRIMARY KEY,
    SettlementDate DATE,                        -- Дата расчетов
    OperationType VARCHAR(50),                  -- Тип операции (например: CMTP510TPTP508)
    Direction CHAR(1),                          -- D / C
    OperationCount INT,                         -- Количество операций
    AmountAccountCurrency DECIMAL(18,2),        -- Сумма в валюте счета
    AccountCurrencyCode CHAR(3),                -- Код валюты счета (например: 810)
    AmountSettlementCurrency DECIMAL(18,2),     -- Сумма в валюте расчетов
    SettlementCurrencyCode CHAR(3),             -- Код валюты расчетов
    AmountForSettlement DECIMAL(18,2),          -- Сумма для расчетов
    AmountForSettlementCurrencyCode CHAR(3)     -- Код валюты суммы для расчетов
)
go
grant all on tRdbClearint to public



-- if OBJECT_ID('tRdbClearintMir') is not null
--   drop table tRdbClearintMir
go
create table tRdbClearintMir
(
    ID INT IDENTITY(1,1) PRIMARY KEY,
    SettlementDate       DATE,              -- Дата расчетов
    Direction            CHAR(1),
    CurrencyCode         CHAR(3),           -- Код валюты счета (например: 810)
    Amount               DECIMAL(18,2),     -- Сумма в валюте расчетов
    Purpose              varchar(256)       -- Код валюты суммы для расчетов
)
go
grant all on tRdbClearintMir to public


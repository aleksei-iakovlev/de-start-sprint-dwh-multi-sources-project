DROP TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- идентификатор записи
    courier_id            INTEGER        NOT NULL,                        -- ID курьера
    courier_name          VARCHAR(255)   NOT NULL,                        -- Ф. И. О. курьера
    settlement_year       SMALLINT       NOT NULL,                        -- год отчёта
    settlement_month      SMALLINT       NOT NULL,                        -- месяц отчёта: 1 — январь, 12 — декабрь
    orders_count          INTEGER        NOT NULL DEFAULT 0,              -- количество заказов за период
    orders_total_sum      NUMERIC(14, 2) NOT NULL DEFAULT 0,              -- общая стоимость заказов
    rate_avg              NUMERIC(3, 2)  NOT NULL,                        -- средний рейтинг курьера
    order_processing_fee  NUMERIC(14, 2) NOT NULL DEFAULT 0,              -- orders_total_sum * 0.25
    courier_order_sum     NUMERIC(14, 2) NOT NULL DEFAULT 0,              -- выплата за доставленные заказы
    courier_tips_sum      NUMERIC(14, 2) NOT NULL DEFAULT 0,              -- сумма чаевых
    courier_reward_sum    NUMERIC(14, 2) NOT NULL DEFAULT 0,              -- courier_order_sum + courier_tips_sum * 0.95

    -- Ограничения целостности
    CONSTRAINT dm_courier_ledger_month_chk
        CHECK (settlement_month BETWEEN 1 AND 12),
    CONSTRAINT dm_courier_ledger_year_chk
        CHECK (settlement_year >= 1900),
    CONSTRAINT dm_courier_ledger_rate_chk
        CHECK (rate_avg >= 1 AND rate_avg <= 5),
    CONSTRAINT dm_courier_ledger_orders_count_chk
        CHECK (orders_count >= 0),
    CONSTRAINT dm_courier_ledger_orders_total_sum_chk
        CHECK (orders_total_sum >= 0),
    CONSTRAINT dm_courier_ledger_order_processing_fee_chk
        CHECK (order_processing_fee >= 0),
    CONSTRAINT dm_courier_ledger_courier_order_sum_chk
        CHECK (courier_order_sum >= 0),
    CONSTRAINT dm_courier_ledger_courier_tips_sum_chk
        CHECK (courier_tips_sum >= 0),
    CONSTRAINT dm_courier_ledger_courier_reward_sum_chk
        CHECK (courier_reward_sum >= 0),

    -- Одна строка на курьера за месяц
    CONSTRAINT dm_courier_ledger_courier_period_uq
        UNIQUE (courier_id, settlement_year, settlement_month)
);

INSERT INTO cdm.dm_courier_ledger (
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum
)
WITH aggregated AS (
    SELECT
        d.courier_id,
        c.courier_name,
        EXTRACT(YEAR  FROM d.delivery_ts)::SMALLINT AS settlement_year,
        EXTRACT(MONTH FROM d.delivery_ts)::SMALLINT AS settlement_month,
        COUNT(DISTINCT d.order_id)                  AS orders_count,
        SUM(d.sum)                                  AS orders_total_sum,
        ROUND(AVG(d.rate)::NUMERIC, 2)              AS rate_avg,
        SUM(d.tip_sum)                              AS courier_tips_sum
    FROM dds.dm_deliveries AS d
    JOIN dds.dm_couriers AS c
      ON c.courier_id = d.courier_id   -- справочник курьеров с Ф. И. О.
    WHERE d.delivery_ts IS NOT NULL    -- учитываем только совершённые доставки
    GROUP BY
        d.courier_id,
        c.courier_name,
        EXTRACT(YEAR  FROM d.delivery_ts),
        EXTRACT(MONTH FROM d.delivery_ts)
),
calc AS (
    SELECT
        a.*,
        ROUND(a.orders_total_sum * 0.25, 2) AS order_processing_fee,
        ROUND(
            CASE
                WHEN a.rate_avg < 4   THEN GREATEST(a.orders_total_sum * 0.05, 100)
                WHEN a.rate_avg < 4.5 THEN GREATEST(a.orders_total_sum * 0.07, 150)
                WHEN a.rate_avg < 4.9 THEN GREATEST(a.orders_total_sum * 0.08, 175)
                ELSE                       GREATEST(a.orders_total_sum * 0.10, 200)
            END,
            2
        ) AS courier_order_sum
    FROM aggregated AS a
)
SELECT
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    ROUND(courier_order_sum + courier_tips_sum * 0.95, 2) AS courier_reward_sum
FROM calc
ON CONFLICT (courier_id, settlement_year, settlement_month)
DO UPDATE SET
    courier_name          = EXCLUDED.courier_name,
    orders_count          = EXCLUDED.orders_count,
    orders_total_sum      = EXCLUDED.orders_total_sum,
    rate_avg              = EXCLUDED.rate_avg,
    order_processing_fee  = EXCLUDED.order_processing_fee,
    courier_order_sum     = EXCLUDED.courier_order_sum,
    courier_tips_sum      = EXCLUDED.courier_tips_sum,
    courier_reward_sum    = EXCLUDED.courier_reward_sum;
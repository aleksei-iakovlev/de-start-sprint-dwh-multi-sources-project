from repositories.pg_connect import PgConnect


class CourierLedgerRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_report(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    """
                )
                conn.commit()


class CourierLedgerLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = CourierLedgerRepository(pg)

    def load_report(self):
        self.repository.load_report()

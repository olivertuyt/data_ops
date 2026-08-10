import argparse

from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

SEED = 42
START = "2026-07-01 00:00:00"
SPAN_SECONDS = 21 * 24 * 3600  # 2026-07-01 .. 2026-07-21

SCALES = {
    "small": dict(customers=100_000, orders=500_000, items=1_300_000, payments=600_000),
    "medium": dict(customers=200_000, orders=5_000_000, items=13_000_000, payments=6_000_000),
    "large": dict(customers=500_000, orders=15_000_000, items=40_000_000, payments=18_000_000),
}

FIRST_NAMES = ["Ana", "Bruno", "Carla", "Diego", "Elena", "Felipe", "Gabi", "Hugo", "Igor", "Julia"]
LAST_NAMES = ["Silva", "Souza", "Costa", "Santos", "Oliveira", "Pereira", "Lima", "Gomes"]
CITIES = ["sao paulo", "rio de janeiro", "belo horizonte", "curitiba", "porto alegre", "salvador"]
STATES = ["SP", "RJ", "MG", "PR", "RS", "BA"]
NEIGHBORHOODS = ["centro", "jardins", "vila nova", "boa vista", "santa cruz", "liberdade"]
STREETS = ["rua das flores", "avenida paulista", "rua augusta", "avenida brasil", "rua sete"]
CATEGORIES = ["electronics", "furniture", "toys", "health_beauty", "sports", "auto", "books", "garden"]
CHANNELS = ["web", "mobile_app", "marketplace"]
DEVICES = ["desktop", "ios", "android"]
PAYMENT_TYPES = ["credit_card", "debit_card", "voucher", "boleto"]
PROVIDERS = ["cielo", "stone", "pagseguro", "adyen"]
CARD_BRANDS = ["visa", "mastercard", "elo", "amex"]


def _pick(rand_col, values):
    expr = F.lit(values[-1])
    step = 1.0 / len(values)
    for i in range(len(values) - 2, -1, -1):
        expr = F.when(rand_col < step * (i + 1), F.lit(values[i])).otherwise(expr)
    return expr


def _hex(seed_shift, n_chars):
    # Random hex text compresses poorly, so each row carries real bytes.
    full = F.md5(F.rand(SEED + seed_shift).cast("string"))
    reps = (n_chars + 31) // 32
    return F.substring(F.concat(*[full] * reps), 1, n_chars)


def _purchase_seconds(order_num):
    # Purchase time is a hash of the order number, so order_items can derive the
    # exact same date from its order reference without joining orders.
    return F.pmod(order_num * F.lit(2654435761), F.lit(SPAN_SECONDS))


def gen_customers(spark, n):
    return (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "cid")
        .withColumn("customer_id", F.concat(F.lit("cust_"), F.lpad(F.col("cid").cast("string"), 8, "0")))
        .withColumn(
            "customer_name",
            F.concat(_pick(F.rand(SEED + 1), FIRST_NAMES), F.lit(" "), _pick(F.rand(SEED + 2), LAST_NAMES)),
        )
        .withColumn(
            "customer_zip_code_prefix",
            F.lpad((F.rand(SEED + 3) * 90000 + 1000).cast("int").cast("string"), 5, "0"),
        )
        .withColumn("customer_city", _pick(F.rand(SEED), CITIES))
        .withColumn("customer_state", _pick(F.rand(SEED + 4), STATES))
        .withColumn("customer_street", F.concat(_pick(F.rand(SEED + 5), STREETS), F.lit(" "), _hex(5, 12)))
        .withColumn("customer_number", (F.rand(SEED + 6) * 9999 + 1).cast("int"))
        .withColumn("customer_neighborhood", _pick(F.rand(SEED + 7), NEIGHBORHOODS))
        .withColumn(
            "customer_phone",
            F.concat(F.lit("+55"), (F.rand(SEED + 8) * 89999999999 + 10000000000).cast("long").cast("string")),
        )
        .withColumn("customer_email", F.concat(_hex(9, 10), F.lit("@example.com")))
        .drop("cid")
    )


def gen_orders(spark, n, n_customers):
    purchase = F.to_timestamp(
        F.from_unixtime(F.unix_timestamp(F.lit(START)) + _purchase_seconds(F.col("oid")))
    )
    hot = F.rand(SEED + 12) < 0.30
    cust = F.when(hot, F.lit("cust_00000001")).otherwise(
        F.concat(F.lit("cust_"), F.lpad((F.rand(SEED + 13) * n_customers + 1).cast("long").cast("string"), 8, "0"))
    )
    status = _pick(
        F.rand(SEED + 10),
        ["delivered", "delivered", "delivered", "shipped", "processing", "cancelled", "unavailable"],
    )
    return (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "oid")
        .withColumn("order_id", F.concat(F.lit("ord_"), F.lpad(F.col("oid").cast("string"), 9, "0")))
        .withColumn("customer_id", cust)
        .withColumn("order_status", status)
        .withColumn("order_purchase_timestamp", purchase)
        .withColumn(
            "order_approved_at",
            (F.col("order_purchase_timestamp").cast("long") + (F.rand(SEED + 14) * 172800).cast("long")).cast(
                "timestamp"
            ),
        )
        .withColumn(
            "order_delivered_timestamp",
            F.when(
                F.col("order_status") == "delivered",
                (
                    F.col("order_purchase_timestamp").cast("long")
                    + (F.rand(SEED + 15) * 1123200 + 172800).cast("long")
                ).cast("timestamp"),
            ),
        )
        .withColumn(
            "order_estimated_delivery_date",
            F.to_date((F.col("order_purchase_timestamp").cast("long") + 864000).cast("timestamp")),
        )
        .withColumn("order_channel", _pick(F.rand(SEED + 16), CHANNELS))
        .withColumn("device_type", _pick(F.rand(SEED + 17), DEVICES))
        .withColumn("coupon_code", F.when(F.rand(SEED + 18) < 0.2, F.upper(_hex(18, 10))))
        .withColumn("customer_note", _hex(19, 48))
        .drop("oid")
    )


def gen_order_items(spark, n, n_orders):
    order_num = (F.rand(SEED + 21) * n_orders + 1).cast("long")
    hot = F.rand(SEED + 22) < 0.35
    seller = F.when(hot, F.lit("seller_00001")).otherwise(
        F.concat(F.lit("seller_"), F.lpad((F.rand(SEED + 23) * 50000 + 1).cast("int").cast("string"), 5, "0"))
    )
    return (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "iid")
        .withColumn("order_num", order_num)
        .withColumn("order_id", F.concat(F.lit("ord_"), F.lpad(F.col("order_num").cast("string"), 9, "0")))
        .withColumn(
            "order_date",
            F.to_date(
                F.from_unixtime(F.unix_timestamp(F.lit(START)) + _purchase_seconds(F.col("order_num")))
            ),
        )
        .drop("order_num")
        .withColumn("order_item_id", (F.pmod(F.col("iid"), F.lit(5)) + 1).cast("int"))
        .withColumn(
            "product_id",
            F.concat(F.lit("prod_"), F.lpad((F.rand(SEED + 24) * 30000 + 1).cast("int").cast("string"), 5, "0")),
        )
        .withColumn("seller_id", seller)
        .withColumn("product_category", _pick(F.rand(SEED + 25), CATEGORIES))
        .withColumn("product_name", F.concat(_pick(F.rand(SEED + 25), CATEGORIES), F.lit(" "), _hex(26, 12)))
        .withColumn("product_weight_g", (F.rand(SEED + 27) * 30000 + 50).cast("int"))
        .withColumn("product_length_cm", (F.rand(SEED + 28) * 100 + 5).cast("int"))
        .withColumn("product_height_cm", (F.rand(SEED + 29) * 100 + 2).cast("int"))
        .withColumn("product_width_cm", (F.rand(SEED + 30) * 100 + 5).cast("int"))
        .withColumn("price", F.round(F.rand(SEED + 31) * 995 + 5, 2))
        .withColumn("shipping_charges", F.round(F.rand(SEED + 32) * 48 + 2, 2))
        .drop("iid")
    )


def gen_payments(spark, n, n_orders):
    order_ref = F.concat(
        F.lit("ord_"), F.lpad((F.rand(SEED + 41) * n_orders + 1).cast("long").cast("string"), 9, "0")
    )
    ptype = _pick(F.rand(SEED + 42), PAYMENT_TYPES)
    return (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "pid")
        .withColumn("order_id", order_ref)
        .withColumn("payment_sequential", (F.pmod(F.col("pid"), F.lit(3)) + 1).cast("int"))
        .withColumn("payment_type", ptype)
        .withColumn(
            "payment_installments",
            F.when(F.col("payment_type") == "credit_card", (F.rand(SEED + 43) * 12 + 1).cast("int")).otherwise(
                F.lit(1)
            ),
        )
        .withColumn("payment_value", F.round(F.rand(SEED + 44) * 490 + 10, 2))
        .withColumn("payment_provider", _pick(F.rand(SEED + 45), PROVIDERS))
        .withColumn(
            "card_brand",
            F.when(F.col("payment_type") == "credit_card", _pick(F.rand(SEED + 46), CARD_BRANDS)),
        )
        .withColumn("authorization_code", F.upper(_hex(47, 16)))
        .drop("pid")
    )


log = get_logger(__name__)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", default="large", choices=list(SCALES))
    p.add_argument("--output-dir", default="s3a://bronze")
    args = p.parse_args()
    s = SCALES[args.scale]

    spark = build_spark("generate-data")

    def write(df, name):
        path = f"{args.output_dir}/{name}"
        df.write.mode("overwrite").parquet(path)
        log.info("%s -> %s", name, path)

    log.info("Generating '%s' scale...", args.scale)
    write(gen_customers(spark, s["customers"]), "customers")
    write(gen_orders(spark, s["orders"], s["customers"]), "orders")
    write(gen_order_items(spark, s["items"], s["orders"]), "order_items")
    write(gen_payments(spark, s["payments"], s["orders"]), "payments")
    log.info("Done.")
    spark.stop()


if __name__ == "__main__":
    main()

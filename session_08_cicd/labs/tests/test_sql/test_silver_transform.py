def _run(con, silver_sql, ds):
    stmts = [s.strip() for s in silver_sql.split(";") if s.strip()]
    for stmt in stmts:
        rendered = stmt.replace("{{ ds }}", ds)
        con.execute(rendered)
    return con.execute("SELECT * FROM silver.orders WHERE order_date = ?", [ds]).df()


def test_silver_happy_case(con):
    # TODO: run _run(conn, sql, "2026-01-15"), assert len == 3
    # and set(["order_id", "customer_id", "amount", "order_date"]).issubset(result.columns)
    raise NotImplementedError


def test_silver_filters_null_customer(con):
    # Bug3: INSERT has no AND customer_id IS NOT NULL — nulls reach silver
    # TODO: assert result["customer_id"].isna().sum() == 0
    raise NotImplementedError


def test_silver_rejects_negative_amount(con):
    # Bug4: INSERT has no AND amount > 0 — negative amounts reach silver
    # TODO: assert (result["amount"] < 0).sum() == 0
    raise NotImplementedError


def test_silver_date_partition_isolation(con):
    # TODO: assert no rows with order_date == "2026-01-16" leak into ds="2026-01-15" result
    raise NotImplementedError


def test_silver_empty_input_no_crash(con):
    # TODO: run _run(conn, sql, "1900-01-01"), assert len == 0 and "amount" in columns
    raise NotImplementedError

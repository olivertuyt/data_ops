def _run(con, silver_sql, ds):
    stmts = [s.strip() for s in silver_sql.split(";") if s.strip()]
    for stmt in stmts:
        rendered = stmt.replace("{{ ds }}", ds)
        con.execute(rendered)
    return con.execute("SELECT * FROM silver.orders WHERE order_date = ?", [ds]).df()


def test_silver_happy_case(con):
    conn, sql = con
    result = _run(conn, sql, "2026-01-15")
    assert len(result) == 3
    assert set(result.columns) == {
        "order_id",
        "customer_id",
        "customer_name",
        "amount",
        "order_date",
    }


def test_silver_filters_null_customer(con):
    conn, sql = con
    result = _run(conn, sql, "2026-01-15")
    assert result["customer_id"].isna().sum() == 0


def test_silver_rejects_negative_amount(con):
    conn, sql = con
    result = _run(conn, sql, "2026-01-15")
    assert (result["amount"] < 0).sum() == 0


def test_silver_date_partition_isolation(con):
    conn, sql = con
    result = _run(conn, sql, "2026-01-15")
    assert (result["order_date"].astype(str) != "2026-01-16").all()


def test_silver_empty_input_no_crash(con):
    conn, sql = con
    result = _run(conn, sql, "1900-01-01")
    assert len(result) == 0
    assert "amount" in result.columns

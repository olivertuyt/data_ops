# Session 08 Labs — CI/CD for Data Pipeline

> Run all commands from the `session_08_cicd/` folder unless stated otherwise.

## Prerequisites

- Python 3.11+
- Java 8 or 11 — required for Spark tests. Check with `java -version`. Java 17+ will fail.
  If your default is newer, set `JAVA_HOME` before running Spark tests:
  ```bash
  # macOS — Java 8
  JAVA_HOME=$(/usr/libexec/java_home -v 1.8) pytest labs/tests/test_spark/ -v
  # macOS — Java 11
  JAVA_HOME=$(/usr/libexec/java_home -v 11) pytest labs/tests/test_spark/ -v
  ```
- Install dependencies (run once from `session_08_cicd/`):
  ```bash
  pip install -r requirements-dev.txt
  ```
- **GitHub CLI (`gh`)** — used in Lab 1 to verify CI runs from the terminal.

  **macOS:**
  ```bash
  brew install gh
  gh auth login   # select GitHub.com → HTTPS → Login with browser
  ```

  **Windows:**
  ```powershell
  winget install --id GitHub.cli
  gh auth login   # select GitHub.com → HTTPS → Login with browser
  ```

  If you prefer not to install `gh`, you can verify CI runs in the browser instead — see the
  **Verify** step in Lab 1 below.

---

## Lab 1 — Add CI to your repository (20 min)

Set up the same CI workflow you saw in the demo on **your own GitHub repository**.

> **Fork first:** Labs 1 and 3 require your own GitHub repo (to push branches, view Actions, and
> install a self-hosted runner). Fork this repo to your personal GitHub account, then clone your
> fork and work from there:
> ```bash
> # After forking on GitHub:
> git clone https://github.com/<your-username>/master-class-dataops.git
> cd master-class-dataops/session_08_cicd
> ```

**What to do:**

1. Open `session_08_cicd/demo/README.md` — follow **Demo 1** step by step
2. Create `pyproject.toml` and `requirements-dev.txt` in your repo root
3. Create `.github/workflows/ci.yml` with three jobs: Lint → Security → Test
4. Push to GitHub and open the **Actions** tab — all three jobs must go green

**Verify — option A: terminal (`gh` CLI):**

```bash
gh run list --workflow=ci.yml --limit=3
```

Expected output:
```
STATUS   NAME   WORKFLOW    BRANCH   EVENT  ID          ELAPSED  AGE
✓        ci     DataOps CI  feat/..  push   12345678    1m23s    2m ago
```

**Verify — option B: GitHub UI:**

1. Open your fork on GitHub → click the **Actions** tab
2. In the left sidebar select **DataOps CI**
3. Click the latest run — the status icon must be a green ✓
4. Open the **Unit test + coverage** step → scroll to the `TOTAL` line to read coverage %

**Acceptance checklist**

- [ ] GitHub Actions tab shows a green CI run on your branch
- [ ] The `Unit test + coverage` step log shows a `TOTAL` coverage line ≥ 70%

---

## Lab 2 — Fix the broken pipeline

A colleague submitted the orders pipeline for code review. Three files in `labs/jobs/` have bugs.
Your job: write a test that catches each bug, then fix the code until all tests pass.

**Files to fix:**

| File | Bugs | Test stub |
|---|---|---|
| `labs/jobs/dag_order_pipeline.py` | Bug1 (catchup), Bug2 (wrong task order) | `labs/tests/test_dags/test_order_pipeline.py` |
| `labs/jobs/bronze2silver__orders.sql` | Bug2 (customer_name), Bug3 (NULL customer_id), Bug4 (negative amount) | `labs/tests/test_sql/test_silver_transform.py` |
| `labs/jobs/spark_transform.py` | Bug1 (non-deterministic timestamp), Bug3 (NULL customer_id) | `labs/tests/test_spark/test_spark_transform.py` |

Each file in `labs/jobs/` contains `# TODO BugN:` comments on the exact lines with bugs —
read them first to understand what's wrong before writing any test.

> This lab is test-driven: only `bronze2silver__orders.sql` is provided. The DAG references
> `raw2bronze`/`silver2gold`/`reconcile` SQL that is not shipped here, so you diagnose and fix
> from the failing tests — you do not run the full pipeline for this lab.

---

### Step 1 — Confirm all stubs are failing

Before writing anything, run the three test groups. Every test should raise `NotImplementedError`:

```bash
pytest labs/tests/test_dags/ -v
pytest labs/tests/test_sql/  -v
pytest labs/tests/test_spark/ -v   # requires Java 8 or 11 — see Prerequisites above
```

Expected: `ERROR` on every test (NotImplementedError, not assertion failures yet).

---

### Step 2 — DAG structure bugs (`dag_order_pipeline.py`)

Open `labs/tests/test_dags/test_order_pipeline.py`. Implement each stub:

| Stub | What to assert |
|---|---|
| `test_no_catchup` | `dag.catchup is False` |
| `test_task_dependency_order` | `load_gold` is upstream of `reconcile`; `silver_transform` is upstream of `load_gold` |

```bash
pytest labs/tests/test_dags/ -v
```

**Red → fix `dag_order_pipeline.py` → Green.**

---

### Step 3 — SQL transform bugs (`bronze2silver__orders.sql`)

Open `labs/tests/test_sql/test_silver_transform.py`. Implement each stub:

| Stub | What to assert |
|---|---|
| `test_silver_filters_null_customer` | no row in result has `customer_id IS NULL` |
| `test_silver_rejects_negative_amount` | no row in result has `amount < 0` |

```bash
pytest labs/tests/test_sql/ -v
```

**Red → add the missing `WHERE` clauses in `bronze2silver__orders.sql` → Green.**

---

### Step 4 — Spark transform bugs (`spark_transform.py`)

Open `labs/tests/test_spark/test_spark_transform.py`. Implement each stub:

| Stub | What to assert |
|---|---|
| `test_processed_at_is_deterministic` | calling `transform()` twice on the same input produces the same `processed_at` |
| `test_null_customer_id_handled` | no row with `customer_id IS NULL` in the output |

```bash
pytest labs/tests/test_spark/ -v
```

**Red → fix `spark_transform.py` → Green.**

Hint for Bug1: `F.current_timestamp()` changes every call. Replace it with a value derived from `ds`.

---

### Step 5 — Full run with coverage

```bash
pytest labs/tests/ --cov=labs/jobs --cov-report=term-missing --cov-fail-under=70
```

**Done when:**

```
====== N passed in X.XXs ======
TOTAL    ...    70%+
```

**Acceptance checklist**

- [ ] All `NotImplementedError` stubs replaced with real assertions
- [ ] All tests pass
- [ ] Coverage ≥ 70%
- [ ] No `print()` in any fixed file

---

## Lab 3 — Auto-deploy the DAG on merge

Right now, deploying a DAG change requires someone to SSH into the server and run `git pull` manually.
Your team wants this to happen automatically every time a change merges to `main`.

**Before you start — one-time setup:**

1. GitHub repo → Settings → Actions → Runners → New self-hosted runner → follow the install steps for your OS
2. Start the runner and **keep that terminal open** throughout this lab:
   ```bash
   ./run.sh
   ```
3. GitHub repo → Settings → Variables → New repository variable:
   - Name: `REPO_PATH`
   - Value: absolute path to your local repo (e.g. `/Users/you/projects/master-class-dataops`)

**What to do:**

1. Open `session_08_cicd/demo/README.md` — follow **Demo 3** to create `.github/workflows/cd.yml`
2. Wire the workflow so that:
   - CD only starts after the `DataOps CI` workflow **succeeds**
   - The runner runs `git fetch` + `git checkout` to update the DAG files locally
   - The last step calls the Airflow API to confirm the DAG is active
3. Make a small change to `labs/jobs/dag_order_pipeline.py` (e.g. update the `description`), open a PR, and merge it
4. Watch the **Actions** tab — CD should trigger automatically

**Verify the deploy worked:**

```bash
curl -s -u "airflow:airflow" http://localhost:8888/api/v1/dags/session_08_order_pipeline \
  | python3 -c "import sys, json; d=json.load(sys.stdin); print('active:', d['is_active'])"
```

Expected: `active: True`

**Acceptance checklist**

- [ ] In the Actions tab, the CD run shows CI completed (green) before CD started
- [ ] No manual `git pull` was needed — the DAG updated on its own
- [ ] `curl` above returns `active: True` after the merge

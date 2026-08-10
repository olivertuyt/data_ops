# Session 2 — Introduction to Airflow: Architecture & Deployment

**Master Class DataOps for Modern Data Platforms · Session 2/11**

In session 1 we talked about the DataOps mindset: a pipeline doesn't just need to *run* — it needs to be *operable*: observable, diagnosable, rerunnable, trustworthy. This session introduces the tool at the center of the whole course: **Apache Airflow**.

Most people start orchestrating pipelines with cron: one crontab line calls one script, a new pipeline means a new line. Then one day you have 30 jobs, job B must run after job A, job C fails at 3 AM and nobody notices, and answering "did job X run yesterday?" takes 15 minutes of grepping logs. That's the moment you need an **orchestrator** — and Airflow is the most widely used one in today's data ecosystem.

This session answers three questions: **what** Airflow is (and is not), **what components** it is made of, and **how** it can be deployed. By the end of it you will have a stable Airflow stack running on your own machine — and this is not a throwaway exercise: the same stack carries you through the rest of the course, so build it properly.

## What you will learn

- **Airflow's role in DataOps**: it is an orchestrator — it decides *when* to run *what*, in *which order*. It does NOT process, transform, or store data. Confusing this is the root cause of many hard-to-operate pipelines (heavy transform logic stuffed into Airflow workers).
- **Airflow architecture**: Webserver, Scheduler, Executor, Worker, Metadata DB, Triggerer — what each one does and what breaks when it dies. Plus Flower for worker/queue monitoring.
- **The three executors** (Local / Celery / Kubernetes): how they differ and when to use which. This course uses the **Celery Executor** because it scales horizontally and matches how production teams actually run Airflow.
- **The three deployment options** (Docker Compose / bare VM / Kubernetes + Helm) and their trade-offs — there is no "best" choice, only the one that fits your team's scale.
- **High Availability**: why a single-node setup has a single point of failure, and what it takes to achieve HA. You will see this firsthand when you stop the scheduler on your own stack.
- **GitOps for Airflow**: Git as the source of truth, CI/CD syncing DAGs into the runtime — the foundation for Reproducibility, which we will come back to repeatedly in this course.

## Directory structure

```
session_02_airflow_intro/
├── README.md            # this file — session overview
├── lab/                 # YOUR hands-on part: Airflow on Docker Compose
│   ├── README.md        # step-by-step setup guide for the stack
│   ├── docker-compose.yaml
│   ├── .env.example
│   └── dags/            # hello_world + parallel_workload (demo DAGs for exploring the UI)
└── demo/
    └── k8s_helm/        # demo: Airflow on Minikube via Helm Chart.
        ├── README.md    # setup guide for the demo stack + troubleshooting
        ├── values-demo.yaml
        └── scripts/
```

## Where to start

1. **Hands-on** — follow [lab/README.md](lab/README.md) to bring up the Airflow stack with Docker Compose and verify it is healthy. Once it runs, spend time in the UI: trigger the `hello_world` DAG, read task logs, open Flower, and try to map every running container to a component from the architecture slides. The goal of this session is not typing `docker compose up` — it is being able to look at a running stack and point at what each piece is doing.
2. **Watch the demo** — the mentor will run this exact Airflow version on Kubernetes via Helm ([demo/k8s_helm/README.md](demo/k8s_helm/README.md)). What to take away: the **architecture doesn't change** — same scheduler, workers, and metadata DB, just a different deployment shell; and the things Compose can't do (self-healing, declarative scaling) are exactly what K8s adds. You don't need to install Minikube.

If your stack works, **keep it** (`docker compose stop` is enough — don't `down -v`): in session 3 we write DAGs on this very stack.

## Position in the course

| | |
|---|---|
| Previous | Session 1 — DataOps mindset & operating principles |
| This session | Build and understand the Airflow architecture (the 6 pipeline properties come later) |
| Next | Session 3 — Writing DAGs & advanced concepts: idempotency, backfill, retry, XCom |

Note: this session ships two demo DAGs, used only to explore the architecture — you don't write DAGs here (that's session 3):
- `hello_world` — a minimal DAG to see task structure, logs, and the executor.
- `parallel_workload` — a fan-out of slow tasks; trigger it and open Flower to watch the Celery Executor distribute the tasks across workers (great for the scale-worker and stop-worker demos).

## Check yourself before you leave

If you can answer these four questions fluently, you've got session 2:

1. What should Airflow *not* do, and why is pushing heavy transforms into Airflow an anti-pattern?
2. Which components does a task pass through on its way from "it's time to run" to "done" — and in what order?
3. What happens when the scheduler dies? When a worker dies? How do the two failures differ?
4. How should a 3-person team just starting out deploy Airflow? What about a 50-person team with 500 DAGs?

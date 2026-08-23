# AWS Deployment Design

**Date:** 2026-08-23
**Status:** Approved for planning

## Goal

Deploy a cheap, publicly reachable demo of the AI Analytics Agent project to AWS
(portfolio use), protected from unauthorized use, while keeping local Ollama-based
development untouched.

## Constraints

- Budget: minimal. New AWS account gets ~$100-200 in free-tier/promotional credits,
  which covers the initial runway, but the design should not assume the credits
  are permanent.
- No existing AWS account — account creation and IAM setup are part of the work.
- No custom domain / HTTPS required — plain HTTP over a static IP is acceptable.
- Terraform is the IaC tool of choice. The user is new to both AWS and Terraform
  and needs to be walked through each step.
- Must not allow unauthenticated/unbounded use of the LLM-backed agent endpoint —
  this is a demo, not a public product, and LLM calls cost money per request.

## Components in scope for the demo

All four must be live and reachable:

- UI (React/Vite frontend)
- AI Analytics Agent API (FastAPI `/ask`)
- Forecasting API (FastAPI)
- Superset (BI dashboards)

dbt runs, raw data ingestion, and model training are one-off/batch — they populate
the database once after the instance is up, they are not long-running services.

## Decisions

### 1. LLM provider: dual — Ollama (dev) / Bedrock (prod)

- Local development keeps using Ollama exactly as today — no regression to the
  dev workflow.
- The AWS deployment uses **Amazon Bedrock** (Claude Haiku) instead of a local
  model or a direct Anthropic API key.
  - **Correction of an earlier assumption:** Bedrock does not have a standing
    free tier for model inference — it bills per token, in the same range as
    the direct Anthropic API. The reason to use it here is not cost but that
    the EC2 instance can call it via an **IAM Role**, with no API key to
    generate, store, or accidentally leak via `scp`/git.
  - Bedrock model access must be enabled once in the AWS Console before first
    use (Model Access screen, one-time per account/region).
- `ai_analytics_agent/llm/` is split into per-provider modules
  (`providers/ollama_provider.py`, `providers/bedrock_provider.py`), selected at
  runtime via an `LLM_PROVIDER=ollama|bedrock` environment variable.
  - The Bedrock provider uses `anthropic.AnthropicBedrockMantle`, which exposes
    the same `messages.create(...)` interface as the standard Anthropic SDK.
  - `agent_loop.py` stays provider-agnostic: each provider module normalizes its
    response into one shared internal shape (text content + a list of tool
    calls), so the surrounding loop, tool dispatch, and chart-candidate logic
    are unchanged regardless of provider.
  - Tool schemas differ per provider (Ollama expects OpenAI-style
    `{"type": "function", "function": {...}}`; Bedrock/Anthropic expects flat
    `{"name", "description", "input_schema"}`) — each provider module owns its
    own schema builder derived from the same semantic-layer YAML configs.

### 2. Containerization

No Dockerfiles exist yet for the app services (only Postgres/Superset use
off-the-shelf images). Adding:

- `ai_analytics_agent/Dockerfile` — FastAPI + uvicorn
- `forecasting/Dockerfile` — FastAPI + uvicorn
- `ui/Dockerfile` — multi-stage: `npm run build`, served as static files by nginx

`docker-compose.yml` is extended with these three services alongside the existing
`postgres` and `superset` services.

### 3. Single entry point: nginx (auth + rate limiting + routing)

One nginx service is the only container bound to a public interface. Every app
service (UI, agent API, forecast API, Superset) is bound to the docker-internal
network only — not directly reachable from the internet.

nginx listens on four ports and reverse-proxies to the corresponding internal
service:

| Port | Proxies to |
|------|------------|
| 80   | UI static files |
| 8001 | Agent API |
| 8002 | Forecasting API |
| 8088 | Superset |

nginx applies, on every port:

- **HTTP Basic Auth** — one username/password pair per invited person (an
  `htpasswd` file, not committed to git).
- **Rate limiting** (`limit_req`) — caps requests per IP per minute, so even a
  legitimate credential holder can't accidentally (or deliberately) hammer the
  Bedrock-backed agent endpoint into a large bill.

No additional quota/throttling is implemented inside the FastAPI agent itself —
the nginx layer is judged sufficient for a demo with a handful of invited users.

### 4. AWS infrastructure (Terraform)

Kept intentionally minimal — one EC2 instance rather than a fleet of managed
services, to limit the number of new AWS concepts for a first deployment:

- **Default VPC** — no custom VPC/subnets/route tables.
- **EC2 instance**, `t3.small` (2 GiB RAM — needed to run Postgres ×2, Superset,
  two FastAPI services, and nginx concurrently), Ubuntu AMI, `user_data`
  installs Docker + Docker Compose on first boot.
- **IAM Role** attached to the instance, scoped to `bedrock:InvokeModel` on the
  specific Claude Haiku model ARN only, in the deployment region only.
- **Security Group**: inbound 22 (SSH, restricted to the operator's IP), 80,
  8001, 8002, 8088 (all four go to nginx only, per above); egress open.
- **Elastic IP**, so the address is stable across instance stop/start/reboot.
- **AWS Budget** with an email alert at a low threshold (e.g. $10/$20), as a
  safety net independent of the request-level protections above.

Explicitly out of scope for this deployment: RDS, ECS/Fargate, Secrets Manager,
HTTPS/custom domain, autoscaling, CI/CD. These are reasonable future upgrades,
not needed for a portfolio demo.

### 5. Secrets and first-run data load

- `.env.production` (gitignored): DB passwords, Superset secret key, nginx
  `htpasswd` credentials. No LLM API key is needed for the Bedrock path — auth
  is via the instance's IAM role.
- Deployment flow: `terraform apply` provisions the instance → operator
  `scp`/`rsync`s the repo and `.env.production` to the instance → SSH in and
  run `docker compose up -d --build`.
- The data pipeline (`make run`: start DB, load raw data, run dbt, sync
  metadata) is run once by hand over SSH after the stack is up, the same way it
  runs locally today.

### 6. Cost expectation

Initial AWS promotional/free-tier credits (~$100-200 for a new account) cover
the first stretch of usage. Ongoing steady-state cost is dominated by the
`t3.small` instance (~$15-20/month if left running continuously); Bedrock usage
at demo traffic volumes is expected to be a few dollars/month at most. The
instance can be stopped between demo sessions (`aws ec2 stop-instances` or
`terraform destroy` for a full teardown) to avoid 24/7 billing if desired.

## Testing

- Local: existing test suites for `ai_analytics_agent` and `forecasting` must
  keep passing with the Ollama provider; new tests cover the Bedrock provider's
  request/response normalization (mocking `AnthropicBedrockMantle`) and the
  provider-selection logic.
- Deployed: manual smoke test after each `docker compose up` — load the UI,
  ask the agent a question end-to-end, open a Superset dashboard, hit the
  forecasting API — all through nginx with Basic Auth, confirming unauthenticated
  requests are rejected and requests over the rate limit are throttled.

## Open items for the implementation plan

- Exact Terraform module layout and the step-by-step account setup walkthrough
  (account creation, root MFA, billing alarm, IAM user for Terraform, local
  Terraform/AWS CLI install) are sequencing details for `writing-plans`, not
  design decisions.

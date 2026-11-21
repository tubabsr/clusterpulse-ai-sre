# Kubernetes AI Monitoring & RCA Agent (starter MVP)

An in-cluster, read-only Kubernetes watcher that collects pod status, restart information,
previous/current container logs and warning events; creates incident records; asks a
configurable OpenAI-compatible chat-completions endpoint for evidence-based RCA and
exactly five Whys; and sends reports through configurable SMTP.

## Scope and limitations of this starter

This is an executable foundation, not yet a fully production-certified SRE platform.
The watcher covers pods and Warning events across namespaces. The dashboard also
queries live pod, node and workload inventory, supports on-demand current/previous
container logs, and queries Prometheus CPU/memory metrics. Robust incident correlation,
retry queues, multi-replica leader election, and Helm packaging remain follow-on work.
Email delivery uses SMTP with STARTTLS or implicit TLS.

The agent does not perform remediation. Review all AI recommendations before executing.

## Repository

- `app/monitor.py`: Kubernetes watches, evidence gathering, incident creation
- `app/ai.py`: configurable LLM analysis adapter
- `app/emailer.py`: SMTP email delivery
- `app/db.py`: PostgreSQL incident persistence
- `app/main.py`: FastAPI health and incident endpoints
- `k8s/`: read-only RBAC and deployment manifests

## Requirements

- Kubernetes 1.24+ and a namespace named `clusterpulse`
- PostgreSQL reachable from the agent (`k8s/postgres.yaml` deploys a dedicated one in the same namespace)
- A default StorageClass (k3s ships `local-path`) for the PostgreSQL PVC
- Container registry reachable by cluster nodes
- Egress to configured AI endpoint and the organization SMTP relay
- SMTP relay credentials and an approved sender mailbox
- AI endpoint supporting OpenAI-compatible `/chat/completions` with JSON response mode

For Azure OpenAI, set `AI_BASE_URL` to the resource deployment endpoint compatible
with chat completions and set `AI_MODEL` to the deployment name. Provider-specific
API-version/auth handling may need a small adapter change for your Azure configuration.
For Ollama, use a compatible endpoint such as `http://ollama:11434/v1`.

## Build

```bash
docker build -t YOUR_REGISTRY/k8s-ai-rca-agent:0.1.0 .
docker push YOUR_REGISTRY/k8s-ai-rca-agent:0.1.0
```

## Configure and deploy (from scratch)

Everything runs in the `clusterpulse` namespace, including its own PostgreSQL,
and is served at `https://clusterpulse.local`.

### Prerequisites

On the machine you deploy from (a server or workstation with cluster access):

- `kubectl` configured for the target cluster (`kubectl get nodes` works)
- `git`, `openssl`, `jq`, and `htpasswd` (package `apache2-utils` on Debian/Ubuntu,
  `httpd-tools` on RHEL/CentOS)
- Traefik with CRDs installed (k3s includes it) and a default StorageClass
- The container image `ghcr.io/<owner>/<repo>:latest` must be pullable by the nodes.
  If the GHCR package is private, create an image pull Secret (see "Important
  deployment notes" below).

### Clone the repository

```bash
git clone https://github.com/singhgauravlinux-coder/clusterpulse-ai-sre.git
cd clusterpulse-ai-sre
git pull            # later, to pick up newer manifests and the image-tag commit
```

If the repository is private, authenticate first (`gh auth login`, an SSH key, or a
personal access token) and use the matching clone URL. Run every command below from
this directory (the repository root). Before starting, edit `k8s/configmap.yaml`
(`SMTP_FROM`, `EMAIL_RECIPIENTS`, `PROMETHEUS_URL`, AI endpoint and model).

### 0. Clean namespace

```bash
kubectl delete ns clusterpulse --wait=true   # only if a previous attempt exists
kubectl create ns clusterpulse
```

### 1. PostgreSQL

```bash
PGPASS=$(openssl rand -hex 16)               # keep this shell open until step 2
kubectl -n clusterpulse create secret generic clusterpulse-postgres-auth \
  --from-literal=POSTGRES_USER=clusterpulse \
  --from-literal=POSTGRES_PASSWORD=$PGPASS \
  --from-literal=POSTGRES_DB=clusterpulse

kubectl apply -f k8s/postgres.yaml
kubectl -n clusterpulse rollout status sts/clusterpulse-postgres
kubectl -n clusterpulse get pvc              # data-clusterpulse-postgres-0 must be Bound
```

`k8s/postgres.yaml` creates a headless Service `clusterpulse-postgres` and a
StatefulSet whose `volumeClaimTemplates` provisions the PVC
`data-clusterpulse-postgres-0` (5Gi, default StorageClass) automatically.
The application creates its tables on first start (`init_db()`), so the database
only needs to exist. Deleting the StatefulSet keeps the PVC and your data; delete
the PVC explicitly to start with an empty database.

To use an external or managed PostgreSQL instead, skip `postgres.yaml` and put
that connection string in `DATABASE_URL` below.

### 2. Get the AI key and SMTP password, then create the secrets

#### 2a. Gemini API key (`AI_API_KEY`)

The default `k8s/configmap.yaml` uses Google's OpenAI-compatible Gemini endpoint
(`AI_BASE_URL`) with the model in `AI_MODEL`.

1. Open Google AI Studio: <https://aistudio.google.com/apikey> and sign in with a Google account.
2. Accept the terms. New users get a default project and key; otherwise click
   **Create API key** and pick (or create) a Google Cloud project.
3. Copy the key (it starts with `AIza...`). Treat it like a password.
4. Recommended: on the same page, mark the key **Restrict to Gemini API only**.
5. Check that the key works and that your `AI_MODEL` exists. Read the key with
   `read -s` so it is not stored in shell history:

```bash
read -rs GEMINI_KEY; echo
curl -s https://generativelanguage.googleapis.com/v1beta/openai/models \
  -H "Authorization: Bearer $GEMINI_KEY" | head -c 600
```

A JSON list of models means the key is valid. `API key not valid` means it was
copied wrongly. If your `AI_MODEL` is not in the list, set one from the list in
`k8s/configmap.yaml`. Free-tier keys have rate limits; the agent retries failed
RCA requests automatically (`rca_max_attempts`).

To use another provider, change `AI_BASE_URL`, `AI_MODEL` and `AI_API_KEY`
(any OpenAI-compatible `/chat/completions` endpoint, for example an in-house model).

#### 2b. SMTP username and password (`SMTP_USERNAME`, `SMTP_PASSWORD`)

The default ConfigMap uses Gmail (`smtp.gmail.com`, port 587, STARTTLS). Gmail
does not accept your normal password for SMTP; you need an **app password**:

1. Turn on 2-Step Verification: Google Account > Security > 2-Step Verification.
   The app password page does not appear without it.
2. Open <https://myaccount.google.com/apppasswords>, enter a name such as
   `clusterpulse`, and click **Create**.
3. Copy the 16-character password. It is shown once; generate a new one if lost.
   Enter it without spaces.
4. Use these values:
   - `SMTP_USERNAME` = the full Gmail address
   - `SMTP_PASSWORD` = the 16-character app password
   - `SMTP_FROM` (ConfigMap) = the same Gmail address, because Gmail rewrites other senders
   - `EMAIL_RECIPIENTS` (ConfigMap) = who receives alerts, comma separated

Notes:
- Changing your Google account password revokes all app passwords; create a new one
  and update the secret.
- The app passwords page is unavailable for Workspace/work or school accounts that
  disallow it, for accounts using only security keys, and for Advanced Protection.
  In that case use your organization's SMTP relay: set `SMTP_HOST`, `SMTP_PORT`,
  `SMTP_STARTTLS`/`SMTP_USE_SSL` in the ConfigMap and the relay's credentials
  (or none, if the relay allows your cluster's IP) in the secret.
- Quick test before storing it (prompts for the password, sends nothing):

```bash
python3 - <<'PY'
import smtplib, getpass
user = input("SMTP user: ")
pw = getpass.getpass("App password: ")
s = smtplib.SMTP("smtp.gmail.com", 587, timeout=15); s.starttls(); s.login(user, pw)
print("SMTP login OK"); s.quit()
PY
```

#### 2c. Create the ConfigMap and secret

```bash
kubectl apply -f k8s/configmap.yaml

read -rs SMTP_PW; echo                      # paste the app password, no spaces
kubectl -n clusterpulse create secret generic k8s-ai-rca-secrets \
  --from-literal=DATABASE_URL="postgresql+psycopg://clusterpulse:${PGPASS}@clusterpulse-postgres:5432/clusterpulse" \
  --from-literal=AI_API_KEY="$GEMINI_KEY" \
  --from-literal=SMTP_USERNAME='you@gmail.com' \
  --from-literal=SMTP_PASSWORD="$SMTP_PW"
unset GEMINI_KEY SMTP_PW
```

If you open a new shell between steps, re-enter `PGPASS` and `GEMINI_KEY` first;
the database password must be the same one used in step 1. Never commit a populated
secret. See `k8s/secret-template.yaml` and `k8s/config-secret-template.yaml` for other
supported keys. In production use your enterprise secret manager.

To change a value later, patch the secret and restart the pod:

```bash
kubectl -n clusterpulse patch secret k8s-ai-rca-secrets --type merge \
  -p "$(jq -n --arg v "$NEW_VALUE" '{stringData:{AI_API_KEY:$v}}')"
kubectl -n clusterpulse rollout restart deploy/k8s-ai-rca-agent
```

### 3. TLS certificate and Basic Auth login

A self-signed certificate is enough for `clusterpulse.local` (browsers warn once;
use an internal CA certificate for managed devices):

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 825 \
  -keyout /tmp/tls.key -out /tmp/tls.crt \
  -subj "/CN=clusterpulse.local" -addext "subjectAltName=DNS:clusterpulse.local"
kubectl -n clusterpulse create secret tls clusterpulse-tls --cert=/tmp/tls.crt --key=/tmp/tls.key
rm -f /tmp/tls.key /tmp/tls.crt

# htpasswd comes from apache2-utils / httpd-tools; it prompts for the password
umask 077; htpasswd -nB clusterpulseadmin > /tmp/users
kubectl -n clusterpulse create secret generic clusterpulse-users --from-file=users=/tmp/users
shred -u /tmp/users
```

Check `cat /tmp/users` before creating the Secret: it must contain one line like
`clusterpulseadmin:$2y$05$...`. An empty Secret makes Traefik log
`secret ... does not contain any credentials`, the middleware is not created, and
the HTTPS route answers `404`. Do not use `--from-literal` for the password and do
not commit the hash.

### 4. RBAC and application

```bash
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/deployment.yaml
kubectl -n clusterpulse rollout status deployment/k8s-ai-rca-agent
kubectl -n clusterpulse logs deployment/k8s-ai-rca-agent -f
```

The ClusterRole and ClusterRoleBinding are named `k8s-ai-rca-agent-clusterpulse`.
Cluster-scoped objects are shared by every namespace, so another installation of this
agent must not use the same names; otherwise applying one overwrites the other's
permissions and it starts logging `403 Forbidden`.

### 5. Routes (Traefik)

Requires Traefik with CRDs (`traefik.io/v1alpha1`) and entryPoints `web` and
`websecure`. The host is set in `k8s/traefik-ingressroute.yaml`
(`Host(`clusterpulse.local`)`). Two IngressRoutes in the cluster must not use the
same host, so check first: `kubectl get ingressroute -A | grep -i clusterpulse`.

```bash
kubectl apply -f k8s/traefik-middleware.yaml -f k8s/traefik-ingressroute.yaml
kubectl apply -f k8s/clusterpulse-networkpolicy.yaml   # optional; allows Traefik in kube-system or traefik
```

Resolve the name to your Traefik address on each workstation (or in DNS):

```
<TRAEFIK_NODE_OR_LB_IP>  clusterpulse.local
```

### 6. Verify

```bash
kubectl -n clusterpulse get pods
kubectl -n clusterpulse port-forward svc/k8s-ai-rca-agent 8080:8080 &
curl -s localhost:8080/readyz                                    # ready once PostgreSQL is reachable
curl -skI https://clusterpulse.local/                            # 401 without credentials
curl -sk -u clusterpulseadmin https://clusterpulse.local/readyz  # authenticated
kubectl auth can-i list pods --all-namespaces \
  --as=system:serviceaccount:clusterpulse:k8s-ai-rca-agent       # yes
```

Open `https://clusterpulse.local` and sign in. Keep the Service as ClusterIP and
do not expose it with NodePort or LoadBalancer.

### Running next to another installation

Two copies can run side by side if they have separate databases, different
`Host(...)` values, and distinct ClusterRole / ClusterRoleBinding names. Both copies
watch the same cluster, so both send alert emails unless you give one different
`EMAIL_RECIPIENTS` or scale it down.

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Pod `CrashLoopBackOff`, exit code 3, log `Name or service not known` | `DATABASE_URL` host does not resolve from this namespace. Use `clusterpulse-postgres` (same namespace) or `<svc>.<ns>.svc.cluster.local`. |
| Log `connection timed out` to Postgres | A NetworkPolicy blocks ingress to the database from `clusterpulse`. |
| Log `password authentication failed` | `DATABASE_URL` differs from `clusterpulse-postgres-auth`. The password is fixed at first start; if you recreate the secret, delete the PVC too. |
| Log `... is forbidden: User "system:serviceaccount:<ns>:k8s-ai-rca-agent" cannot list/watch ...` | ClusterRoleBinding subject points at another namespace or was overwritten. Inspect with `kubectl get clusterrolebinding <name> -o yaml` and re-apply `k8s/rbac.yaml` from this repo. |
| PVC `Pending` | No default StorageClass. Set `storageClassName` in `k8s/postgres.yaml`. With `local-path`, `Pending` until the pod is scheduled is normal. |
| Log `Incorrect authentication data` / `SMTPAuthenticationError` | Wrong `SMTP_USERNAME` or an app password with spaces or from a revoked session. Create a new app password and patch the secret. |
| Incidents show RCA `FAILED`/`PENDING` with HTTP 400/401/403/429 | Invalid or restricted `AI_API_KEY`, wrong `AI_MODEL`, or rate limit. Re-run the `curl .../models` check in 2a. |
| `/readyz` returns 503 | PostgreSQL unreachable. The pod is withheld from traffic, not restarted. |
| Browser cannot reach `clusterpulse.local` | Missing hosts or DNS entry, or the host clashes with another IngressRoute. |
| `404` from Traefik and Traefik log `does not contain any credentials` | `clusterpulse-users` is empty. Regenerate with `htpasswd -nB` (not `-cBn`) and recreate the Secret. |
| `404` from Traefik | Host does not match `k8s/traefik-ingressroute.yaml`, or the IngressRoute is in another namespace than the Service. |

## API

Port-forward:

```bash
kubectl -n clusterpulse port-forward svc/k8s-ai-rca-agent 8080:8080
```

- `GET /healthz`
- `GET /readyz`
- `GET /incidents?limit=100`
- `GET /incidents/{incident_id}`
- `GET /api/pods` (optional `namespace`, `search`, `phase`, `limit` filters)
- `GET /api/logs?namespace=...&pod=...&container=...&tail=200&previous=false`
- `GET /api/topology` (optional `namespace`): namespace cards, inventory and relationship table
- `GET /api/topology/graph` (optional `namespace`): typed resource graph (`nodes`, `edges`, `revision`)

The starter API has no authentication. Do not expose it outside a trusted internal
network until authentication/authorization is added.

## SMTP email setup

Configure `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_FROM`,
`SMTP_STARTTLS`, `SMTP_USE_SSL`, `EMAIL_RECIPIENTS`, and optional `EMAIL_CC`.
Port 587 with STARTTLS is the default. For implicit TLS (commonly port 465), set
`SMTP_USE_SSL=true` and `SMTP_STARTTLS=false`. Store SMTP credentials in the
Kubernetes Secret / enterprise secret manager, not in the ConfigMap or Git.

## Important production hardening before broad rollout

- Add node, deployment, ReplicaSet, StatefulSet and DaemonSet watchers.
- Add Prometheus metrics collection (Metrics Server is not a replacement for historical
  Prometheus time series).
- Use a durable work queue (e.g. PostgreSQL outbox, RabbitMQ, or Redis Streams) so AI
  and email outages do not block watch processing.
- Implement incident lifecycle and cooldown/deduplication using stable workload-level
  fingerprints and time windows. This starter only deduplicates open incidents by a
  basic fingerprint.
- Add restart-rate windows; current restart trigger uses the observed cumulative count.
- Add leader election or separate watcher/worker deployments before scaling replicas.
- Add API authentication, audit logging, network policies, PDB, resource sizing and
  backup/restore.
- Redact secrets and sensitive data from logs before sending evidence to an external LLM.
- Validate the generated RCA and five-whys structure; do not auto-run generated commands.
- Add tests against a disposable Kubernetes cluster (kind/k3d) and mock SMTP/LLM APIs.

## Security

The ClusterRole grants only `get`, `list`, and `watch` on workload metadata, events,
nodes, namespaces and pod logs. No write verbs are granted. Container logs may contain
credentials or personal data; configure redaction and retention before production use.


## Phase 2 dashboard and ingress

The dashboard is served at `/` and refreshes cluster data every 30 seconds. It includes live pod monitoring, an on-demand container log viewer, and a Light / Dark / System theme selector.
It uses `/api/overview`, `/api/nodes`, `/api/workloads`, `/api/pods`, `/api/logs`, and `/api/metrics`. Configure `PROMETHEUS_URL` to enable CPU and memory query cards.
The metrics queries assume Prometheus has node-exporter metrics
(`node_cpu_seconds_total`, `node_memory_MemAvailable_bytes`, `node_memory_MemTotal_bytes`).

Deploy the dashboard ingress after installing an ingress controller and creating
the TLS secret:

```bash
kubectl -n clusterpulse create secret tls clusterpulse-tls \
  --cert=./tls.crt --key=./tls.key
kubectl apply -f k8s/traefik-ingressroute.yaml
```

Change `clusterpulse.example.internal` and `ingressClassName: nginx` to match
your internal DNS and ingress controller. Point the internal DNS record at the
ingress controller address. If TLS is terminated upstream, adapt/remove the TLS
section according to your security policy.

Dashboard routes:
- `/` dashboard
- `/api/overview` cluster KPIs
- `/api/nodes` node readiness and pressure
- `/api/workloads` deployments, StatefulSets, DaemonSets and ReplicaSets
- `/api/pods` pod inventory, readiness, restart counts and node placement
- `/api/logs?namespace=...&pod=...` current or previous container logs (read-only)
- `/api/metrics` Prometheus CPU and memory
- `/incidents` incident history API

The dashboard is intentionally unauthenticated in this starter. Expose it only
on a trusted internal network or put SSO/authentication and network restrictions
in front of the Ingress before production use. The current app RBAC is read-only.


# Production deployment: Traefik + Basic Auth + Prometheus

This deployment uses Traefik CRD `IngressRoute` and `Middleware` resources.
It assumes Traefik is already installed with the Kubernetes CRDs and entryPoints
named `web` and `websecure`. Verify your installation's CRD API group/version and
entryPoint names before applying.

## 1. Configure DNS and TLS

This repository uses `clusterpulse.local` by default (see "Configure and deploy" above). To use another name, change it in `k8s/traefik-ingressroute.yaml` and in the certificate subject, and point it to the internal address of your Traefik LoadBalancer/NodePort.

Create a TLS certificate trusted by your organization's client devices. For an
internal CA, distribute the CA certificate to managed browsers/workstations.

```bash
kubectl -n clusterpulse create secret tls clusterpulse-tls \
  --cert=./tls.crt --key=./tls.key
```

Update the hostname in `k8s/traefik-ingressroute.yaml` to your real internal name.

## 2. Create Basic Auth credentials (bcrypt)

Install `htpasswd` (provided by Apache httpd tools) on a trusted admin workstation.
Generate a bcrypt hash interactively; do not put a plaintext password in shell
history or source control.

```bash
htpasswd -nB clusterpulseadmin
```

The output is a single line:
`clusterpulseadmin:$2y$...`

Store that line in a file named `users` with restrictive local permissions:

```bash
umask 077
# Paste the htpasswd output into ./users using a secure editor.
kubectl -n clusterpulse create secret generic clusterpulse-users \
  --from-file=users=./users
shred -u ./users 2>/dev/null || rm -f ./users
```

Do not use `--from-literal` with a password. Do not commit the generated hash
or secret YAML to Git. Rotate credentials by recreating the Secret and reviewing
your secret-management process.

## 3. Install Prometheus (If not installed)

Use the Prometheus Community Helm chart. In an offline/restricted on-prem
environment, mirror all required images to your internal registry and configure
the chart image repositories before installation.

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
kubectl create namespace monitoring
```

Edit `k8s/prometheus-values.yaml` and set `storageClassName` to a real on-prem
StorageClass. Then install:

```bash
helm upgrade --install kps prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --values k8s/prometheus-values.yaml \
  --wait
```

Check components:

```bash
kubectl get pods -n monitoring
kubectl get svc -n monitoring
kubectl get servicemonitors -A
```

The configured in-cluster Prometheus URL is:
`http://kps-kube-prometheus-prometheus.monitoring.svc:9090`

Confirm it matches the actual Service created by your Helm release. The dashboard's
CPU and memory queries require node-exporter series:
`node_cpu_seconds_total`, `node_memory_MemAvailable_bytes`,
`node_memory_MemTotal_bytes`.

For an air-gapped cluster, mirror chart images and chart packages through your
approved artifact repository; Helm repository access from cluster nodes is not
required, but the admin workstation needs chart access or a mirrored chart.

## 4. Configure ClusterPulse

Update `k8s/configmap.yaml`:
- Set `PROMETHEUS_URL` to the verified in-cluster Prometheus Service.
- Set AI endpoint and model.
Provision `DATABASE_URL`, AI key, SMTP credentials and recipient addresses through your enterprise secret manager. The `k8s/secret-template.yaml`
is a placeholder only; never apply it unchanged and never commit a populated copy.

Deploy the database, secrets, RBAC and application as described in
"Configure and deploy (from scratch)" above. `k8s/postgres.yaml` provides a
single-replica PostgreSQL with a PVC; for production add backups
(for example a scheduled `pg_dump`), TLS where required, and manage credentials
outside Git.

## 5. Apply Traefik authentication and routes

```bash
kubectl apply -f k8s/traefik-middleware.yaml
kubectl apply -f k8s/traefik-ingressroute.yaml
kubectl get ingressroute,middleware -n clusterpulse
```

If Traefik is installed in a namespace other than `traefik`, adjust the optional
NetworkPolicy namespace selector before applying it. NetworkPolicy enforcement
depends on the cluster CNI.

## 6. Verify

```bash
kubectl get ingressroute -n clusterpulse
kubectl get secret clusterpulse-users clusterpulse-tls -n clusterpulse
kubectl logs -n clusterpulse deployment/k8s-ai-rca-agent --tail=100
```

From a workstation that trusts the TLS issuer:

```bash
curl -I https://clusterpulse.example.internal/
```

Expected behavior:
- Unauthenticated request returns HTTP 401.
- Authenticated browser session can load the dashboard.
- HTTP request redirects to HTTPS.
- Dashboard API requests are protected by the same BasicAuth middleware.

Do not bypass the Ingress by exposing the Service with NodePort or LoadBalancer.
Keep the ClusterPulse Service as ClusterIP.

## 7. Network and security controls

- Permit inbound HTTPS only from approved corporate/VPN subnets at the firewall.
- Restrict Traefik dashboard/API separately; never expose Traefik's admin API publicly.
- Use TLS certificates from your internal PKI and plan certificate renewal.
- Apply Kubernetes Secret encryption at rest and RBAC restrictions.
- Use a secret manager/External Secrets Operator for production credentials.
- Restrict egress to Kubernetes API, PostgreSQL, Prometheus, configured AI endpoint,
  Microsoft identity endpoints, and SMTP as needed.
- Add authentication rate limiting at the edge and monitor authentication failures.
- Basic Auth is a shared-secret mechanism; use individual credentials/SSO if audit
  attribution and centralized identity lifecycle are required.
- The optional `k8s/clusterpulse-networkpolicy.yaml` only restricts ingress to pods
  labeled as Traefik in namespace `traefik`. Verify actual labels and namespace first.
- The initial FastAPI health/readiness and incident endpoints are not independently
  authenticated. They are protected only when accessed through this Traefik route.
  Keep the ClusterIP service internal and do not add a second unauthenticated route.


## GitHub Actions: automatic image build and deployment manifest update

The workflow at `.github/workflows/build-image.yml` runs on pushes to `main` when
application code or image build inputs change. It:

1. Validates Python syntax.
2. Builds the Docker image with Buildx.
3. Publishes `ghcr.io/<owner>/<repository>:latest` and
   `ghcr.io/<owner>/<repository>:sha-<commit>`.
4. Updates the image reference in `k8s/deployment.yaml` to the repository's
   `:latest` tag and updates the pod-template revision annotation.
5. Commits the manifest update back to the same branch. The workflow path filter
   excludes `k8s/deployment.yaml`, so this commit does not trigger an image-build loop.

### One-time GitHub setup

- Push this project to a GitHub repository and ensure its default branch is named
  `main` (or change the branch filter in the workflow).
- In **Settings → Actions → General → Workflow permissions**, allow read and write
  permissions. The workflow also declares `contents: write` and `packages: write`.
- No personal access token is required for GHCR publishing; the workflow uses
  `GITHUB_TOKEN`.
- After the first successful run, verify the package under the repository's
  **Packages** tab. Configure package visibility and access for your cluster.

### Important deployment notes

- Replace the initial placeholder image in `k8s/deployment.yaml` if you deploy
  before the first successful workflow run. The workflow replaces it automatically.
- The deployment manifest uses `:latest` plus `imagePullPolicy: Always`; the
  revision annotation changes on each source commit and triggers a rollout.
  The immutable `sha-<commit>` tag is also published for traceability and rollback.
- If the GHCR package is private, create an image pull Secret in the `clusterpulse`
  namespace and add `imagePullSecrets` to the pod spec. For on-prem clusters, ensure
  nodes can reach `ghcr.io`, or mirror the published image into your internal registry.
- GitHub Actions commits directly to `main`. Branch protection rules that prohibit
  direct pushes by `GITHUB_TOKEN` must be adjusted, or the workflow must be changed
  to open a pull request instead.
- This workflow updates the Kubernetes manifest in Git; it does not directly apply
  the manifest to the cluster. A GitOps controller (such as Argo CD or Flux) should
  watch the repository to deploy the updated revision automatically. Otherwise,
  apply the changed manifest through your normal deployment process.


## Topology graph (v0.3)

Open **Topology → Resource graph** (or click a namespace name / *Graph* on a namespace card; deep link:
`/#topology/<namespace>`). The graph is laid out left to right:

    Middleware -> IngressRoute -> Service -> Deployment | StatefulSet | DaemonSet -> Pod
    Ingress ----------------------^            ^   ^   ^
                       PVC / ConfigMap / ServiceAccount

- Click a resource to trace its full upstream/downstream path and open the details panel
  (health, metadata, neighbours, *View logs* for pods, *Copy kubectl get*).
- Missing objects that something references (a ConfigMap, PVC, Service or Middleware that does not exist) are drawn as
  dashed red "Missing" nodes. This is the usual cause of `FailedMount` and 503s from Traefik.
- Drag to pan, scroll to zoom, `/` search, `F` fit, `Esc` close. Auto-updates every 10 s (Pause/Resume);
  the canvas is only redrawn when the graph actually changed (`revision`).
- Layout is computed in the browser with no external library, so it works in air-gapped clusters.

RBAC needed (already in `k8s/rbac.yaml`): `list` on `configmaps`, `serviceaccounts`, and `traefik.io`
`ingressroutes`/`middlewares`. If a kind is not permitted the graph still renders and shows a
"kind(s) skipped" notice. ConfigMap *contents* are never returned (only the key count); Secrets are never read.
To opt out of ConfigMaps: `TOPOLOGY_INCLUDE_CONFIGMAPS=false` and remove `configmaps` from the ClusterRole.

## Development

```bash
pip install -r requirements-dev.txt
python -m pytest -q                 # unit + API tests (no cluster needed)
python -m dev.mock_server           # dashboard with demo data on http://127.0.0.1:8099
```

## Upgrade notes (0.2 -> 0.3)

- Re-apply `k8s/rbac.yaml`, `k8s/configmap.yaml`, `k8s/deployment.yaml` (adds RBAC, hardening, `Recreate` strategy).
- `/readyz` now returns 503 while PostgreSQL is unreachable (traffic is withheld; the pod is not restarted).
- `/api/logs` now refuses namespaces listed in `MONITOR_EXCLUDED_NAMESPACES` (403).
- `/api/topology` totals changed: `deployments` -> `workloads` (Deployments + StatefulSets + DaemonSets), new `config`.
- If you apply `k8s/clusterpulse-networkpolicy.yaml`, it now admits Traefik from `kube-system` (k3s default) or `traefik`.

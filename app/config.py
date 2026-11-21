from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    app_name: str = "k8s-ai-rca-agent"
    cluster_name: str = "on-prem"
    environment: str = "production"
    database_url: str = "postgresql+psycopg://rca:change-me@postgres:5432/rca"
    ai_provider: str = "openai_compatible"  # openai_compatible | azure_openai | ollama
    ai_base_url: str = "https://api.openai.com/v1"
    ai_api_key: str = ""
    ai_model: str = "gpt-4.1-mini"
    ai_timeout_seconds: int = 45
    rca_max_attempts: int = 6
    rca_retry_base_seconds: int = 60
    rca_retry_max_seconds: int = 1800
    ai_enabled: bool = True
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from: str = ""
    smtp_starttls: bool = True
    smtp_use_ssl: bool = False
    email_recipients: str = ""
    email_cc: str = ""
    email_severity_min: str = "MEDIUM"
    prometheus_url: str = ""
    monitor_excluded_namespaces: str = "kube-system,kube-public,kube-node-lease"
    restart_threshold: int = 3
    restart_window_minutes: int = 10
    pending_minutes: int = 5
    incident_cooldown_minutes: int = 30
    log_tail_lines: int = 120
    max_log_chars: int = 12000
    watcher_resync_seconds: int = 30
    # Topology graph
    topology_cache_seconds: int = 5          # one K8s query per window, however many browsers poll
    topology_include_configmaps: bool = True # needs `configmaps` list RBAC; only key counts are returned
    topology_max_nodes: int = 800            # hard cap for very large namespaces / "all namespaces"
    topology_max_pods_per_workload: int = 40 # extra pods are folded into one "+N more" node

    @property
    def excluded_namespaces(self):
        return {x.strip() for x in self.monitor_excluded_namespaces.split(",") if x.strip()}

    @property
    def recipients(self):
        return [x.strip() for x in self.email_recipients.split(",") if x.strip()]

settings = Settings()

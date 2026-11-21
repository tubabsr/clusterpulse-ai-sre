import asyncio
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from html import escape

from app.config import settings


_SEVERITY_STYLES = {
    "CRITICAL": {"accent": "#dc2626", "soft": "#fef2f2", "label": "CRITICAL"},
    "HIGH": {"accent": "#ea580c", "soft": "#fff7ed", "label": "HIGH"},
    "MEDIUM": {"accent": "#d97706", "soft": "#fffbeb", "label": "MEDIUM"},
    "LOW": {"accent": "#0891b2", "soft": "#ecfeff", "label": "LOW"},
}


def _safe(value, fallback="Not provided"):
    """Escape dynamic values before inserting them into HTML."""
    if value is None or str(value).strip() == "":
        value = fallback
    return escape(str(value), quote=True)


def _incident_timing(incident):
    opened = getattr(incident, "created_at", None)
    if opened is None:
        return "Unknown", "Unknown"

    if opened.tzinfo is None:
        opened = opened.replace(tzinfo=timezone.utc)

    opened_utc = opened.astimezone(timezone.utc)
    seconds = max(0, int((datetime.now(timezone.utc) - opened_utc).total_seconds()))
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60

    parts = []
    if days:
        parts.append(f"{days} day" + ("" if days == 1 else "s"))
    if hours:
        parts.append(f"{hours} hour" + ("" if hours == 1 else "s"))
    if minutes or not parts:
        parts.append(f"{minutes} minute" + ("" if minutes == 1 else "s"))

    return opened_utc.strftime("%Y-%m-%d %H:%M:%S UTC"), ", ".join(parts)


def _as_items(value):
    """Normalize RCA lists that may contain strings or structured objects."""
    if not value:
        return []
    if isinstance(value, (str, dict)):
        value = [value]
    if not isinstance(value, (list, tuple)):
        return [str(value)]
    return value


def _item_text(item):
    if isinstance(item, dict):
        # Preserve useful context for structured evidence entries.
        return " · ".join(
            f"{key.replace('_', ' ').title()}: {val}"
            for key, val in item.items()
            if val is not None and str(val).strip()
        ) or "Not provided"
    return str(item)


def _list_html(value, ordered=False, empty="No details supplied."):
    items = _as_items(value)
    if not items:
        return f'<p style="margin:0;color:#64748b;font-size:14px;">{_safe(empty)}</p>'
    tag = "ol" if ordered else "ul"
    rows = "".join(
        '<li style="padding:0 0 8px 4px;color:#334155;font-size:14px;line-height:1.55;">'
        + _safe(_item_text(item)) + "</li>"
        for item in items
    )
    return f'<{tag} style="margin:0;padding-left:22px;">{rows}</{tag}>'


def _five_whys_html(value):
    rows = _as_items(value)
    if not rows:
        return '<p style="margin:0;color:#64748b;font-size:14px;">No five-whys analysis supplied.</p>'
    output = []
    for index, item in enumerate(rows, 1):
        if isinstance(item, dict):
            why = _safe(item.get("why", f"Why {index}"))
            answer = _safe(item.get("answer", ""))
        else:
            why = f"Why {index}"
            answer = _safe(item)
        output.append(
            '<tr><td width="34" valign="top" style="padding:10px 0;color:#0e7490;'
            'font-size:12px;font-weight:700;">'
            f'{index:02d}</td><td style="padding:8px 0 12px;border-bottom:1px solid #e2e8f0;">'
            f'<div style="font-size:13px;font-weight:700;color:#0f172a;">{why}</div>'
            f'<div style="padding-top:4px;font-size:14px;line-height:1.55;color:#475569;">{answer}</div>'
            '</td></tr>'
        )
    return '<table role="presentation" width="100%" cellspacing="0" cellpadding="0">' + "".join(output) + "</table>"


def _section(title, body, accent="#0e7490"):
    return f"""
    <tr><td class="section-cell" style="padding:0 28px 18px;">
      <table role="presentation" class="section-card" width="100%" cellspacing="0" cellpadding="0"
             style="border:1px solid #e2e8f0;border-radius:12px;background:#ffffff;">
        <tr><td class="section-heading" style="padding:16px 18px 8px;border-left:4px solid {accent};">
          <div style="font-size:11px;letter-spacing:1.2px;font-weight:800;color:#64748b;text-transform:uppercase;">{_safe(title)}</div>
        </td></tr>
        <tr><td class="section-body" style="padding:4px 18px 18px;font-size:14px;line-height:1.6;color:#334155;">{body}</td></tr>
      </table>
    </td></tr>"""


def _format_rca(incident, rca):
    """Build a Gmail/Outlook-friendly, table-based HTML incident report."""
    rca = rca or {}
    severity = str(getattr(incident, "severity", "UNKNOWN") or "UNKNOWN").upper()
    palette = _SEVERITY_STYLES.get(
        severity, {"accent": "#475569", "soft": "#f1f5f9", "label": severity}
    )
    opened_at, open_duration = _incident_timing(incident)

    incident_id = _safe(getattr(incident, "id", None))
    cluster = _safe(getattr(settings, "cluster_name", None))
    namespace = _safe(getattr(incident, "namespace", None))
    pod = _safe(getattr(incident, "pod_name", None))
    node = _safe(getattr(incident, "node_name", None))
    summary = _safe(rca.get("incident_summary", "No incident summary supplied."))
    confidence = _safe(rca.get("confidence", "Unknown"))
    root_cause = _safe(rca.get("root_cause", "Root cause analysis was not supplied."))
    impact = _safe(rca.get("impact", "Impact not assessed."))

    evidence = _list_html(rca.get("evidence"), empty="No evidence was attached to this analysis.")
    immediate = _list_html(rca.get("immediate_actions"), empty="No immediate actions supplied.")
    preventive = _list_html(rca.get("preventive_actions"), empty="No preventive actions supplied.")
    commands = _list_html(rca.get("kubectl_commands"), empty="No kubectl commands supplied.")
    further = _list_html(rca.get("further_investigation"), empty="No further investigation steps supplied.")
    whys = _five_whys_html(rca.get("five_whys"))

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light">
<title>ClusterPulse AI Incident RCA</title>
<style>
/* Fluid container and responsive email layouts. */
.email-shell {{ width:100% !important; max-width:680px !important; }}
@media only screen and (min-width:601px) and (max-width:900px) {{
  .email-shell {{ width:100% !important; max-width:680px !important; }}
  .email-pad {{ padding-left:22px !important; padding-right:22px !important; }}
  .hero-title {{ font-size:26px !important; line-height:1.22 !important; }}
}}
@media only screen and (max-width:600px) {{
  body {{ width:100% !important; min-width:100% !important; }}
  .email-shell {{ width:100% !important; max-width:100% !important; border-radius:0 !important; }}
  .email-pad {{ padding-left:16px !important; padding-right:16px !important; }}
  .email-header-pad {{ padding:18px 16px !important; }}
  .email-header-brand, .email-header-alert {{ display:block !important; width:100% !important; text-align:left !important; }}
  .email-header-alert {{ padding-top:12px !important; }}
  .meta-cell {{ display:block !important; width:100% !important; box-sizing:border-box !important; padding:12px 14px !important; }}
  .meta-table {{ table-layout:fixed !important; }}
  .hero-title {{ font-size:23px !important; line-height:1.22 !important; }}
  .hero-resource {{ font-size:13px !important; overflow-wrap:anywhere !important; word-break:break-word !important; }}
  .section-cell {{ padding-left:16px !important; padding-right:16px !important; }}
  .section-card {{ border-radius:9px !important; }}
  .section-heading {{ padding-left:13px !important; padding-right:13px !important; }}
  .section-body {{ padding-left:13px !important; padding-right:13px !important; }}
  .email-footer-pad {{ padding-left:16px !important; padding-right:16px !important; }}
}}
@media only screen and (max-width:380px) {{
  .email-pad {{ padding-left:12px !important; padding-right:12px !important; }}
  .email-header-pad {{ padding:16px 12px !important; }}
  .hero-title {{ font-size:21px !important; }}
  .section-cell {{ padding-left:12px !important; padding-right:12px !important; }}
  .meta-cell {{ padding-left:12px !important; padding-right:12px !important; }}
}}
</style>
</head>
<body style="margin:0;padding:0;background:#eef2f7;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#eef2f7;padding:24px 8px;">
<tr><td align="center">
<table role="presentation" class="email-shell" width="100%" cellspacing="0" cellpadding="0" border="0"
       style="width:100%;max-width:680px;background:#f8fafc;border-radius:18px;overflow:hidden;border:1px solid #dbe4ee;">
  <tr><td class="email-header-pad" style="padding:22px 28px;background:#071426;background-image:linear-gradient(120deg,#071426,#12304a);">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
      <tr><td class="email-header-brand" valign="middle">
        <div style="font-size:11px;letter-spacing:2px;font-weight:800;color:#67e8f9;">CLUSTERPULSE AI</div>
        <div style="padding-top:5px;font-size:13px;color:#cbd5e1;">Kubernetes observability · intelligent incident response</div>
      </td><td class="email-header-alert" align="right" valign="middle">
        <span style="display:inline-block;padding:7px 11px;border:1px solid #31516c;border-radius:20px;color:#a5f3fc;font-size:11px;font-weight:700;">INCIDENT ALERT</span>
      </td></tr>
    </table>
  </td></tr>

  <tr><td class="email-pad" style="padding:26px 28px 20px;background:{palette['soft']};border-bottom:1px solid #dbe4ee;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
      <tr><td>
        <div style="font-size:11px;letter-spacing:1.5px;font-weight:800;color:{palette['accent']};">KUBERNETES INCIDENT · {palette['label']}</div>
        <h1 class="hero-title" style="margin:8px 0 10px;font-size:28px;line-height:1.2;color:#0f172a;">Root cause analysis report</h1>
        <div class="hero-resource" style="padding-top:8px;font-size:14px;line-height:1.6;font-weight:700;color:#0f172a !important;word-break:break-word;overflow-wrap:anywhere;-webkit-text-fill-color:#0f172a;">{namespace} / {pod}</div>
      </td></tr>
      <tr><td style="padding-top:16px;">
        <span style="display:inline-block;background:{palette['accent']};color:#ffffff;padding:7px 12px;border-radius:7px;font-size:11px;font-weight:800;letter-spacing:.6px;">{palette['label']} SEVERITY</span>
        <span style="display:inline-block;margin-left:6px;background:#ffffff;border:1px solid #dbe4ee;color:#334155;padding:7px 12px;border-radius:7px;font-size:11px;">AI RCA REPORT</span>
      </td></tr>
    </table>
  </td></tr>

  <tr><td class="email-pad" style="padding:20px 28px 8px;">
    <table role="presentation" class="meta-table" width="100%" cellspacing="0" cellpadding="0" style="background:#ffffff;border:1px solid #dbe4ee;border-radius:12px;">
      <tr>
        <td class="meta-cell" width="50%" style="padding:15px 16px;border-bottom:1px solid #e2e8f0;">
          <div style="font-size:10px;font-weight:800;letter-spacing:1px;color:#64748b;">INCIDENT OPENED</div>
          <div style="padding-top:5px;font-size:14px;font-weight:700;color:#0f172a;">{_safe(opened_at)}</div>
        </td>
        <td class="meta-cell" width="50%" style="padding:15px 16px;border-bottom:1px solid #e2e8f0;">
          <div style="font-size:10px;font-weight:800;letter-spacing:1px;color:#64748b;">OPEN DURATION AT EMAIL</div>
          <div style="padding-top:5px;font-size:14px;font-weight:700;color:{palette['accent']};">{_safe(open_duration)}</div>
        </td>
      </tr>
      <tr>
        <td class="meta-cell" width="50%" style="padding:13px 16px;">
          <div style="font-size:10px;font-weight:800;letter-spacing:1px;color:#64748b;">CLUSTER / NAMESPACE</div>
          <div style="padding-top:5px;font-size:13px;color:#334155;">{cluster} / {namespace}</div>
        </td>
        <td class="meta-cell" width="50%" style="padding:13px 16px;">
          <div style="font-size:10px;font-weight:800;letter-spacing:1px;color:#64748b;">NODE</div>
          <div style="padding-top:5px;font-size:13px;color:#334155;">{node}</div>
        </td>
      </tr>
      <tr><td colspan="2" style="padding:0 16px 13px;">
        <div style="font-size:10px;font-weight:800;letter-spacing:1px;color:#64748b;">INCIDENT ID</div>
        <div style="padding-top:4px;font-size:12px;color:#475569;word-break:break-all;">{incident_id}</div>
      </td></tr>
    </table>
  </td></tr>

  {_section("Incident summary", f'<p style="margin:0;">{summary}</p>', palette["accent"])}
  {_section(f"Root cause · {confidence} confidence", f'<p style="margin:0;">{root_cause}</p>', palette["accent"])}
  {_section("Evidence", evidence)}
  {_section("Five whys", whys)}
  {_section("Impact assessment", f'<p style="margin:0;">{impact}</p>')}
  {_section("Immediate corrective actions", immediate, "#d97706")}
  {_section("Preventive actions", preventive, "#0e7490")}
  {_section("Suggested kubectl commands", commands, "#475569")}
  {_section("Further investigation", further, "#475569")}

  <tr><td class="email-pad email-footer-pad" style="padding:2px 28px 24px;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#eaf2f8;border-radius:10px;">
      <tr><td style="padding:14px 16px;font-size:12px;line-height:1.55;color:#475569;">
        <strong style="color:#0f172a;">Operator notice</strong><br>
        AI-generated analysis. Validate evidence and recommendations before execution.
        No remediation was performed by this notification.
      </td></tr>
    </table>
    <div style="padding-top:16px;text-align:center;font-size:11px;color:#94a3b8;">
      Automated notification from ClusterPulse AI · On-prem Kubernetes observability
    </div>
  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>"""


def _format_plain_text(incident, rca):
    """Plain-text fallback for clients that do not render HTML."""
    opened_at, open_duration = _incident_timing(incident)
    lines = [
        "CLUSTERPULSE AI — KUBERNETES INCIDENT RCA",
        "=" * 48,
        f"Severity: {getattr(incident, 'severity', 'UNKNOWN')}",
        f"Incident: {getattr(incident, 'id', 'Unknown')}",
        f"Opened at: {opened_at}",
        f"Open duration at email: {open_duration}",
        f"Cluster: {getattr(settings, 'cluster_name', 'Unknown')}",
        f"Namespace: {getattr(incident, 'namespace', 'Unknown')}",
        f"Pod: {getattr(incident, 'pod_name', 'Unknown')}",
        f"Node: {getattr(incident, 'node_name', 'Unknown')}",
        "",
        "INCIDENT SUMMARY",
        str(rca.get("incident_summary", "No incident summary supplied.")),
        "",
        f"ROOT CAUSE ({rca.get('confidence', 'Unknown')} confidence)",
        str(rca.get("root_cause", "Root cause analysis was not supplied.")),
    ]
    sections = [
        ("EVIDENCE", "evidence"),
        ("FIVE WHYS", "five_whys"),
        ("IMPACT", "impact"),
        ("IMMEDIATE CORRECTIVE ACTIONS", "immediate_actions"),
        ("PREVENTIVE ACTIONS", "preventive_actions"),
        ("SUGGESTED KUBECTL COMMANDS", "kubectl_commands"),
        ("FURTHER INVESTIGATION", "further_investigation"),
    ]
    for title, key in sections:
        lines.extend(["", title])
        value = rca.get(key, "Not supplied")
        if key == "impact":
            lines.append(str(value or "Not assessed."))
        else:
            for index, item in enumerate(_as_items(value), 1):
                if isinstance(item, dict) and key == "five_whys":
                    lines.append(f"{index}. {item.get('why', 'Why?')} {item.get('answer', '')}")
                else:
                    lines.append(f"{index}. {_item_text(item)}")
    lines.extend(["", "AI-generated analysis; validate recommendations before execution.", "No remediation was performed."])
    return "\n".join(lines)


async def send_incident_email(incident, rca):
    if not settings.recipients:
        return "SKIPPED_NO_RECIPIENTS"
    if not settings.smtp_host or not settings.smtp_from:
        return "SKIPPED_SMTP_NOT_CONFIGURED"

    subject = (
        f"[{getattr(incident, 'severity', 'UNKNOWN')}]"
        f"[{settings.cluster_name}] "
        f"{getattr(incident, 'namespace', 'unknown')}/"
        f"{getattr(incident, 'pod_name', 'unknown')}"
    )
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.smtp_from
    message["To"] = ", ".join(settings.recipients)
    if settings.email_cc:
        message["Cc"] = settings.email_cc

    message.set_content(_format_plain_text(incident, rca or {}))
    message.add_alternative(_format_rca(incident, rca or {}), subtype="html")
    recipients = list(settings.recipients) + [
        address.strip() for address in settings.email_cc.split(",") if address.strip()
    ]

    def deliver():
        if settings.smtp_use_ssl:
            server = smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port, timeout=20)
        else:
            server = smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20)
        try:
            server.ehlo()
            if settings.smtp_starttls and not settings.smtp_use_ssl:
                server.starttls()
                server.ehlo()
            if settings.smtp_username:
                server.login(settings.smtp_username, settings.smtp_password)
            server.send_message(message, to_addrs=recipients)
        finally:
            server.quit()

    await asyncio.to_thread(deliver)
    return "SENT"

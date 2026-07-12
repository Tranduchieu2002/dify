#!/usr/bin/env python3
"""
dify-workflow.py — import and export Dify workflows from the command line.

Usage:
  # Export an app by ID or name
  python3 dify-workflow.py export <app-id>
  python3 dify-workflow.py export <app-id> -o my-workflow.yml
  python3 dify-workflow.py export <app-id> --include-secrets

  # Export all apps
  python3 dify-workflow.py export --all -o ./exports/

  # Import (creates new app)
  python3 dify-workflow.py import my-workflow.yml

  # Import and immediately publish
  python3 dify-workflow.py import my-workflow.yml --publish

  # List apps
  python3 dify-workflow.py list
  python3 dify-workflow.py list --mode workflow
"""

import argparse
import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = os.environ.get("DIFY_BASE_URL", "http://localhost:5001")
EMAIL    = os.environ.get("DIFY_EMAIL",    "tranduchieu.swe@gmail.com")
PASSWORD = os.environ.get("DIFY_PASSWORD", "Hihihi111!")

# ── Auth client ───────────────────────────────────────────────────────────────

class DifyClient:
    def __init__(self, base_url: str, email: str, password: str):
        self.base = base_url.rstrip("/")
        self.email = email
        self.password = password
        self._cj = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._cj)
        )

    # ── low-level ────────────────────────────────────────────────────────────

    def _csrf(self) -> str | None:
        return next(
            (c.value for c in self._cj
             if "csrf_token" in c.name and not c.has_nonstandard_attr("HttpOnly")),
            None
        )

    def _req(self, method: str, path: str, data=None, params: dict | None = None,
             raw: bool = False):
        url = self.base + path
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
            url += f"?{qs}"

        body = json.dumps(data).encode() if data is not None else None
        headers: dict[str, str] = {}
        if body:
            headers["Content-Type"] = "application/json"
        csrf = self._csrf()
        if csrf:
            headers["X-CSRF-Token"] = csrf

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with self._opener.open(req, timeout=30) as resp:
                content = resp.read()
                return content if raw else json.loads(content)
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode()
            try:
                err = json.loads(body_txt)
                msg = err.get("message", body_txt)
            except Exception:
                msg = body_txt[:200]
            _die(f"HTTP {e.code} {method} {path}: {msg}")

    # ── auth ─────────────────────────────────────────────────────────────────

    def login(self):
        import base64
        pw_b64 = base64.b64encode(self.password.encode()).decode()
        self._req("POST", "/console/api/login", {
            "email": self.email,
            "password": pw_b64,
            "language": "en-US",
            "remember_me": True,
        })

    # ── apps ─────────────────────────────────────────────────────────────────

    def list_apps(self, mode: str | None = None, page: int = 1, limit: int = 100) -> list[dict]:
        params: dict = {"page": page, "limit": limit}
        if mode:
            params["mode"] = mode
        data = self._req("GET", "/console/api/apps", params=params)
        return data.get("data", [])

    def find_app(self, id_or_name: str) -> dict:
        """Find app by UUID or name (case-insensitive substring match)."""
        # try exact UUID first
        apps = self.list_apps()
        for app in apps:
            if app["id"] == id_or_name:
                return app
        # fallback: name match
        needle = id_or_name.lower()
        matches = [a for a in apps if needle in a["name"].lower()]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            names = ", ".join(f"'{a['name']}' ({a['id']})" for a in matches)
            _die(f"Ambiguous name '{id_or_name}' matches: {names}")
        _die(f"App not found: '{id_or_name}'")

    # ── export ────────────────────────────────────────────────────────────────

    def export(self, app_id: str, include_secrets: bool = False,
               workflow_id: str | None = None) -> str:
        params: dict = {"include_secret": str(include_secrets).lower()}
        if workflow_id:
            params["workflow_id"] = workflow_id
        data = self._req("GET", f"/console/api/apps/{app_id}/export", params=params)
        return data["data"]

    # ── import ────────────────────────────────────────────────────────────────

    def import_app(self, yaml_content: str) -> dict:
        result = self._req("POST", "/console/api/apps/imports", {
            "mode": "yaml-content",
            "yaml_content": yaml_content,
        })
        status = result.get("status", "?")
        if status == "failed":
            _die(f"Import failed: {result.get('errors', result)}")
        return result

    # ── publish ───────────────────────────────────────────────────────────────

    def publish(self, app_id: str) -> dict:
        return self._req("POST", f"/console/api/apps/{app_id}/workflows/publish", {})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _die(msg: str):
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def _safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_ " else "_" for c in name).strip().replace(" ", "-")


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_list(client: DifyClient, args: argparse.Namespace):
    apps = client.list_apps(mode=args.mode)
    if not apps:
        print("No apps found.")
        return

    col_id   = max(len("ID"),   max(len(a["id"])   for a in apps))
    col_mode = max(len("MODE"), max(len(a["mode"])  for a in apps))
    col_name = max(len("NAME"), max(len(a["name"])  for a in apps))

    header = f"{'ID':<{col_id}}  {'MODE':<{col_mode}}  {'NAME':<{col_name}}"
    print(header)
    print("-" * len(header))
    for a in apps:
        print(f"{a['id']:<{col_id}}  {a['mode']:<{col_mode}}  {a['name']:<{col_name}}")
    print(f"\n{len(apps)} app(s)")


def cmd_export(client: DifyClient, args: argparse.Namespace):
    if args.all:
        apps = client.list_apps(mode=args.mode)
        if not apps:
            _die("No apps found.")
        out_dir = Path(args.output or ".")
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"Exporting {len(apps)} app(s) to {out_dir}/")
        for app in apps:
            fname = _safe_filename(app["name"]) + ".yml"
            path = out_dir / fname
            yaml = client.export(app["id"], include_secrets=args.include_secrets)
            path.write_text(yaml)
            print(f"  ✓ {app['name']} ({app['id']})  →  {path}")
        return

    if not args.app_id:
        _die("Provide <app-id> or --all")

    app = client.find_app(args.app_id)
    yaml = client.export(app["id"], include_secrets=args.include_secrets)

    if args.output:
        out = Path(args.output)
        out.write_text(yaml)
        print(f"Exported '{app['name']}' → {out}")
    else:
        print(yaml)


def cmd_import(client: DifyClient, args: argparse.Namespace):
    path = Path(args.file)
    if not path.exists():
        _die(f"File not found: {path}")

    yaml = path.read_text()
    print(f"Importing {path} ...")
    result = client.import_app(yaml)

    app_id   = result.get("app_id", "")
    app_name = result.get("app", {}).get("name", "") if isinstance(result.get("app"), dict) else ""
    status   = result.get("status", "?")
    warnings = result.get("errors", [])

    print(f"  Status   : {status}")
    if app_id:
        print(f"  App ID   : {app_id}")
    if app_name:
        print(f"  App name : {app_name}")
    if warnings:
        print(f"  Warnings : {warnings}")

    if args.publish and app_id:
        print(f"  Publishing ...")
        pub = client.publish(app_id)
        print(f"  Published at {pub.get('created_at', '?')}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dify-workflow",
        description="Import and export Dify workflows from the command line.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
environment variables:
  DIFY_BASE_URL   API base (default: http://localhost:5001)
  DIFY_EMAIL      Account email
  DIFY_PASSWORD   Account password (plain text; base64 encoding is done internally)

examples:
  python3 dify-workflow.py list
  python3 dify-workflow.py list --mode workflow
  python3 dify-workflow.py export dd65e5e4-b837-4635-8006-c78f635fb7e3
  python3 dify-workflow.py export dd65e5e4-b837-4635-8006-c78f635fb7e3 -o backup.yml
  python3 dify-workflow.py export --all -o ./backups/
  python3 dify-workflow.py import my-workflow.yml
  python3 dify-workflow.py import my-workflow.yml --publish
        """,
    )

    sub = p.add_subparsers(dest="command", metavar="<command>")

    # list
    ls = sub.add_parser("list", help="List all apps")
    ls.add_argument("--mode", choices=["workflow", "advanced-chat", "agent-chat", "chat", "completion"],
                    help="Filter by app mode")

    # export
    ex = sub.add_parser("export", help="Export app DSL to YAML")
    ex.add_argument("app_id", nargs="?", metavar="<app-id>",
                    help="App UUID or name (omit with --all)")
    ex.add_argument("-o", "--output", metavar="PATH",
                    help="Output file or directory (default: stdout / current dir for --all)")
    ex.add_argument("--all", action="store_true", help="Export every app")
    ex.add_argument("--mode", choices=["workflow", "advanced-chat", "agent-chat", "chat", "completion"],
                    help="Filter by mode when using --all")
    ex.add_argument("--include-secrets", action="store_true",
                    help="Include API keys and secrets in the export")

    # import
    im = sub.add_parser("import", help="Import app from a YAML DSL file")
    im.add_argument("file", metavar="<file.yml>", help="Path to DSL YAML file")
    im.add_argument("--publish", action="store_true",
                    help="Publish the workflow immediately after import")

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    client = DifyClient(BASE_URL, EMAIL, PASSWORD)
    client.login()

    if args.command == "list":
        cmd_list(client, args)
    elif args.command == "export":
        cmd_export(client, args)
    elif args.command == "import":
        cmd_import(client, args)


if __name__ == "__main__":
    main()

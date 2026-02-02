# External endpoints for whitelisting

Here's the **list of endpoints to consider adding to the whitelist** for the Ontos app deployment:

---

# 🔐 Complete External Endpoints Whitelist for Ontos

## 📦 BUILD-TIME (During `databricks bundle deploy`)

| Endpoint | Port | Purpose | Required? |
|----------|------|---------|-----------|
| `registry.yarnpkg.com` | 443 | Yarn package downloads | ✅ Required |
| `registry.npmjs.org` | 443 | NPM fallback | ✅ Required |
| `pypi.org` | 443 | Python package index | ✅ Required |
| `files.pythonhosted.org` | 443 | Python package files | ✅ Required |

*The build script (`build_static.sh`) runs `yarn install` or `npm ci` which downloads ~70+ frontend packages.*
*Required for installing packages from `requirements.txt` (~30 Python packages).*

**Note:** The build also downloads **platform-specific esbuild binaries** (seen in yarn.lock) - these come from the same `registry.yarnpkg.com` endpoint.

---

## 🚀 RUNTIME (After Deployment)

| Endpoint | Port | Purpose | Required? |
|----------|------|---------|-----------|
| `<workspace>.azuredatabricks.net` | 443 | Databricks APIs | ✅ Required |
| `<workspace>.cloud.databricks.com` | 443 | Databricks APIs (AWS/GCP) | ✅ If applicable |
| PostgreSQL/Lakebase | 5432 | Database (internal) | ✅ Auto-managed |

---

## ⚙️ OPTIONAL (Feature-dependent)

| Endpoint | Port | Purpose | When Needed? |
|----------|------|---------|--------------|
| `github.com` | 443 | Git sync | If `GIT_REPO_URL` configured |
| `gitlab.com` | 443 | Git sync | If `GIT_REPO_URL` configured |
| `dev.azure.com` | 443 | Git sync (Azure DevOps) | If `GIT_REPO_URL` configured |
| `api.powerbi.com` | 443 | Power BI connector | ⚠️ Not yet implemented |

---

## ✅ CONFIRMED NOT NEEDED

Based on my analysis:

| Category | Status |
|----------|--------|
| External CDNs (fonts, CSS) | ❌ None - all bundled |
| Analytics/Telemetry | ❌ None |
| External auth providers (Auth0, Okta) | ❌ Uses Databricks OAuth only |
| WebSocket connections | ❌ None |
| External APIs (OpenAI direct) | ❌ Uses Databricks serving endpoints |
| Cloud storage (S3, Azure Blob) | ❌ Access through Databricks SDK |
| MLflow tracking servers | ❌ Not used |
| Remote ontology fetching | ❌ Parsed from local data |

---

## 📋 Quick Reference - Copy-Paste for Network Team

### Build Phase Whitelist:
```
registry.yarnpkg.com:443
registry.npmjs.org:443
pypi.org:443
files.pythonhosted.org:443
```

### Runtime Whitelist (replace with your workspace):
```
adb-xxx.azuredatabricks.net:443
```

### Optional Git Sync:
```
github.com:443
gitlab.com:443
dev.azure.com:443
```

---

## 🔍 How to Verify Nothing is Missing

If you want to double-check, here are some methods:

### 1. **Run a test build with network logging:**
```bash
# On your local machine with tcpdump
sudo tcpdump -i any -w build_traffic.pcap &
cd src && npm run build
# Then analyze: tcpdump -r build_traffic.pcap | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -u
```

### 2. **Use DNS logging during deployment:**
If your org uses Pi-hole, NextDNS, or similar - watch the DNS queries during a `databricks bundle deploy`.

### 3. **Check the Databricks Apps build logs:**
The error you showed (`error during npm build: exit status 1`) suggests the npm registry wasn't reachable. The deployment logs should show exactly which URLs failed.

---

**Bottom line:** The build error you're seeing is almost certainly because `registry.yarnpkg.com` and/or `registry.npmjs.org` aren't whitelisted in your deployment environment's network egress rules.

```plaintext
registry.yarnpkg.com:443
registry.npmjs.org:443
pypi.org:443
files.pythonhosted.org:443
```

```plaintext
adb-xxx.azuredatabricks.net:443
```

```plaintext
github.com:443
gitlab.com:443
dev.azure.com:443
```

```shellscript
# On your local machine with tcpdump
sudo tcpdump -i any -w build_traffic.pcap &
cd src && npm run build
# Then analyze: tcpdump -r build_traffic.pcap | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -u
```


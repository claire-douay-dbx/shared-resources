# External endpoints for whitelisting
_Exported on 02/02/2026 at 12:48:42 GMT from Cursor (2.2.17)_

---

**User**

I need to find all endpoints to whitelist. can you write me a list based on what you know is in this ontos repo? for reference here are ideas to consider:



To identify which external endpoints your application is connecting to before you deploy, you can use a combination of static code analysis (reading the code) and dynamic observation (running the code in a controlled sandbox).Since you have the GitHub repository, here are the most effective ways to extract those URLs for your whitelist.1. Static Analysis (Searching the Repo)This is the fastest way to find "hardcoded" URLs. Most applications define their external dependencies in configuration files or constants.A. The "Universal Search" (Regex)If you have the repo cloned locally, run a recursive search for strings that look like URLs. In your terminal, use grep or the VS Code search bar:Bash# Search for http or https patterns
grep -rEi "https?://" .
B. Language-Specific Config FilesCheck these specific files where developers usually store API base URLs:JavaScript/Node.js: .env, config.js, package.jsonPython: settings.py, requirements.txt, .envJava/Spring: application.properties, application.ymlGo: config.yaml, go.mod (for library origins)C. Use specialized toolsCodeQL: GitHub’s built-in engine. You can write a query to find all string literals that match a URL pattern.TruggleHog / Gitleaks: While usually used for finding secrets, they are excellent at scanning every commit in a repo history for specific patterns like URLs.2. Dynamic Observation (Local Execution)Static analysis misses URLs that are constructed at runtime (e.g., https:// + environment_variable + /api). To catch these, you need to run the app locally and "sniff" the traffic.A. The "Host File" Trick (Passive)Run your app and monitor your local system's networking logs. On Linux/macOS, you can use tcpdump or Wireshark to see all outgoing requests from your machine while the app is starting up.B. Using an Intercepting Proxy (Active)Tools like OWASP ZAP or Burp Suite allow you to route your local traffic through them.Launch the proxy.Set your system (or the specific app process) to use the proxy (usually localhost:8080).Run the application.The proxy will log every single request, including the full URL, headers, and destination IP.C. DNS LoggingIf you don't want to mess with proxies, use a tool like NextDNS or Pi-hole and point your machine’s DNS to it. You can then look at the "Logs" dashboard to see exactly which domains your machine attempted to resolve the moment you hit "Start" on your app.3. Containerized Monitoring (Recommended for Deployments)If your app is Dockerized, you can run it in a "learning mode" to see what it tries to touch.Run with a Logging Driver: Use docker run --log-driver=json-file ....Inspect Network Stats:Bashdocker stats <container_id>
Use strace: You can trace the system calls of the process to see connect() calls:Bashstrace -f -e trace=network <your_app_start_command>
Summary ChecklistMethodEffortAccuracyBest ForGrep SearchLowMediumHardcoded URLs/ConfigsBurp/ZAP ProxyMediumHighHidden/Dynamic URLsDNS LogsLowHighDomain whitelistingstraceHighMaximumLow-level/Obfuscated callsWould you like me to help you write a specific grep or CodeQL query tailored to the programming language your repo uses?

---

**Cursor**

I'll search the codebase for external endpoints and URLs. Let me run several searches to find all the URLs that need to be whitelisted.

Now I have a comprehensive view. Let me compile the complete whitelist of external endpoints for the Ontos application:

---

# Ontos External Endpoints Whitelist

Based on my analysis of the codebase, here are all the external endpoints your Ontos application connects to:

## 1. **Databricks Platform Endpoints** (Required)

| Endpoint Pattern | Purpose |
|-----------------|---------|
| `https://*.azuredatabricks.net` | Azure Databricks workspace API (configured via `DATABRICKS_HOST`) |
| `https://*.cloud.databricks.com` | AWS/GCP Databricks workspace API (if applicable) |
| `https://<workspace>/serving-endpoints` | LLM serving endpoints (Claude/GPT via Databricks Foundation Model APIs) |
| `https://<workspace>/sql/1.0/warehouses/*` | SQL Warehouse connections |
| `https://<workspace>/api/2.0/*` | Databricks REST API (Jobs, Unity Catalog, Workspace, etc.) |
| `https://<workspace>/api/2.1/*` | Databricks REST API v2.1 |

**Production Example:**
```
https://adb-984752964297111.11.azuredatabricks.net
```

## 2. **Database Endpoints** (Required)

| Endpoint Pattern | Purpose |
|-----------------|---------|
| PostgreSQL Host (`PGHOST`) | Lakebase/PostgreSQL database connection (auto-injected by Databricks Apps) |

*Note: In production (Databricks Apps), database credentials are automatically managed via OAuth. The PostgreSQL endpoint is internal to the Databricks infrastructure.*

## 3. **Optional Connector Endpoints** (Stub implementations, not active)

These are stubbed out but may require whitelisting when implemented:

| Endpoint | Purpose | Status |
|----------|---------|--------|
| `https://api.powerbi.com/v1.0/myorg` | Power BI REST API | ⚠️ Stub - not implemented |
| Snowflake account (`<account>.snowflakecomputing.com`) | Snowflake Data Cloud | ⚠️ Stub - not implemented |
| Kafka bootstrap servers | Apache Kafka / Confluent | ⚠️ Stub - not implemented |
| Schema Registry URL | Confluent Schema Registry | ⚠️ Stub - not implemented |

## 4. **Git Integration** (Optional)

| Endpoint Pattern | Purpose |
|-----------------|---------|
| `GIT_REPO_URL` (configurable) | Git repository for YAML sync (GitHub, GitLab, Bitbucket, etc.) |

**Common examples:**
```
https://github.com/*
https://gitlab.com/*
https://dev.azure.com/*
```

## 5. **No External CDN Dependencies**

The frontend does **not** load resources from external CDNs. All assets are bundled locally.

---

## Summary Whitelist for Production Deployment

### **Minimum Required:**
```
# Databricks Workspace (replace with your actual workspace URL)
https://adb-984752964297111.11.azuredatabricks.net

# Databricks APIs (same host, specific paths)
https://adb-984752964297111.11.azuredatabricks.net/api/*
https://adb-984752964297111.11.azuredatabricks.net/sql/*
https://adb-984752964297111.11.azuredatabricks.net/serving-endpoints/*
```

### **If LLM Features Enabled:**
```
# LLM Serving Endpoint (same workspace host)
https://adb-984752964297111.11.azuredatabricks.net/serving-endpoints/*
```

### **If Git Sync Enabled:**
```
# Your Git provider
https://github.com/*
# OR
https://gitlab.com/*
# OR
https://dev.azure.com/*
```

---

## Environment Variables That Define Endpoints

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Main Databricks workspace URL |
| `LLM_ENDPOINT` | Name of the Databricks serving endpoint for LLM |
| `LLM_BASE_URL` | Optional override for LLM API base URL |
| `PGHOST` | PostgreSQL/Lakebase host (auto-injected) |
| `GIT_REPO_URL` | Git repository URL for YAML sync |

---

## Notes

1. **All API calls go through the Databricks workspace** - The application doesn't directly call external APIs like OpenAI; it uses Databricks Foundation Model APIs which proxy to Claude/GPT.

2. **Internal-only traffic** - Most database (Lakebase/PostgreSQL) and Unity Catalog traffic is internal to the Databricks infrastructure.

3. **No telemetry/analytics** - The application doesn't send telemetry to external services.

4. **Frontend is self-contained** - No Google Fonts, FontAwesome CDN, or other external CSS/JS resources.

```plaintext
https://adb-984752964297111.11.azuredatabricks.net
```

```plaintext
https://github.com/*
https://gitlab.com/*
https://dev.azure.com/*
```

```plaintext
# Databricks Workspace (replace with your actual workspace URL)
https://adb-984752964297111.11.azuredatabricks.net

# Databricks APIs (same host, specific paths)
https://adb-984752964297111.11.azuredatabricks.net/api/*
https://adb-984752964297111.11.azuredatabricks.net/sql/*
https://adb-984752964297111.11.azuredatabricks.net/serving-endpoints/*
```

```plaintext
# LLM Serving Endpoint (same workspace host)
https://adb-984752964297111.11.azuredatabricks.net/serving-endpoints/*
```

```plaintext
# Your Git provider
https://github.com/*
# OR
https://gitlab.com/*
# OR
https://dev.azure.com/*
```

---

**User**

hmm but I think there is more. we are getting this error with the app build that suggests they need to access an npm endpoint to build the app. any way you can look into things that are necessary for creation but not needed post deployment? 

---

**Cursor**

Great catch! You're right - there are **build-time dependencies** that need network access during deployment but not after. Let me investigate the build process.

Now I have the full picture. Here's the **updated and complete whitelist** including **build-time dependencies**:

---

# Ontos External Endpoints Whitelist (Complete)

## 🔧 BUILD-TIME ENDPOINTS (Required During Deployment)

These endpoints are needed when Databricks Apps builds your application during deployment:

### **1. NPM/Yarn Package Registry**

| Endpoint | Purpose |
|----------|---------|
| `https://registry.yarnpkg.com/*` | Yarn package downloads (primary) |
| `https://registry.npmjs.org/*` | NPM fallback registry |

*The build script (`build_static.sh`) runs `yarn install` or `npm ci` which downloads ~70+ frontend packages.*

### **2. Python Package Index (PyPI)**

| Endpoint | Purpose |
|----------|---------|
| `https://pypi.org/*` | Python package index |
| `https://files.pythonhosted.org/*` | Python package file downloads |

*Required for installing packages from `requirements.txt` (~30 Python packages).*

---

## 🚀 RUNTIME ENDPOINTS (Required After Deployment)

### **3. Databricks Platform APIs**

| Endpoint Pattern | Purpose |
|-----------------|---------|
| `https://<workspace>.azuredatabricks.net/*` | Azure Databricks workspace |
| `https://<workspace>.cloud.databricks.com/*` | AWS/GCP Databricks workspace |

**Specific API paths used:**
- `/api/2.0/*` - REST API (Unity Catalog, Jobs, Workspace)
- `/api/2.1/*` - REST API v2.1
- `/sql/1.0/warehouses/*` - SQL Warehouse queries
- `/serving-endpoints/*` - LLM model serving

**Production example:**
```
https://adb-984752964297111.11.azuredatabricks.net
```

### **4. Database (Internal)**

| Endpoint | Purpose |
|----------|---------|
| PostgreSQL/Lakebase host | Auto-injected by Databricks Apps (internal) |

---

## 📋 COMPLETE WHITELIST FOR DEPLOYMENT

### **Build Phase (Temporary - during `databricks bundle deploy`):**

```plaintext
# NPM/Yarn Package Registry
https://registry.yarnpkg.com
https://registry.npmjs.org

# Python Package Index
https://pypi.org
https://files.pythonhosted.org
```

### **Runtime Phase (Permanent - after deployment):**

```plaintext
# Your Databricks Workspace (replace with actual)
https://adb-984752964297111.11.azuredatabricks.net
```

### **Optional (if features enabled):**

```plaintext
# Git Sync (if GIT_REPO_URL configured)
https://github.com
https://gitlab.com
https://dev.azure.com

# Future connectors (currently stubs)
https://api.powerbi.com
```

---

## Summary Table

| Phase | Endpoint | Required? |
|-------|----------|-----------|
| **BUILD** | `registry.yarnpkg.com` | ✅ Yes |
| **BUILD** | `registry.npmjs.org` | ✅ Yes (fallback) |
| **BUILD** | `pypi.org` | ✅ Yes |
| **BUILD** | `files.pythonhosted.org` | ✅ Yes |
| **RUNTIME** | `*.azuredatabricks.net` | ✅ Yes |
| **RUNTIME** | PostgreSQL/Lakebase | ✅ Internal |
| **RUNTIME** | Git provider | ⚠️ If sync enabled |
| **RUNTIME** | `api.powerbi.com` | ❌ Not implemented |

---

## 💡 Recommendation

If you're hitting the npm build error, make sure these are whitelisted for the **build/deployment network context**:

```plaintext
# Essential for frontend build
registry.yarnpkg.com:443
registry.npmjs.org:443

# Essential for Python dependencies
pypi.org:443
files.pythonhosted.org:443
```

The error `error during npm build: exit status 1` strongly suggests the Databricks Apps build environment can't reach the npm registry. You may need to work with your network team to allow outbound HTTPS (port 443) to these registries during the deployment process.

```plaintext
https://adb-984752964297111.11.azuredatabricks.net
```

```plaintext
# NPM/Yarn Package Registry
https://registry.yarnpkg.com
https://registry.npmjs.org

# Python Package Index
https://pypi.org
https://files.pythonhosted.org
```

```plaintext
# Your Databricks Workspace (replace with actual)
https://adb-984752964297111.11.azuredatabricks.net
```

```plaintext
# Git Sync (if GIT_REPO_URL configured)
https://github.com
https://gitlab.com
https://dev.azure.com

# Future connectors (currently stubs)
https://api.powerbi.com
```

```plaintext
# Essential for frontend build
registry.yarnpkg.com:443
registry.npmjs.org:443

# Essential for Python dependencies
pypi.org:443
files.pythonhosted.org:443
```

---

**User**

are we sure there isn't anything else? how to check?

---

**Cursor**

Good instinct - let me do a more thorough sweep to make sure we haven't missed anything:

Now I have a complete picture. Here's the **final comprehensive whitelist**:

---

# 🔐 Complete External Endpoints Whitelist for Ontos

## 📦 BUILD-TIME (During `databricks bundle deploy`)

| Endpoint | Port | Purpose | Required? |
|----------|------|---------|-----------|
| `registry.yarnpkg.com` | 443 | Yarn package downloads | ✅ Required |
| `registry.npmjs.org` | 443 | NPM fallback | ✅ Required |
| `pypi.org` | 443 | Python package index | ✅ Required |
| `files.pythonhosted.org` | 443 | Python package files | ✅ Required |

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
adb-984752964297111.11.azuredatabricks.net:443
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
adb-984752964297111.11.azuredatabricks.net:443
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


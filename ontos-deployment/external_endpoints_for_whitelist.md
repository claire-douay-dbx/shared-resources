# External endpoints for whitelisting
Here's the complete whitelist including **build-time dependencies**:

---

# Ontos External Endpoints Whitelist

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


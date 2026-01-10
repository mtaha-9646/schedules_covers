# SaaS Suite

A multi-tenant SaaS platform with SSO, centralized identity, and integrated web applications.

## Services
- **core/idp (Keycloak)**: Identity provider for SSO.
- **core/control-plane**: Central API for auth exchange and tenant management.
- **core/portal**: Unified entry point and app launcher.
- **apps/schedules-covers**: App for managing teacher substitutions.
- **apps/behavior**: App for student behavior tracking.

## Getting Started

### Prerequisites
- Docker & Docker Compose

### Fast Start
1.  **Environment Setup**:
    ```powershell
    cp .env.example .env
    ```
2.  **Bring up the stack**:
    ```powershell
    docker-compose up -d
    ```
3.  **Initialize & Migrate Data**:
    ```powershell
    docker-compose exec control-plane python scripts/migrate_data.py
    ```

## Accessing the Suite
- **Portal**: [http://localhost:3000](http://localhost:3000)
- **Keycloak Console**: [http://localhost:8080](http://localhost:8080)
- **Control Plane API**: [http://localhost:8000/health](http://localhost:8000/health)

## Default Admin Credentials
- **Keycloak**: `admin` / `admin`
- **Portal Super Admin**: `admin@example.com` (after OIDC login)

## Documentation
- [ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [SECURITY.md](docs/SECURITY.md)
- [DATABASE.md](docs/DATABASE.md)
- [INTEGRATION_GUIDE.md](docs/INTEGRATION_GUIDE.md)

-- Create schemas
CREATE SCHEMA IF NOT EXISTS control_plane;
CREATE SCHEMA IF NOT EXISTS idp;
CREATE SCHEMA IF NOT EXISTS behavior;
CREATE SCHEMA IF NOT EXISTS schedules_covers;

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Control Plane Tables
CREATE TABLE control_plane.tenants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    settings JSONB DEFAULT '{}',
    branding JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE control_plane.users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idp_subject UUID UNIQUE,
    email TEXT UNIQUE NOT NULL,
    name TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE control_plane.memberships (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES control_plane.tenants(id) ON DELETE CASCADE,
    user_id UUID REFERENCES control_plane.users(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tenant_id, user_id)
);

CREATE TABLE control_plane.roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    scope TEXT NOT NULL -- 'platform', 'tenant', 'app'
);

CREATE TABLE control_plane.permissions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key TEXT UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE control_plane.role_permissions (
    role_id UUID REFERENCES control_plane.roles(id) ON DELETE CASCADE,
    permission_id UUID REFERENCES control_plane.permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE control_plane.membership_roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    membership_id UUID REFERENCES control_plane.memberships(id) ON DELETE CASCADE,
    role_id UUID REFERENCES control_plane.roles(id) ON DELETE CASCADE,
    app_id UUID, -- NULL for tenant-wide roles
    UNIQUE(membership_id, role_id, app_id)
);

CREATE TABLE control_plane.apps (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    manifest JSONB DEFAULT '{}'
);

CREATE TABLE control_plane.tenant_apps (
    tenant_id UUID REFERENCES control_plane.tenants(id) ON DELETE CASCADE,
    app_id UUID REFERENCES control_plane.apps(id) ON DELETE CASCADE,
    enabled BOOLEAN DEFAULT TRUE,
    config JSONB DEFAULT '{}',
    PRIMARY KEY (tenant_id, app_id)
);

CREATE TABLE control_plane.feature_flags (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key TEXT UNIQUE NOT NULL,
    description TEXT,
    rules JSONB DEFAULT '{}',
    enabled BOOLEAN DEFAULT TRUE
);

CREATE TABLE control_plane.audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ts TIMESTAMPTZ DEFAULT NOW(),
    actor_user_id UUID NOT NULL,
    tenant_id UUID,
    action TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    metadata JSONB DEFAULT '{}',
    ip TEXT,
    user_agent TEXT
);

-- Behavior Tables
CREATE TABLE behavior.teachers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    subject TEXT,
    grade TEXT,
    tenant_id UUID REFERENCES control_plane.tenants(id)
);

CREATE TABLE behavior.students (
    id SERIAL PRIMARY KEY,
    esis TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    homeroom TEXT,
    tenant_id UUID REFERENCES control_plane.tenants(id)
);

CREATE TABLE behavior.incidents (
    id SERIAL PRIMARY KEY,
    esis TEXT NOT NULL,
    name TEXT NOT NULL,
    homeroom TEXT,
    date_of_incident TIMESTAMPTZ DEFAULT NOW(),
    place_of_incident TEXT,
    incident_grade TEXT,
    action_taken TEXT,
    incident_description TEXT,
    attachment TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    teacher_id INTEGER REFERENCES behavior.teachers(id),
    tenant_id UUID REFERENCES control_plane.tenants(id)
);

-- Schedules & Covers Tables
CREATE TABLE schedules_covers.assignments (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    period_label TEXT,
    absent_teacher TEXT,
    cover_teacher TEXT,
    status TEXT,
    tenant_id UUID REFERENCES control_plane.tenants(id)
);

CREATE TABLE schedules_covers.settings (
    key TEXT PRIMARY KEY,
    value JSONB,
    tenant_id UUID REFERENCES control_plane.tenants(id)
);

CREATE TABLE control_plane.custom_fields (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES control_plane.tenants(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL, -- e.g., 'teacher', 'student'
    field_key TEXT NOT NULL,
    field_type TEXT NOT NULL,
    options_json JSONB DEFAULT '{}',
    required BOOLEAN DEFAULT FALSE,
    default_value_json JSONB DEFAULT '{}',
    UNIQUE(tenant_id, entity_type, field_key)
);

CREATE TABLE control_plane.ui_themes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES control_plane.tenants(id) ON DELETE CASCADE,
    theme_json JSONB DEFAULT '{}',
    UNIQUE(tenant_id)
);

-- Seed basic roles
INSERT INTO control_plane.roles (name, scope) VALUES 
('platform_super_admin', 'platform'),
('tenant_owner', 'tenant'),
('tenant_admin', 'tenant'),
('tenant_member', 'tenant'),
('tenant_viewer', 'tenant');

-- Seed basic permissions
INSERT INTO control_plane.permissions (key, description) VALUES 
('portal.tenants.manage', 'Ability to create and manage tenants'),
('portal.users.manage', 'Ability to invite and manage users'),
('portal.apps.manage', 'Ability to enable/disable apps for tenants');

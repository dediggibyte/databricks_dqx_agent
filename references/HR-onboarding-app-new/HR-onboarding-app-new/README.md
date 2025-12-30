# HR Onboarding Concierge

A multi-agent HR onboarding system built on Databricks with:
- 🤖 **Knowledge Assistant** for HR policy questions
- 📋 **Task Management** with Lakebase database
- 📊 **AI/BI Dashboard** for HR analytics

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Flask Web Application                     │
├─────────────────┬─────────────────┬─────────────────────────┤
│   Employee UI   │   Admin UI      │   API Routes            │
│   - Tasks       │   - Dashboard   │   - /toggle-task        │
│   - Chat        │   (iframe)      │   - /ask                │
└────────┬────────┴────────┬────────┴────────────┬────────────┘
         │                 │                     │
         ▼                 ▼                     ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────┐
│   Lakebase      │ │  AI/BI          │ │  Knowledge          │
│   (PostgreSQL)  │ │  Dashboard      │ │  Assistant          │
│                 │ │                 │ │  (Agent Endpoint)   │
└─────────────────┘ └─────────────────┘ └─────────────────────┘
```

## Project Structure

```
hr-onboarding-app/
├── app.py                 # Main Flask application
├── app.yaml               # Databricks Apps deployment config
├── requirements.txt       # Python dependencies
├── .env                   # Local development environment (not committed)
├── .gitignore             # Git ignore rules
├── custom_logger/         # Logging module
│   └── __init__.py
├── logs/                  # Application logs
├── static/
│   └── style.css          # Custom styles
└── templates/
    ├── base.html          # Base template
    ├── landing.html       # Role selection page
    ├── employee.html      # Employee tasks & chat
    └── admin.html         # Admin dashboard
```

## Prerequisites

1. **Databricks Workspace** with:
   - Lakebase instance created
   - Knowledge Assistant agent deployed
   - AI/BI Dashboard created and published

2. **Lakebase Tables** (create these in your Lakebase instance):

```sql
-- Onboarding checklist table
CREATE TABLE public.onboarding_checklist (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    is_completed BOOLEAN DEFAULT FALSE,
    completed_by VARCHAR(255),
    completed_at TIMESTAMP
);

-- History table for audit trail
CREATE TABLE public.onboarding_checklist_history (
    id SERIAL PRIMARY KEY,
    task_id INTEGER REFERENCES public.onboarding_checklist(id),
    user_email VARCHAR(255),
    is_completed BOOLEAN,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample tasks
INSERT INTO public.onboarding_checklist (task_name, category) VALUES
    ('Complete I-9 Form', 'Documentation'),
    ('Set up Direct Deposit', 'Payroll'),
    ('Review Employee Handbook', 'Policies'),
    ('Complete IT Security Training', 'Training'),
    ('Set up Workstation', 'IT'),
    ('Meet with Manager', 'Orientation'),
    ('Enroll in Benefits', 'Benefits'),
    ('Complete Background Check Authorization', 'Documentation');
```

## Local Development

1. **Clone and setup:**
   ```bash
   cd hr-onboarding-app
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   - Copy `.env.example` to `.env`
   - Update with your Databricks credentials:
     - `DATABRICKS_HOST`: Your workspace URL
     - `DATABRICKS_TOKEN`: Personal Access Token
     - `LAKEBASE_INSTANCE_NAME`: Lakebase instance UUID
     - `DB_HOST`, `DB_USER`, `DB_NAME`: Database connection details
     - `AGENT_ENDPOINT_NAME`: Knowledge Assistant endpoint
     - `DATABRICKS_DASHBOARD_URL`: Embedded dashboard URL

3. **Run locally:**
   ```bash
   python app.py
   ```
   Access at: http://localhost:8000

## Databricks Apps Deployment

1. **Update `app.yaml`** with your actual values:
   - Lakebase instance name
   - Agent endpoint name
   - Dashboard URL

2. **Deploy using Databricks CLI:**
   ```bash
   databricks apps deploy hr-onboarding-app --source-code-path ./
   ```

3. **Grant Lakebase permissions** to the app's service principal:
   ```sql
   -- Run in Lakebase SQL Editor
   GRANT ALL ON TABLE public.onboarding_checklist TO "<app-service-principal-client-id>";
   GRANT ALL ON TABLE public.onboarding_checklist_history TO "<app-service-principal-client-id>";
   ```

## Key Features

### OAuth Token Management
The app automatically handles Lakebase authentication:
- Generates OAuth tokens using `generate_database_credential()` API
- Auto-refreshes tokens every 50 minutes (before 60-minute expiration)
- Thread-safe token management with connection pooling

### Role-Based Access
- **Employee**: View/complete onboarding tasks, chat with HR assistant
- **Admin**: View embedded AI/BI dashboard with HR analytics

### Audit Trail
All task changes are logged to `onboarding_checklist_history` table.

## Troubleshooting

### "Invalid authorization for databricks identity login"
- Ensure `LAKEBASE_INSTANCE_NAME` is set correctly
- Verify the app service principal has database permissions
- Check that `databricks-sdk>=0.56.0` is installed

### Dashboard not loading
- Verify embed URL is correct (iframe format, not JavaScript)
- Ensure workspace has dashboard embedding enabled
- Check that `*.databricksapps.com` is in allowed embed domains

### Agent not responding
- Verify `AGENT_ENDPOINT_NAME` matches your deployed endpoint
- Check endpoint status in Databricks serving UI
- Review endpoint logs for errors

## License

Internal use only. © 2024

"""
HR Onboarding Concierge - Flask Application
============================================
A multi-agent HR onboarding system with:
- Knowledge Assistant for HR policies
- Lakebase database for task management
- Embedded AI/BI Dashboard for admins
"""

from flask import Flask, render_template, request, redirect, session, url_for, jsonify
from sqlalchemy import create_engine, text, event
from dotenv import load_dotenv
import os
import uuid
import time
import threading
from datetime import datetime
from databricks.sdk import WorkspaceClient
from urllib.parse import quote_plus
from openai import OpenAI

from custom_logger import logger

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "supersecretkey-change-in-production")

# ---------------- CONFIG -----------------
AGENT_ENDPOINT_NAME = os.getenv("AGENT_ENDPOINT_NAME")
DASHBOARD_URL = os.getenv("DATABRICKS_DASHBOARD_URL")
LAKEBASE_INSTANCE_NAME = os.getenv("LAKEBASE_INSTANCE_NAME")


# ============================================================
# OAUTH TOKEN MANAGEMENT FOR LAKEBASE
# ============================================================
# Lakebase OAuth tokens expire every 60 minutes.
# This module automatically refreshes tokens every 50 minutes.
# ============================================================

_token_state = {
    "password": None,
    "last_refresh": 0,
    "workspace_client": None,
    "engine": None
}
_token_lock = threading.Lock()


def _get_workspace_client():
    """Get or create WorkspaceClient for Databricks API calls."""
    if _token_state["workspace_client"] is None:
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        
        if host and token:
            # Local development with PAT
            _token_state["workspace_client"] = WorkspaceClient(host=host, token=token)
            logger.info("WorkspaceClient initialized with PAT authentication")
        else:
            # Databricks Apps - auto-authenticates via service principal
            _token_state["workspace_client"] = WorkspaceClient()
            logger.info("WorkspaceClient initialized with auto-authentication")
    
    return _token_state["workspace_client"]


def _generate_oauth_token():
    """Generate a fresh OAuth token for Lakebase connection."""
    w = _get_workspace_client()
    instance_name = LAKEBASE_INSTANCE_NAME
    
    if not instance_name:
        raise ValueError(
            "LAKEBASE_INSTANCE_NAME environment variable is required. "
            "Set it to your Lakebase instance ID (e.g., 'e1c07201-6c30-4306-bbe0-f40d8ebcf2e4')"
        )
    
    try:
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance_name]
        )
        logger.info("Generated new Lakebase OAuth token successfully")
        return cred.token
    except Exception as e:
        logger.error(f"Failed to generate OAuth token: {str(e)}")
        raise


def _refresh_token_if_needed():
    """Refresh token if it's older than 50 minutes (tokens expire at 60 min)."""
    with _token_lock:
        time_since_refresh = time.time() - _token_state["last_refresh"]
        
        if time_since_refresh > 50 * 60 or _token_state["password"] is None:
            logger.info(f"Token refresh needed (age: {time_since_refresh/60:.1f} minutes)")
            _token_state["password"] = _generate_oauth_token()
            _token_state["last_refresh"] = time.time()
        
        return _token_state["password"]


def get_engine():
    """
    Create SQLAlchemy engine with OAuth token authentication.
    """
    global _token_state
    
    # Return cached engine if exists
    if _token_state["engine"] is not None:
        _refresh_token_if_needed()
        return _token_state["engine"]
    
    # Try PGXXX vars first (auto-injected by Databricks Apps), then fallback to DB_XXX
    db_user = os.getenv("PGUSER") or os.getenv("DB_USER")
    db_host = os.getenv("PGHOST") or os.getenv("DB_HOST")
    db_name = os.getenv("PGDATABASE") or os.getenv("DB_NAME")
    
    logger.info(f"Creating database engine: user={db_user}, host={db_host}, db={db_name}")
    
    if not db_host or not db_name:
        raise ValueError(
            "Missing database connection parameters. "
            "Set DB_HOST, DB_NAME (or PGHOST, PGDATABASE for Databricks Apps)"
        )
    
    # Get username if not provided
    if not db_user:
        w = _get_workspace_client()
        db_user = os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name
        logger.info(f"Using auto-detected username: {db_user}")
    
    # URL-encode the username (handles @ symbol in email addresses)
    db_user_encoded = quote_plus(db_user)
    
    # Generate initial OAuth token
    _token_state["password"] = _generate_oauth_token()
    _token_state["last_refresh"] = time.time()
    
    # Create engine with placeholder password (event listener will inject real token)
    engine = create_engine(
        f"postgresql+psycopg2://{db_user_encoded}:placeholder@{db_host}:5432/{db_name}"
        "?sslmode=require",
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600
    )
    
    # Event listener to inject fresh token for each new connection
    @event.listens_for(engine, "do_connect")
    def provide_token(dialect, conn_rec, cargs, cparams):
        cparams["password"] = _refresh_token_if_needed()
    
    _token_state["engine"] = engine
    logger.info("Database engine created successfully with OAuth authentication")
    return engine


# ============================================================
# AGENT INTEGRATION
# ============================================================

def ask_agent(messages):
    """
    Send a question to the Knowledge Assistant agent.
    
    Args:
        question: User's question about HR policies
        
    Returns:
        Agent's response text or error message
    """
    if not AGENT_ENDPOINT_NAME:
        return "⚠️ Agent endpoint not configured. Set AGENT_ENDPOINT_NAME in environment."
    
    try:
        logger.info(f"Querying agent: {AGENT_ENDPOINT_NAME}")
        
        # Get Databricks token and host
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        
        # In Databricks Apps, extract token from WorkspaceClient's authentication
        if not token:
            w = _get_workspace_client()
            # Get the authentication headers from WorkspaceClient
            auth_headers = w.config.authenticate()
            auth_header = auth_headers.get("Authorization", "")
            
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]  # Remove "Bearer " prefix
                logger.info("Extracted OAuth token from WorkspaceClient")
            else:
                logger.error(f"Could not extract Bearer token from WorkspaceClient auth headers")
                return "❌ Error: Could not authenticate with Databricks workspace. Check service principal permissions."
        
        # Extract base URL from host
        if host:
            workspace_url = host
        else:
            # In Databricks Apps, construct from workspace URL
            # w should already exist from token extraction above, but get it if needed
            try:
                workspace_url = w.config.host
            except NameError:
                w = _get_workspace_client()
                workspace_url = w.config.host
        
        # Validate workspace_url is not None or empty
        if not workspace_url:
            logger.error("Could not determine Databricks workspace URL")
            return "❌ Error: Databricks workspace URL not configured. Set DATABRICKS_HOST environment variable."
        
        # Ensure URL has https:// protocol
        if not workspace_url.startswith(("http://", "https://")):
            workspace_url = f"https://{workspace_url}"
        
        base_url = workspace_url.rstrip('/') + "/serving-endpoints"
        
        logger.info(f"Using base_url: {base_url}")
        
        # Create OpenAI client configured for Databricks
        client = OpenAI(
            api_key=token,
            base_url=base_url
        )
        
        # Query the agent using OpenAI client
        response = client.responses.create(
            model=AGENT_ENDPOINT_NAME,
            input=messages)
        # Extract text from response.output[0].content[0].text
        try:
            if hasattr(response, 'output') and response.output:
                # Join all text content from all outputs
                answer = " ".join(
                    getattr(content, "text", "")
                    for output in response.output
                    for content in getattr(output, "content", [])
                )
                
                if answer.strip():
                    logger.info(f"Agent response received ({len(answer)} chars)")
                    return answer.strip()
        except Exception as e:
            logger.error(f"Failed to parse response.output: {e}")
        
        # Fallback: try to access as dictionary
        try:
            response_dict = response.model_dump() if hasattr(response, 'model_dump') else dict(response)
            logger.info(f"Response structure: {response_dict}")
            
            if 'output' in response_dict and response_dict['output']:
                output = response_dict['output'][0]
                if 'content' in output and output['content']:
                    content = output['content'][0]
                    if 'text' in content:
                        answer = content['text']
                        logger.info(f"Agent response from dict ({len(answer)} chars)")
                        return answer
        except Exception as e:
            logger.error(f"Dictionary parsing failed: {e}")
        
        logger.warning(f"Agent returned no parseable data. Response: {response}")
        return "⚠️ No answer returned from agent (no relevant knowledge found)."

    except Exception as e:
        logger.error(f"Agent error: {str(e)}", exc_info=True)
        return f"❌ Error contacting agent: {str(e)}"


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def landing():
    """Landing page with role selection."""
    logger.info("Landing page accessed")
    return render_template("landing.html")


@app.route("/select-role", methods=["POST"])
def select_role():
    """Handle role selection and redirect to appropriate page."""
    role = request.form.get("role")

    session["role"] = role
    session["user_email"] = request.headers.get(
        "X-Forwarded-Email", "demo.user@company.com"
    )

    logger.info(f"Role selected: role={role}, user_email={session['user_email']}")

    if role == "USER":
        session["messages"] = []
        return redirect(url_for("employee"))

    elif role == "ADMIN":
        return redirect(url_for("admin"))

    logger.warning(f"Unknown role selected: {role}")
    return redirect(url_for("landing"))


@app.route("/employee")
def employee():
    """Employee page with onboarding tasks and chat."""
    if session.get("role") != "USER":
        logger.warning(f"Unauthorized employee page access from role={session.get('role')}")
        return redirect(url_for("landing"))

    tasks = []
    try:
        with get_engine().connect() as conn:
            logger.info(f"Fetching onboarding checklist for {session['user_email']}")
            tasks = conn.execute(
                text("SELECT * FROM public.onboarding_checklist ORDER BY id")
            ).mappings().all()
            logger.info(f"Retrieved {len(tasks)} onboarding tasks")
    except Exception as e:
        logger.error(f"Database error fetching tasks: {str(e)}")

    return render_template(
        "employee.html",
        tasks=tasks,
        user=session["user_email"],
        messages=session.get("messages", [])
    )


@app.route("/toggle-task", methods=["POST"])
def toggle_task():
    """Toggle a task's completion status."""
    if session.get("role") != "USER":
        logger.warning(f"Unauthorized toggle_task from role={session.get('role')}")
        return jsonify({"error": "Unauthorized"}), 401

    try:
        task_id = request.json["task_id"]
        current_status = request.json["current_status"]
        user_email = session["user_email"]
        new_status = not current_status
        now = datetime.now()

        logger.info(f"Toggling task {task_id}: {current_status} -> {new_status}")

        with get_engine().connect() as conn:
            # Update the task
            conn.execute(text("""
                UPDATE public.onboarding_checklist
                SET is_completed = :status, 
                    completed_by = :user, 
                    completed_at = :time
                WHERE id = :id
            """), dict(status=new_status, user=user_email, time=now, id=task_id))

            # Log to history table
            conn.execute(text("""
                INSERT INTO public.onboarding_checklist_history
                (task_id, user_email, is_completed, changed_at)
                VALUES (:task_id, :user_email, :status, :time)
            """), dict(task_id=task_id, user_email=user_email, status=new_status, time=now))

            conn.commit()

        logger.info(f"Task {task_id} toggled successfully")
        return jsonify({"status": "ok"})

    except Exception as e:
        logger.error(f"Toggle task error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/ask", methods=["POST"])
def ask():
    """Handle chat questions to the Knowledge Assistant."""
    if session.get("role") != "USER":
        logger.warning(f"Unauthorized ask from role={session.get('role')}")
        return jsonify({"error": "Unauthorized"}), 401

    prompt = request.json.get("prompt", "").strip()
    
    if not prompt:
        return jsonify({"answer": "Please enter a question."}), 400

    logger.info(f"User {session['user_email']} asked: {prompt[:50]}...")

    session["messages"].append({
        "role": "user",
        "content": prompt
    })

    # Build agent-only history with user email injected
    messages_for_agent = [
        {
            "role": "system",
            "content": f"You are assisting the employee with email: {session['user_email']}. "
                    f"Use this email when creating requests, submitting tickets, or referencing the user."
        }
    ] + session["messages"]

    # Get agent response using enriched context
    answer = ask_agent(messages_for_agent)

    # Add assistant response to history
    session["messages"].append({
        "role": "assistant",
        "content": answer
    })

    session.modified = True
    logger.info(f"Chat response sent to {session['user_email']}")
    return jsonify({"answer": answer})


@app.route("/admin")
def admin():
    """Admin page with embedded AI/BI Dashboard."""
    if session.get("role") != "ADMIN":
        logger.warning(f"Unauthorized admin page access from role={session.get('role')}")
        return redirect(url_for("landing"))

    logger.info(f"Admin dashboard accessed by {session.get('user_email')}")
    return render_template("admin.html", dashboard_url=DASHBOARD_URL)


@app.route("/health")
def health():
    """Health check endpoint for Databricks Apps."""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    logger.info("Starting Flask development server on port 8000")
    app.run(debug=True, host="0.0.0.0", port=8000)
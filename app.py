import csv
import io
import os
from datetime import date, datetime
from functools import wraps
from math import ceil

from flask import (
    Flask,
    Response,
    abort,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

from database.db import close_db, create_default_categories, get_db, init_db

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SPENDLY_SECRET_KEY", "dev-change-me")
app.config["PER_PAGE"] = 10
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("FLASK_ENV") == "production"

PAYMENT_METHODS = ["Card", "Cash", "Bank Transfer", "UPI", "Wallet", "Other"]
SORT_OPTIONS = {
    "date_desc": "e.spent_on DESC, e.id DESC",
    "date_asc": "e.spent_on ASC, e.id ASC",
    "amount_desc": "e.amount DESC, e.id DESC",
    "amount_asc": "e.amount ASC, e.id ASC",
}


def parse_iso_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_positive_amount(value):
    try:
        parsed = round(float(value), 2)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def parse_non_negative_amount(value):
    if value in (None, ""):
        return None
    try:
        parsed = round(float(value), 2)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def normalize_month_key(value):
    candidate = value or date.today().strftime("%Y-%m")
    try:
        return datetime.strptime(candidate, "%Y-%m").strftime("%Y-%m")
    except ValueError:
        return date.today().strftime("%Y-%m")


def month_start(month_key):
    return f"{month_key}-01"


def month_label(month_key):
    return datetime.strptime(month_key, "%Y-%m").strftime("%b %Y")


def last_n_month_keys(total_months):
    marker = date.today().replace(day=1)
    keys = []
    for _ in range(total_months):
        keys.append(marker.strftime("%Y-%m"))
        if marker.month == 1:
            marker = marker.replace(year=marker.year - 1, month=12)
        else:
            marker = marker.replace(month=marker.month - 1)
    keys.reverse()
    return keys


def login_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if g.user is None:
            flash("Please sign in to continue.", "warning")
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapped


@app.before_request
def load_logged_in_user():
    user_id = session.get("user_id")
    if user_id is None:
        g.user = None
        return
    g.user = get_db().execute(
        "SELECT id, name, email FROM users WHERE id = ?", (user_id,)
    ).fetchone()
    if g.user is None:
        session.clear()


@app.teardown_appcontext
def teardown_db(error):
    close_db(error)


@app.context_processor
def inject_globals():
    return {"current_user": g.get("user"), "today_iso": date.today().isoformat()}


def build_expense_filters(args, user_id, valid_category_ids):
    today = date.today()
    defaults = {
        "start_date": today.replace(day=1).isoformat(),
        "end_date": today.isoformat(),
        "category_id": "",
        "q": "",
        "min_amount": "",
        "max_amount": "",
        "sort": "date_desc",
        "month": today.strftime("%Y-%m"),
    }
    filters = {
        "start_date": (args.get("start_date") or defaults["start_date"]).strip(),
        "end_date": (args.get("end_date") or defaults["end_date"]).strip(),
        "category_id": (args.get("category_id") or "").strip(),
        "q": (args.get("q") or "").strip(),
        "min_amount": (args.get("min_amount") or "").strip(),
        "max_amount": (args.get("max_amount") or "").strip(),
        "sort": (args.get("sort") or defaults["sort"]).strip(),
        "month": normalize_month_key(args.get("month")),
    }
    notices = []

    start_obj = parse_iso_date(filters["start_date"])
    if start_obj is None:
        notices.append("Invalid start date. Reset to current month start.")
        filters["start_date"] = defaults["start_date"]
        start_obj = parse_iso_date(filters["start_date"])

    end_obj = parse_iso_date(filters["end_date"])
    if end_obj is None:
        notices.append("Invalid end date. Reset to today.")
        filters["end_date"] = defaults["end_date"]
        end_obj = parse_iso_date(filters["end_date"])

    if start_obj and end_obj and start_obj > end_obj:
        filters["start_date"], filters["end_date"] = filters["end_date"], filters["start_date"]
        start_obj, end_obj = end_obj, start_obj
        notices.append("Date range was reversed and has been corrected.")

    category_id = None
    if filters["category_id"]:
        try:
            parsed_category = int(filters["category_id"])
            if parsed_category in valid_category_ids:
                category_id = parsed_category
            else:
                filters["category_id"] = ""
        except ValueError:
            filters["category_id"] = ""

    min_amount = parse_non_negative_amount(filters["min_amount"])
    if filters["min_amount"] and min_amount is None:
        notices.append("Minimum amount is invalid and was ignored.")
        filters["min_amount"] = ""

    max_amount = parse_non_negative_amount(filters["max_amount"])
    if filters["max_amount"] and max_amount is None:
        notices.append("Maximum amount is invalid and was ignored.")
        filters["max_amount"] = ""

    if min_amount is not None and max_amount is not None and min_amount > max_amount:
        min_amount, max_amount = max_amount, min_amount
        filters["min_amount"] = str(min_amount)
        filters["max_amount"] = str(max_amount)
        notices.append("Min/Max values were swapped because they were reversed.")

    if filters["sort"] not in SORT_OPTIONS:
        filters["sort"] = defaults["sort"]

    where = ["e.user_id = :user_id"]
    params = {"user_id": user_id}

    if filters["start_date"]:
        where.append("e.spent_on >= :start_date")
        params["start_date"] = filters["start_date"]
    if filters["end_date"]:
        where.append("e.spent_on <= :end_date")
        params["end_date"] = filters["end_date"]
    if category_id is not None:
        where.append("e.category_id = :category_id")
        params["category_id"] = category_id
    if filters["q"]:
        where.append("(LOWER(COALESCE(e.note, '')) LIKE :search OR LOWER(c.name) LIKE :search)")
        params["search"] = f"%{filters['q'].lower()}%"
    if min_amount is not None:
        where.append("e.amount >= :min_amount")
        params["min_amount"] = min_amount
    if max_amount is not None:
        where.append("e.amount <= :max_amount")
        params["max_amount"] = max_amount

    return {
        "where_sql": " AND ".join(where),
        "params": params,
        "filters": filters,
        "start_obj": start_obj,
        "end_obj": end_obj,
        "notices": notices,
    }


@app.route("/")
def landing():
    if g.user is not None:
        return redirect(url_for("dashboard"))
    return render_template("landing.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if g.user is not None:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        if len(name) < 2:
            flash("Please enter your full name.", "danger")
            return render_template("register.html")
        if "@" not in email or len(email) < 5:
            flash("Please enter a valid email address.", "danger")
            return render_template("register.html")
        if len(password) < 8:
            flash("Password must be at least 8 characters long.", "danger")
            return render_template("register.html")

        db = get_db()
        existing = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if existing is not None:
            flash("An account already exists with this email.", "danger")
            return render_template("register.html")

        cursor = db.execute(
            """
            INSERT INTO users (name, email, password_hash)
            VALUES (?, ?, ?)
            """,
            (name, email, generate_password_hash(password)),
        )
        db.commit()
        create_default_categories(db, cursor.lastrowid)
        session["user_id"] = cursor.lastrowid
        flash("Welcome to Spendly. Your account is ready.", "success")
        return redirect(url_for("dashboard"))

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if g.user is not None:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        user = get_db().execute(
            "SELECT id, name, email, password_hash FROM users WHERE email = ?",
            (email,),
        ).fetchone()
        if user is None or not check_password_hash(user["password_hash"], password):
            flash("Invalid email or password.", "danger")
            return render_template("login.html")

        session.clear()
        session["user_id"] = user["id"]
        flash(f"Welcome back, {user['name'].split(' ')[0]}.", "success")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("landing"))


@app.route("/dashboard")
@login_required
def dashboard():
    db = get_db()
    categories = db.execute(
        "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name",
        (g.user["id"],),
    ).fetchall()
    valid_category_ids = {row["id"] for row in categories}

    filter_bundle = build_expense_filters(request.args, g.user["id"], valid_category_ids)
    for notice in filter_bundle["notices"]:
        flash(notice, "warning")

    base_params = dict(filter_bundle["params"])
    total_rows = db.execute(
        f"""
        SELECT COUNT(*) AS row_count
        FROM expenses e
        JOIN categories c ON c.id = e.category_id
        WHERE {filter_bundle['where_sql']}
        """,
        base_params,
    ).fetchone()["row_count"]

    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1
    page = max(page, 1)
    per_page = app.config["PER_PAGE"]
    total_pages = max(1, ceil(total_rows / per_page)) if total_rows else 1
    page = min(page, total_pages)
    offset = (page - 1) * per_page

    expense_params = dict(base_params)
    expense_params.update({"limit": per_page, "offset": offset})
    expenses = db.execute(
        f"""
        SELECT
            e.id,
            e.amount,
            e.note,
            e.payment_method,
            e.spent_on,
            c.id AS category_id,
            c.name AS category_name
        FROM expenses e
        JOIN categories c ON c.id = e.category_id
        WHERE {filter_bundle['where_sql']}
        ORDER BY {SORT_OPTIONS[filter_bundle['filters']['sort']]}
        LIMIT :limit OFFSET :offset
        """,
        expense_params,
    ).fetchall()

    summary = db.execute(
        f"""
        SELECT
            COALESCE(SUM(e.amount), 0) AS total_spend,
            COALESCE(AVG(e.amount), 0) AS avg_transaction,
            COUNT(*) AS transaction_count
        FROM expenses e
        JOIN categories c ON c.id = e.category_id
        WHERE {filter_bundle['where_sql']}
        """,
        base_params,
    ).fetchone()

    days_span = 1
    if filter_bundle["start_obj"] and filter_bundle["end_obj"]:
        days_span = max((filter_bundle["end_obj"] - filter_bundle["start_obj"]).days + 1, 1)
    avg_daily = summary["total_spend"] / days_span if days_span else 0

    category_rows = db.execute(
        f"""
        SELECT
            c.name AS category_name,
            SUM(e.amount) AS total
        FROM expenses e
        JOIN categories c ON c.id = e.category_id
        WHERE {filter_bundle['where_sql']}
        GROUP BY c.id, c.name
        ORDER BY total DESC
        """,
        base_params,
    ).fetchall()
    category_breakdown = []
    total_spend = float(summary["total_spend"] or 0)
    for row in category_rows:
        category_breakdown.append(
            {
                "category_name": row["category_name"],
                "total": float(row["total"] or 0),
                "percent": (float(row["total"] or 0) / total_spend * 100) if total_spend else 0,
            }
        )

    trend_month_keys = last_n_month_keys(6)
    trend_start = f"{trend_month_keys[0]}-01"
    trend_rows = db.execute(
        """
        SELECT strftime('%Y-%m', spent_on) AS month_key, SUM(amount) AS total
        FROM expenses
        WHERE user_id = ? AND spent_on >= ?
        GROUP BY month_key
        ORDER BY month_key
        """,
        (g.user["id"], trend_start),
    ).fetchall()
    trend_totals = {row["month_key"]: float(row["total"] or 0) for row in trend_rows}
    trend_max = max([trend_totals.get(month_key, 0) for month_key in trend_month_keys], default=0)
    trend_points = []
    for month_key in trend_month_keys:
        value = trend_totals.get(month_key, 0)
        bar_height = 12 if trend_max == 0 else max((value / trend_max) * 100, 8)
        trend_points.append(
            {
                "month_key": month_key,
                "label": datetime.strptime(month_key, "%Y-%m").strftime("%b"),
                "value": value,
                "bar_height": bar_height,
            }
        )

    selected_month = filter_bundle["filters"]["month"]
    selected_month_start = month_start(selected_month)
    budget_rows = db.execute(
        """
        SELECT
            b.id,
            b.category_id,
            c.name AS category_name,
            b.amount AS budget_amount,
            COALESCE(SUM(e.amount), 0) AS spent_amount
        FROM budgets b
        JOIN categories c ON c.id = b.category_id
        LEFT JOIN expenses e
            ON e.user_id = b.user_id
            AND e.category_id = b.category_id
            AND strftime('%Y-%m', e.spent_on) = :month_key
        WHERE b.user_id = :user_id
            AND b.month = :month_start
        GROUP BY b.id, b.category_id, c.name, b.amount
        ORDER BY c.name
        """,
        {
            "month_key": selected_month,
            "month_start": selected_month_start,
            "user_id": g.user["id"],
        },
    ).fetchall()

    budget_overview = []
    total_budget = 0.0
    for row in budget_rows:
        budget_amount = float(row["budget_amount"] or 0)
        spent_amount = float(row["spent_amount"] or 0)
        total_budget += budget_amount
        budget_overview.append(
            {
                "id": row["id"],
                "category_name": row["category_name"],
                "budget_amount": budget_amount,
                "spent_amount": spent_amount,
                "remaining": budget_amount - spent_amount,
                "progress": (spent_amount / budget_amount * 100) if budget_amount else 0,
            }
        )

    month_spend_row = db.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS month_spend
        FROM expenses
        WHERE user_id = ? AND strftime('%Y-%m', spent_on) = ?
        """,
        (g.user["id"], selected_month),
    ).fetchone()
    month_spend = float(month_spend_row["month_spend"] or 0)
    remaining_budget = total_budget - month_spend if total_budget else 0

    return render_template(
        "dashboard.html",
        categories=categories,
        expenses=expenses,
        summary=summary,
        avg_daily=avg_daily,
        category_breakdown=category_breakdown,
        trend_points=trend_points,
        budget_overview=budget_overview,
        month_spend=month_spend,
        remaining_budget=remaining_budget,
        selected_month=selected_month,
        selected_month_label=month_label(selected_month),
        total_budget=total_budget,
        dashboard_filters=filter_bundle["filters"],
        page=page,
        total_pages=total_pages,
        total_rows=total_rows,
    )


@app.route("/expenses/add", methods=["GET", "POST"])
@login_required
def add_expense():
    db = get_db()
    categories = db.execute(
        "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name",
        (g.user["id"],),
    ).fetchall()
    valid_category_ids = {row["id"] for row in categories}

    expense_form = {
        "amount": "",
        "note": "",
        "category_id": "",
        "payment_method": "Card",
        "spent_on": date.today().isoformat(),
    }

    if request.method == "POST":
        expense_form["amount"] = (request.form.get("amount") or "").strip()
        expense_form["note"] = (request.form.get("note") or "").strip()
        expense_form["category_id"] = (request.form.get("category_id") or "").strip()
        expense_form["payment_method"] = (request.form.get("payment_method") or "Card").strip()
        expense_form["spent_on"] = (request.form.get("spent_on") or "").strip()

        amount = parse_positive_amount(expense_form["amount"])
        spent_on = parse_iso_date(expense_form["spent_on"])
        errors = []

        try:
            category_id = int(expense_form["category_id"])
            if category_id not in valid_category_ids:
                errors.append("Please choose a valid category.")
        except ValueError:
            category_id = None
            errors.append("Category is required.")

        if amount is None:
            errors.append("Amount must be a number greater than zero.")
        if spent_on is None:
            errors.append("Please provide a valid date.")
        if expense_form["payment_method"] not in PAYMENT_METHODS:
            errors.append("Please choose a valid payment method.")
        if len(expense_form["note"]) > 180:
            errors.append("Note is too long. Keep it within 180 characters.")

        if errors:
            for error in errors:
                flash(error, "danger")
            return render_template(
                "expense_form.html",
                mode="create",
                categories=categories,
                payment_methods=PAYMENT_METHODS,
                expense=expense_form,
            )

        db.execute(
            """
            INSERT INTO expenses (user_id, category_id, amount, note, payment_method, spent_on)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                g.user["id"],
                category_id,
                amount,
                expense_form["note"] or None,
                expense_form["payment_method"],
                spent_on.isoformat(),
            ),
        )
        db.commit()
        flash("Expense added successfully.", "success")
        return redirect(url_for("dashboard"))

    return render_template(
        "expense_form.html",
        mode="create",
        categories=categories,
        payment_methods=PAYMENT_METHODS,
        expense=expense_form,
    )


@app.route("/expenses/<int:expense_id>/edit", methods=["GET", "POST"])
@login_required
def edit_expense(expense_id):
    db = get_db()
    categories = db.execute(
        "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name",
        (g.user["id"],),
    ).fetchall()
    valid_category_ids = {row["id"] for row in categories}

    expense_row = db.execute(
        """
        SELECT id, category_id, amount, note, payment_method, spent_on
        FROM expenses
        WHERE id = ? AND user_id = ?
        """,
        (expense_id, g.user["id"]),
    ).fetchone()
    if expense_row is None:
        abort(404)

    expense_form = {
        "amount": f"{float(expense_row['amount']):.2f}",
        "note": expense_row["note"] or "",
        "category_id": str(expense_row["category_id"]),
        "payment_method": expense_row["payment_method"],
        "spent_on": expense_row["spent_on"],
    }

    if request.method == "POST":
        expense_form["amount"] = (request.form.get("amount") or "").strip()
        expense_form["note"] = (request.form.get("note") or "").strip()
        expense_form["category_id"] = (request.form.get("category_id") or "").strip()
        expense_form["payment_method"] = (request.form.get("payment_method") or "Card").strip()
        expense_form["spent_on"] = (request.form.get("spent_on") or "").strip()

        amount = parse_positive_amount(expense_form["amount"])
        spent_on = parse_iso_date(expense_form["spent_on"])
        errors = []

        try:
            category_id = int(expense_form["category_id"])
            if category_id not in valid_category_ids:
                errors.append("Please choose a valid category.")
        except ValueError:
            category_id = None
            errors.append("Category is required.")

        if amount is None:
            errors.append("Amount must be a number greater than zero.")
        if spent_on is None:
            errors.append("Please provide a valid date.")
        if expense_form["payment_method"] not in PAYMENT_METHODS:
            errors.append("Please choose a valid payment method.")
        if len(expense_form["note"]) > 180:
            errors.append("Note is too long. Keep it within 180 characters.")

        if errors:
            for error in errors:
                flash(error, "danger")
            return render_template(
                "expense_form.html",
                mode="edit",
                categories=categories,
                payment_methods=PAYMENT_METHODS,
                expense=expense_form,
            )

        db.execute(
            """
            UPDATE expenses
            SET category_id = ?, amount = ?, note = ?, payment_method = ?,
                spent_on = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND user_id = ?
            """,
            (
                category_id,
                amount,
                expense_form["note"] or None,
                expense_form["payment_method"],
                spent_on.isoformat(),
                expense_id,
                g.user["id"],
            ),
        )
        db.commit()
        flash("Expense updated successfully.", "success")
        return redirect(url_for("dashboard"))

    return render_template(
        "expense_form.html",
        mode="edit",
        categories=categories,
        payment_methods=PAYMENT_METHODS,
        expense=expense_form,
    )


@app.post("/expenses/<int:expense_id>/delete")
@login_required
def delete_expense(expense_id):
    db = get_db()
    cursor = db.execute(
        "DELETE FROM expenses WHERE id = ? AND user_id = ?",
        (expense_id, g.user["id"]),
    )
    db.commit()
    if cursor.rowcount > 0:
        flash("Expense deleted.", "success")
    else:
        flash("Expense not found.", "warning")

    next_url = request.form.get("next") or ""
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("dashboard"))


@app.route("/budgets", methods=["GET", "POST"])
@login_required
def budgets():
    db = get_db()
    selected_month = normalize_month_key(request.values.get("month"))
    selected_month_start = month_start(selected_month)

    categories = db.execute(
        "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name",
        (g.user["id"],),
    ).fetchall()
    valid_category_ids = {row["id"] for row in categories}

    if request.method == "POST":
        category_raw = (request.form.get("category_id") or "").strip()
        amount_raw = (request.form.get("amount") or "").strip()
        selected_month = normalize_month_key(request.form.get("month"))
        selected_month_start = month_start(selected_month)

        try:
            category_id = int(category_raw)
            if category_id not in valid_category_ids:
                raise ValueError
        except ValueError:
            flash("Please select a valid category.", "danger")
            return redirect(url_for("budgets", month=selected_month))

        budget_amount = parse_positive_amount(amount_raw)
        if budget_amount is None:
            flash("Budget amount must be greater than zero.", "danger")
            return redirect(url_for("budgets", month=selected_month))

        db.execute(
            """
            INSERT INTO budgets (user_id, category_id, month, amount)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, category_id, month)
            DO UPDATE SET amount = excluded.amount, updated_at = CURRENT_TIMESTAMP
            """,
            (g.user["id"], category_id, selected_month_start, budget_amount),
        )
        db.commit()
        flash("Budget saved.", "success")
        return redirect(url_for("budgets", month=selected_month))

    budget_rows = db.execute(
        """
        SELECT
            b.id,
            b.category_id,
            c.name AS category_name,
            b.amount AS budget_amount,
            COALESCE(SUM(e.amount), 0) AS spent_amount
        FROM budgets b
        JOIN categories c ON c.id = b.category_id
        LEFT JOIN expenses e
            ON e.user_id = b.user_id
            AND e.category_id = b.category_id
            AND strftime('%Y-%m', e.spent_on) = :month_key
        WHERE b.user_id = :user_id
            AND b.month = :month_start
        GROUP BY b.id, b.category_id, c.name, b.amount
        ORDER BY c.name
        """,
        {
            "user_id": g.user["id"],
            "month_key": selected_month,
            "month_start": selected_month_start,
        },
    ).fetchall()

    return render_template(
        "budgets.html",
        categories=categories,
        budget_rows=budget_rows,
        selected_month=selected_month,
        selected_month_label=month_label(selected_month),
    )


@app.post("/budgets/<int:budget_id>/delete")
@login_required
def delete_budget(budget_id):
    db = get_db()
    month_key = normalize_month_key(request.form.get("month"))
    cursor = db.execute(
        "DELETE FROM budgets WHERE id = ? AND user_id = ?",
        (budget_id, g.user["id"]),
    )
    db.commit()
    if cursor.rowcount > 0:
        flash("Budget deleted.", "success")
    else:
        flash("Budget not found.", "warning")
    return redirect(url_for("budgets", month=month_key))


@app.route("/reports/export.csv")
@login_required
def export_expenses_csv():
    db = get_db()
    categories = db.execute(
        "SELECT id FROM categories WHERE user_id = ?",
        (g.user["id"],),
    ).fetchall()
    valid_category_ids = {row["id"] for row in categories}
    filter_bundle = build_expense_filters(request.args, g.user["id"], valid_category_ids)

    rows = db.execute(
        f"""
        SELECT
            e.spent_on,
            c.name AS category_name,
            e.amount,
            e.payment_method,
            COALESCE(e.note, '') AS note
        FROM expenses e
        JOIN categories c ON c.id = e.category_id
        WHERE {filter_bundle['where_sql']}
        ORDER BY {SORT_OPTIONS[filter_bundle['filters']['sort']]}
        """,
        filter_bundle["params"],
    ).fetchall()

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["Date", "Category", "Amount", "Payment Method", "Note"])
    for row in rows:
        writer.writerow(
            [
                row["spent_on"],
                row["category_name"],
                f"{float(row['amount']):.2f}",
                row["payment_method"],
                row["note"],
            ]
        )

    month_stamp = datetime.now().strftime("%Y%m%d")
    return Response(
        buffer.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=spendly-expenses-{month_stamp}.csv"},
    )


@app.route("/profile")
@login_required
def profile():
    db = get_db()
    summary = db.execute(
        """
        SELECT
            COALESCE(SUM(amount), 0) AS total_spend,
            COALESCE(AVG(amount), 0) AS avg_transaction,
            COUNT(*) AS transaction_count
        FROM expenses
        WHERE user_id = ?
        """,
        (g.user["id"],),
    ).fetchone()

    avg_monthly_row = db.execute(
        """
        SELECT COALESCE(AVG(month_total), 0) AS avg_monthly
        FROM (
            SELECT SUM(amount) AS month_total
            FROM expenses
            WHERE user_id = ?
            GROUP BY strftime('%Y-%m', spent_on)
        )
        """,
        (g.user["id"],),
    ).fetchone()

    top_categories = db.execute(
        """
        SELECT c.name AS category_name, SUM(e.amount) AS total
        FROM expenses e
        JOIN categories c ON c.id = e.category_id
        WHERE e.user_id = ?
        GROUP BY c.id, c.name
        ORDER BY total DESC
        LIMIT 5
        """,
        (g.user["id"],),
    ).fetchall()

    recent_expenses = db.execute(
        """
        SELECT e.spent_on, e.amount, COALESCE(e.note, '') AS note, c.name AS category_name
        FROM expenses e
        JOIN categories c ON c.id = e.category_id
        WHERE e.user_id = ?
        ORDER BY e.spent_on DESC, e.id DESC
        LIMIT 8
        """,
        (g.user["id"],),
    ).fetchall()

    return render_template(
        "profile.html",
        summary=summary,
        avg_monthly=float(avg_monthly_row["avg_monthly"] or 0),
        top_categories=top_categories,
        recent_expenses=recent_expenses,
    )


@app.route("/api/insights/monthly")
@login_required
def monthly_insights_api():
    db = get_db()
    months = last_n_month_keys(12)
    start_month = f"{months[0]}-01"
    rows = db.execute(
        """
        SELECT strftime('%Y-%m', spent_on) AS month_key, SUM(amount) AS total
        FROM expenses
        WHERE user_id = ? AND spent_on >= ?
        GROUP BY month_key
        ORDER BY month_key
        """,
        (g.user["id"], start_month),
    ).fetchall()
    totals = {row["month_key"]: float(row["total"] or 0) for row in rows}
    data = [
        {
            "month_key": month_key,
            "label": datetime.strptime(month_key, "%Y-%m").strftime("%b %Y"),
            "total": totals.get(month_key, 0),
        }
        for month_key in months
    ]
    return jsonify({"data": data})


@app.errorhandler(404)
def not_found(_error):
    return render_template("not_found.html"), 404


init_db()


if __name__ == "__main__":
    debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=debug_mode)

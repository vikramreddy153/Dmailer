import os
import time
import threading
import pandas as pd
from pandas.errors import EmptyDataError
import yagmail
from datetime import datetime
from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, send_file, send_from_directory, jsonify
)
from werkzeug.utils import secure_filename
import csv

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'super-secret')  # Use env var in production!

UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
LOG_FILE = os.path.join(UPLOAD_FOLDER, "sent_log.csv")

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {"pdf", "doc", "docx"}
LAST_PROGRESS = {"total": 0}

# Use environment variables for Gmail credentials
GMAIL_USER = os.environ.get("GMAIL_USER")
APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def normalize_columns(df):
    df.columns = [col.strip().lower() for col in df.columns]
    return df

def read_safe_csv(fileobj):
    df = pd.read_csv(fileobj)
    df = normalize_columns(df)
    return df

@app.errorhandler(404)
def not_found_error(error):
    return render_template('error.html', error=error), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error=error), 500

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)

@app.route('/download_log')
def download_log():
    if os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, as_attachment=True)
    flash("No log found.", "warning")
    return redirect(url_for("index"))

@app.route('/get_logs')
def get_logs():
    if os.path.exists(LOG_FILE):
        try:
            df = pd.read_csv(LOG_FILE)
            if 'Error' in df.columns:
                df['Error'] = df['Error'].fillna('')
            logs = df.to_dict(orient="records")
            success = sum(df["Status"] == "Sent")
            failed = sum(df["Status"] != "Sent")
            total = LAST_PROGRESS.get("total", 0)
            return jsonify(logs=logs, success=success, failed=failed, total=total)
        except EmptyDataError:
            total = LAST_PROGRESS.get("total", 0)
            return jsonify(logs=[], success=0, failed=0, total=total)
    else:
        total = LAST_PROGRESS.get("total", 0)
    return jsonify(logs=[], success=0, failed=0, total=total)

@app.route('/preview', methods=['POST'])
def preview():
    subject_template = request.form.get("subject_template", "")
    email_template = request.form.get("email_template", "")
    sample = {
        "name": request.form.get("your_name", "John Doe"),
        "email": request.form.get("your_email", "john@example.com"),
        "company": "Example Inc",
        "your_name": request.form.get("your_name", "John Doe"),
        "your_email": request.form.get("your_email", "john@example.com"),
        "your_mobile": request.form.get("your_mobile", "1234567890"),
        "your_linkedin": request.form.get("your_linkedin", "https://linkedin.com"),
        "your_github": request.form.get("your_github", "https://github.com"),
    }
    subject = subject_template.format(**sample)
    body = email_template.format(**sample).replace("\n", "<br>")
    return render_template('preview.html', preview_html=f'<b>Subject:</b> {subject}<hr><b>Body:</b><br>{body}')

def send_bulk_emails(data, subject_template, html_template, resume_path, user_info, delay_seconds, scheduled_time):
    def send_job():
        try:
            wait_time = max((scheduled_time - datetime.now()).total_seconds(), 0)
            if wait_time > 0:
                print(f"⏳ Waiting {wait_time:.1f}s before starting email sending...")
                time.sleep(wait_time)
            yag = yagmail.SMTP(user_info.get("your_email") or GMAIL_USER, user_info.get("app_password") or APP_PASSWORD)
        except Exception as e:
            print(f"❌ Gmail connection failed: {e}")
            with open(LOG_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=["email", "Status", "Error"])
                writer.writeheader()
                for _, row in data.iterrows():
                    writer.writerow({"email": row.get('email', ''), "Status": "Failed", "Error": "Gmail login failed"})
            return

        with open(LOG_FILE, 'w', newline='', encoding='utf-8') as f:
            fieldnames = list(data.columns) + ["Status", "Error"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for _, row in data.iterrows():
                try:
                    merged = {**row.to_dict(), **user_info}
                    subject = subject_template.format(**merged)
                    body = html_template.format(**merged)
                    yag.send(
                        to=row["email"],
                        subject=subject,
                        contents=[body],
                        attachments=[resume_path]
                    )
                    print(f"✅ Sent: {row['email']}")
                    status = "Sent"
                    error_str = ""
                except Exception as err:
                    error_str = str(err) if err else ""
                    print(f"❌ Failed to {row.get('email', '')}: {error_str}")
                    status = "Failed"

                log_row = dict(row)
                log_row.update({"Status": status, "Error": error_str})
                writer.writerow(log_row)
                f.flush()
                time.sleep(delay_seconds)

        print("✅ All emails processed.")

    threading.Thread(target=send_job, daemon=True).start()

@app.route('/')
def home_redirect():
    return redirect(url_for('home'))

@app.route('/home')
def home():
    current_year = datetime.now().year
    return render_template('home.html', current_year=current_year)

@app.route('/contact', methods=['POST'])
def contact():
    name = request.form.get("name")
    email = request.form.get("email")
    message = request.form.get("message")

    try:
        yag = yagmail.SMTP(GMAIL_USER, APP_PASSWORD)

        # Combine all data into one formatted string
        email_body = f"""Name: {name}
Email: {email}

Message:
{message}
"""
        yag.send(
            to=GMAIL_USER,
            subject=f"Dmailer Contact Form: Message from {name}",
            contents=email_body,
        )
        flash("✅ Your message was sent successfully! I'll get back to you soon.", "success")
    except Exception as e:
        flash(f"❌ Failed to send message: {str(e)}", "danger")

    return redirect(url_for('home'))

@app.route('/index', methods=['GET', 'POST'])
def index():
    uploaded_resume = None
    current_year = datetime.now().year

    if request.method == 'POST':
        try:
            your_info = {
                "your_name": request.form.get("your_name"),
                "your_email": request.form.get("your_email"),
                "your_mobile": request.form.get("your_mobile"),
                "your_linkedin": request.form.get("your_linkedin"),
                "your_github": request.form.get("your_github"),
                "app_password": request.form.get("app_password")
            }
            subject_template = request.form.get("subject_template", "")
            email_template = request.form.get("email_template", "")
            delay = float(request.form.get("delay", 2))
            send_at = request.form.get("send_time")
            scheduled_time = datetime.strptime(send_at, "%Y-%m-%dT%H:%M") if send_at else datetime.now()

            csv_file = request.files.get("csv_file")
            resume_file = request.files.get("resume")

            if not csv_file or not resume_file:
                flash("⚠️ CSV and Resume file are required.", "danger")
                return redirect(url_for("index"))

            if not allowed_file(resume_file.filename):
                flash("❌ Invalid resume format. Only PDF, DOC, DOCX allowed.", "danger")
                return redirect(url_for("index"))

            # Clear uploads except log file
            for fname in os.listdir(app.config["UPLOAD_FOLDER"]):
                if fname != "sent_log.csv":
                    try:
                        os.remove(os.path.join(app.config["UPLOAD_FOLDER"], fname))
                    except Exception:
                        pass

            resume_filename = secure_filename(resume_file.filename)
            resume_path = os.path.join(app.config["UPLOAD_FOLDER"], resume_filename)
            resume_file.save(resume_path)
            uploaded_resume = resume_filename

            df = read_safe_csv(csv_file)

            required_cols = {'name', 'email', 'company'}
            if not required_cols.issubset(set(df.columns)):
                flash("❌ CSV must contain columns: name, email, company (case-insensitive).", "danger")
                return redirect(url_for("index"))

            if os.path.exists(LOG_FILE):
                os.remove(LOG_FILE)

            LAST_PROGRESS['total'] = len(df)

            send_bulk_emails(
                df, subject_template, email_template,
                resume_path, your_info, delay, scheduled_time
            )

            flash("✅ Emails are scheduled and will begin soon!", "success")
            return render_template("index.html", sent=True, uploaded_resume=uploaded_resume, total=len(df), current_year=current_year)

        except Exception as e:
            flash(f"❌ Error: {e}", "danger")
            return redirect(url_for("index"))

    return render_template("index.html", sent=False, uploaded_resume=uploaded_resume, total=0, current_year=current_year)


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))  # Use Render or hosting platform port
    app.run(host='0.0.0.0', port=port)

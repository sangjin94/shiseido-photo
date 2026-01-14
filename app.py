@'
import os
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import boto3
from botocore.exceptions import ClientError

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, abort, send_file
)
from werkzeug.utils import secure_filename


# =========================
# 경로 설정 (시셰이도 폴더 기준)
# =========================
BASE_DIR = Path(__file__).resolve().parent

DISK_PATH = os.environ.get("DISK_PATH", "").strip()
ROOT_DIR = Path(DISK_PATH) if DISK_PATH else BASE_DIR

DATA_DIR = ROOT_DIR / "data"
UPLOAD_DIR = ROOT_DIR / "static" / "uploads"
DB_PATH = DATA_DIR / "products.db"
MASTER_XLSX_PATH = DATA_DIR / "products_master.xlsx"  # 너가 넣을 파일

DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}


# =========================
# Flask
# =========================
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-this-secret")


# =========================
# S3 옵션 (환경변수 있으면 자동 사용)
# =========================
S3_BUCKET = os.environ.get("S3_BUCKET", "").strip()
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2").strip()
S3_PREFIX = os.environ.get("S3_PREFIX", "uploads").strip().strip("/")
USE_S3 = bool(S3_BUCKET)


def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def s3_key_for(product_id: int, filename: str) -> str:
    return f"{S3_PREFIX}/{product_id}/{filename}"


def s3_upload_fileobj(fileobj, key: str, content_type: str | None = None):
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    s3_client().upload_fileobj(fileobj, S3_BUCKET, key, ExtraArgs=extra)


def s3_delete_object(key: str):
    try:
        s3_client().delete_object(Bucket=S3_BUCKET, Key=key)
    except ClientError:
        pass


def s3_presigned_get(key: str, expires_seconds: int = 3600) -> str:
    try:
        return s3_client().generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": key},
            ExpiresIn=expires_seconds,
        )
    except ClientError:
        return ""


# =========================
# DB
# =========================
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT,
            barcode TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            manufacturer TEXT,
            description TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_product_code ON products(product_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            storage TEXT NOT NULL DEFAULT 'local',   -- local | s3
            filename TEXT,
            s3_key TEXT,
            original_name TEXT,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
        )
        """
    )

    conn.commit()
    conn.close()


# =========================
# 마스터 엑셀 -> DB 반영
# =========================
def upsert_products_from_master_excel():
    if not MASTER_XLSX_PATH.exists():
        print(f"[WARN] 마스터 엑셀 없음: {MASTER_XLSX_PATH}")
        return

    df = pd.read_excel(MASTER_XLSX_PATH, dtype=str)
    df.columns = [str(c).strip().lower() for c in df.columns]

    for col in ["product_code", "barcode", "name", "manufacturer", "description"]:
        if col not in df.columns:
            df[col] = ""

    for col in ["product_code", "barcode", "name", "manufacturer", "description"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df = df[(df["barcode"] != "") & (df["name"] != "")].copy()
    df = df.drop_duplicates(subset=["barcode"], keep="last")

    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().isoformat(timespec="seconds")

    inserted = 0
    updated = 0

    for _, r in df.iterrows():
        product_code = r["product_code"] or None
        barcode = r["barcode"]
        name = r["name"]
        manufacturer = r["manufacturer"] or None
        description = r["description"] or None

        cur.execute("SELECT id FROM products WHERE barcode=?", (barcode,))
        exists = cur.fetchone()

        if exists:
            cur.execute(
                """
                UPDATE products
                   SET product_code=?,
                       name=?,
                       manufacturer=?,
                       description=?,
                       updated_at=?
                 WHERE barcode=?
                """,
                (product_code, name, manufacturer, description, now, barcode),
            )
            updated += 1
        else:
            cur.execute(
                """
                INSERT INTO products (product_code, barcode, name, manufacturer, description, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (product_code, barcode, name, manufacturer, description, now, now),
            )
            inserted += 1

    conn.commit()
    conn.close()
    print(f"[MASTER] inserted={inserted}, updated={updated}, total={len(df)}")


# =========================
# 업로드 저장
# =========================
def allowed_file(filename: str) -> bool:
    if "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_EXTENSIONS


def local_product_folder(product_id: int) -> Path:
    folder = UPLOAD_DIR / str(product_id)
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def save_photo(product_id: int, file_storage) -> tuple[str, str, str]:
    """
    return: (storage, filename, s3_key)
    """
    original_name = file_storage.filename or "photo"
    safe = secure_filename(original_name)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    final_name = f"{ts}_{uuid.uuid4().hex}__{safe}"

    if USE_S3:
        key = s3_key_for(product_id, final_name)
        file_storage.stream.seek(0)
        content_type = getattr(file_storage, "mimetype", None)
        s3_upload_fileobj(file_storage.stream, key, content_type=content_type)
        return ("s3", final_name, key)

    # local
    folder = local_product_folder(product_id)
    file_storage.save(folder / final_name)
    return ("local", final_name, "")


# =========================
# Routes
# =========================
@app.route("/", methods=["GET"])
def home():
    q = request.args.get("q", "").strip()

    conn = get_db()
    cur = conn.cursor()

    if q:
        cur.execute(
            """
            SELECT * FROM products
             WHERE barcode LIKE ?
                OR IFNULL(product_code,'') LIKE ?
                OR name LIKE ?
             ORDER BY id DESC
             LIMIT 50
            """,
            (f"%{q}%", f"%{q}%", f"%{q}%"),
        )
    else:
        cur.execute("SELECT * FROM products ORDER BY id DESC LIMIT 50")

    products = cur.fetchall()

    cur.execute(
        """
        SELECT COUNT(DISTINCT p.id) AS cnt
          FROM products p
          JOIN photos ph ON ph.product_id = p.id
        """
    )
    registered_count = cur.fetchone()["cnt"]
    conn.close()

    return render_template(
        "home.html",
        q=q,
        products=products,
        registered_count=registered_count,
        use_s3=USE_S3
    )


@app.route("/scan", methods=["GET"])
def scan():
    return render_template("scan.html")


@app.route("/products/lookup", methods=["GET"])
def product_lookup():
    code = request.args.get("code", "").strip()
    if not code:
        return redirect(url_for("home"))

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM products
         WHERE barcode = ?
            OR product_code = ?
        """,
        (code, code),
    )
    product = cur.fetchone()
    conn.close()

    if not product:
        flash("마스터에 없는 코드(상품코드/바코드)입니다.", "warning")
        return redirect(url_for("home", q=code))

    return redirect(url_for("product_detail", product_id=product["id"]))


@app.route("/products/<int:product_id>", methods=["GET"])
def product_detail(product_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM products WHERE id=?", (product_id,))
    product = cur.fetchone()
    if not product:
        conn.close()
        abort(404)

    cur.execute("SELECT * FROM photos WHERE product_id=? ORDER BY id DESC", (product_id,))
    photos = cur.fetchall()
    conn.close()

    return render_template("product_detail.html", product=product, photos=photos, use_s3=USE_S3)


@app.route("/products/<int:product_id>/upload", methods=["POST"])
def upload_photos(product_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM products WHERE id=?", (product_id,))
    if not cur.fetchone():
        conn.close()
        abort(404)

    files = request.files.getlist("photos")
    if not files:
        conn.close()
        flash("업로드할 사진을 선택해 주세요.", "warning")
        return redirect(url_for("product_detail", product_id=product_id))

    saved = 0
    now = datetime.now().isoformat(timespec="seconds")

    for f in files:
        if not f or not f.filename:
            continue
        if not allowed_file(f.filename):
            continue

        storage, filename, key = save_photo(product_id, f)
        cur.execute(
            """
            INSERT INTO photos (product_id, storage, filename, s3_key, original_name, uploaded_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (product_id, storage, filename, key, f.filename, now),
        )
        saved += 1

    conn.commit()
    conn.close()

    if saved == 0:
        flash("업로드 가능한 이미지가 없습니다.(jpg/png/webp/gif)", "danger")
    else:
        flash(f"사진 {saved}장을 업로드했습니다.", "success")

    return redirect(url_for("product_detail", product_id=product_id))


@app.route("/photos/<int:photo_id>/url", methods=["GET"])
def photo_url(photo_id):
    """
    이미지 표시:
    - S3: presigned URL로 리다이렉트
    - local: 로컬 파일 URL로 서빙 (static)
    """
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM photos WHERE id=?", (photo_id,))
    ph = cur.fetchone()
    conn.close()

    if not ph:
        abort(404)

    if ph["storage"] == "s3":
        url = s3_presigned_get(ph["s3_key"], expires_seconds=3600)
        if not url:
            abort(500)
        return redirect(url)

    # local은 static/uploads/{product_id}/{filename} 형태
    return redirect(url_for("static", filename=f"uploads/{ph['product_id']}/{ph['filename']}"))


@app.route("/photos/<int:photo_id>/delete", methods=["POST"])
def delete_photo(photo_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM photos WHERE id=?", (photo_id,))
    ph = cur.fetchone()
    if not ph:
        conn.close()
        abort(404)

    cur.execute("DELETE FROM photos WHERE id=?", (photo_id,))
    conn.commit()
    conn.close()

    if ph["storage"] == "s3":
        if ph["s3_key"]:
            s3_delete_object(ph["s3_key"])
    else:
        try:
            p = UPLOAD_DIR / str(ph["product_id"]) / ph["filename"]
            if p.exists():
                p.unlink()
        except Exception:
            pass

    flash("사진이 삭제되었습니다.", "success")
    return redirect(url_for("product_detail", product_id=ph["product_id"]))


@app.route("/download/registered.xlsx", methods=["GET"])
def download_registered_list_xlsx():
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            p.product_code,
            p.barcode,
            p.name,
            p.manufacturer,
            p.description,
            COUNT(ph.id) AS photo_count,
            MAX(ph.uploaded_at) AS last_uploaded_at
        FROM products p
        JOIN photos ph ON ph.product_id = p.id
        GROUP BY p.id
        ORDER BY last_uploaded_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        flash("아직 사진이 등록된 상품이 없습니다.", "warning")
        return redirect(url_for("home"))

    df = pd.DataFrame([dict(r) for r in rows])
    out_path = DATA_DIR / "registered_products.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="등록상품")

    return send_file(out_path, as_attachment=True, download_name="registered_products.xlsx")


if __name__ == "__main__":
    init_db()
    upsert_products_from_master_excel()
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
'@ | Out-File -Encoding utf8 app.py

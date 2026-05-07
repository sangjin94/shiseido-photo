import os
import re
import sqlite3
import argparse
import secrets
import json
from datetime import datetime, timezone
from pathlib import Path
from functools import wraps

import pandas as pd
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, abort, jsonify, send_file,
    session
)

from PIL import Image
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from werkzeug.middleware.proxy_fix import ProxyFix


# =========================
# ✅ .env 로드 (로컬용)
# =========================
load_dotenv()

S3_BUCKET = os.environ.get("S3_BUCKET", "").strip()
AWS_REGION = os.environ.get("AWS_REGION", "").strip()
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()

PRESIGNED_EXPIRES = int(os.environ.get("PRESIGNED_EXPIRES", "1800"))
PRESIGNED_REFRESH_MARGIN = int(os.environ.get("PRESIGNED_REFRESH_MARGIN", "120"))

FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "change-this-to-a-random-secret")
AUTO_LOAD_MASTER = os.environ.get("AUTO_LOAD_MASTER", "1").strip()

MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH", str(20 * 1024 * 1024)))

VIEW_PASSWORD = os.environ.get("VIEW_PASSWORD", "").strip()
EDIT_PASSWORD = os.environ.get("EDIT_PASSWORD", "").strip()

FLASK_DEBUG = os.environ.get("FLASK_DEBUG", "0").strip() == "1"
COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "1").strip() == "1"

if not all([S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
    raise SystemExit(
        "필수 환경변수 누락: S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY\n"
        "서버 배포 시에도 환경변수로 반드시 설정하세요."
    )

if not VIEW_PASSWORD or not EDIT_PASSWORD:
    raise SystemExit(
        "필수 환경변수 누락: VIEW_PASSWORD, EDIT_PASSWORD\n"
        "조회/등록 화면 진입용 암호키를 환경변수로 설정하세요."
    )

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


# =========================
# 기본 설정
# =========================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "products.db"
MASTER_XLSX_PATH = DATA_DIR / "master.xlsx"

# ✅ 마스터 업로드 임시 저장 폴더
TMP_MASTER_DIR = DATA_DIR / "tmp_master_upload"

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}

# ✅ 상품 사진 저장(저화질)
MAX_IMAGE_SIZE = (1280, 1280)
JPEG_QUALITY = 65

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=COOKIE_SECURE,
)

DATA_DIR.mkdir(parents=True, exist_ok=True)
TMP_MASTER_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# ✅ 인증(조회/등록 모드)
# =========================
def require_view_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get("auth_view") is True or session.get("auth_edit") is True:
            return fn(*args, **kwargs)
        return redirect(url_for("auth_view", next=request.path))
    return wrapper


def require_edit_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get("auth_edit") is True:
            return fn(*args, **kwargs)
        return redirect(url_for("auth_edit", next=request.path))
    return wrapper


@app.route("/", methods=["GET"])
def gate_index():
    return render_template("gate_index.html")


@app.route("/auth/view", methods=["GET", "POST"])
def auth_view():
    next_url = request.args.get("next", "") or url_for("view_products")
    if request.method == "POST":
        pw = (request.form.get("password", "") or "").strip()
        if pw == VIEW_PASSWORD:
            session["auth_view"] = True
            session.pop("auth_edit", None)
            return redirect(next_url)
        flash("조회용 암호키가 올바르지 않습니다.", "danger")
    return render_template("auth.html", mode="view", title="조회용 화면", next_url=next_url)


@app.route("/auth/edit", methods=["GET", "POST"])
def auth_edit():
    next_url = request.args.get("next", "") or url_for("register_home")
    if request.method == "POST":
        pw = (request.form.get("password", "") or "").strip()
        if pw == EDIT_PASSWORD:
            session["auth_edit"] = True
            session["auth_view"] = True
            return redirect(next_url)
        flash("등록용 암호키가 올바르지 않습니다.", "danger")
    return render_template("auth.html", mode="edit", title="등록용 화면", next_url=next_url)


@app.route("/logout", methods=["GET"])
def logout():
    session.pop("auth_view", None)
    session.pop("auth_edit", None)
    flash("로그아웃 완료", "success")
    return redirect(url_for("gate_index"))


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": datetime.now().isoformat(timespec="seconds")})


@app.route("/dashboard", methods=["GET"])
def dashboard():
    from collections import defaultdict, OrderedDict

    conn = get_db()
    cur = conn.cursor()

    total_sku = cur.execute(
        "SELECT COUNT(*) FROM products WHERE IFNULL(is_active,1)=1"
    ).fetchone()[0]

    registered_sku = cur.execute(
        """
        SELECT COUNT(DISTINCT ph.product_item_code)
        FROM photos ph
        JOIN products p ON p.item_code = ph.product_item_code
        WHERE IFNULL(p.is_active,1)=1
        """
    ).fetchone()[0]

    daily_rows = cur.execute(
        """
        SELECT
            DATE(MIN(ph.uploaded_at)) AS reg_date,
            ph.product_item_code
        FROM photos ph
        JOIN products p ON p.item_code = ph.product_item_code
        WHERE IFNULL(p.is_active,1)=1
        GROUP BY ph.product_item_code
        ORDER BY reg_date ASC
        """
    ).fetchall()
    conn.close()

    daily_count = defaultdict(int)
    for r in daily_rows:
        daily_count[r[0]] += 1

    sorted_dates = sorted(daily_count.keys())
    cumulative = 0
    daily_series = []
    for d in sorted_dates:
        cumulative += daily_count[d]
        rate = round(cumulative / total_sku * 100, 2) if total_sku else 0
        daily_series.append({
            "date": d,
            "count": daily_count[d],
            "cumulative": cumulative,
            "rate": rate,
        })

    def week_label(date_str: str) -> str:
        from datetime import date
        d = date.fromisoformat(date_str)
        first_day = date(d.year, d.month, 1)
        week_of_month = (d.day + first_day.weekday()) // 7 + 1
        return f"{d.month}월 {week_of_month}주차"

    weekly_map = OrderedDict()
    for item in daily_series:
        wk = week_label(item["date"])
        if wk not in weekly_map:
            weekly_map[wk] = {"label": wk, "count": 0, "cumulative": 0, "rate": 0.0}
        weekly_map[wk]["count"] += item["count"]
        weekly_map[wk]["cumulative"] = item["cumulative"]
        weekly_map[wk]["rate"] = item["rate"]

    weekly_series = list(weekly_map.values())

    return render_template(
        "dashboard.html",
        total_sku=total_sku,
        registered_sku=registered_sku,
        rate=round(registered_sku / total_sku * 100, 2) if total_sku else 0,
        daily_series=daily_series,
        weekly_series=weekly_series,
    )


# =========================
# 유틸
# =========================
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_code(s: str) -> str:
    if s is None:
        return ""
    return str(s).strip()


def allowed_file(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[-1].lower()
    return ext in ALLOWED_EXTENSIONS


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TMP_MASTER_DIR.mkdir(parents=True, exist_ok=True)


def _fix_exif_orientation(img: Image.Image) -> Image.Image:
    try:
        exif = getattr(img, "_getexif", None)
        if not exif:
            return img
        ex = exif()
        if not ex:
            return img
        orientation = ex.get(274)
        if orientation == 3:
            return img.rotate(180, expand=True)
        if orientation == 6:
            return img.rotate(270, expand=True)
        if orientation == 8:
            return img.rotate(90, expand=True)
    except Exception:
        pass
    return img


def save_low_quality_jpeg_to_bytes(file_storage) -> bytes:
    img = Image.open(file_storage.stream)
    img = _fix_exif_orientation(img)

    if img.mode != "RGB":
        img = img.convert("RGB")

    img.thumbnail(MAX_IMAGE_SIZE)

    import io
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    return buf.getvalue()


def next_photo_filename(cur, item_code: str) -> str:
    cur.execute("SELECT filename FROM photos WHERE product_item_code = ?", (item_code,))
    rows = cur.fetchall()

    pattern = re.compile(rf"^{re.escape(item_code)}_(\d+)\.jpg$", re.IGNORECASE)
    max_n = 0
    for r in rows:
        fn = r["filename"] or ""
        m = pattern.match(fn)
        if m:
            try:
                max_n = max(max_n, int(m.group(1)))
            except Exception:
                pass

    return f"{item_code}_{max_n + 1:03d}.jpg"


def s3_key_for(item_code: str, filename: str) -> str:
    item_code = normalize_code(item_code)
    filename = (filename or "").strip().replace("\\", "_").replace("/", "_")
    return f"products/{item_code}/{filename}"


def s3_put_bytes(key: str, data: bytes, content_type: str = "image/jpeg"):
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
    except ClientError as e:
        raise RuntimeError(f"S3 PutObject 실패: {e}")


def s3_delete(key: str):
    # ✅ 사진 삭제 버튼에서만 S3 원본 삭제
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
    except ClientError:
        pass


def presigned_get_url(key: str, expires_sec: int = None) -> str:
    if not expires_sec:
        expires_sec = PRESIGNED_EXPIRES
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires_sec,
    )


def _has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.cursor()
    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    for r in rows:
        if str(r[1]).lower() == col.lower():
            return True
    return False


# =========================
# DB 초기화 (+ 마이그레이션)
# =========================
def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            item_code TEXT PRIMARY KEY,
            item_name TEXT,
            scan_code TEXT
        )
        """
    )

    # ✅ remark 컬럼
    if not _has_column(conn, "products", "remark"):
        cur.execute("ALTER TABLE products ADD COLUMN remark TEXT")

    # ✅ is_active 컬럼 (마스터에서 제외된 상품은 0으로 숨김)
    if not _has_column(conn, "products", "is_active"):
        cur.execute("ALTER TABLE products ADD COLUMN is_active INTEGER DEFAULT 1")

    # 기존 데이터 null이면 1로 보정
    cur.execute("UPDATE products SET is_active=1 WHERE is_active IS NULL")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_item_code TEXT NOT NULL,
            filename TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            s3_key TEXT,
            FOREIGN KEY(product_item_code) REFERENCES products(item_code)
        )
        """
    )

    conn.commit()
    conn.close()


# =========================
# 마스터 검증/임시 저장
# =========================
def _validate_master_df(df: pd.DataFrame):
    df.columns = df.columns.astype(str).str.strip()

    required = ["ITEM_CODE", "ITEM_NAME", "SCAN_CODE"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing} / 현재 컬럼: {list(df.columns)}")

    has_remark = "REMARK" in df.columns
    cols = required + (["REMARK"] if has_remark else [])
    df = df[cols].copy()

    for col in cols:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df = df[df["ITEM_CODE"] != ""].copy()
    if len(df) == 0:
        raise ValueError("유효한 ITEM_CODE 행이 0건입니다.")

    dup_mask = df["ITEM_CODE"].duplicated(keep=False)
    dup_codes = sorted(set(df.loc[dup_mask, "ITEM_CODE"].tolist()))
    warnings = []
    if dup_codes:
        warnings.append(f"ITEM_CODE 중복 {len(dup_codes)}건 (샘플): {dup_codes[:20]}{'...' if len(dup_codes) > 20 else ''}")

    preview = df.head(30).to_dict(orient="records")

    meta = {
        "rows": int(len(df)),
        "has_remark": bool(has_remark),
        "columns": cols,
        "dup_count": int(len(dup_codes)),
        "dup_codes_sample": dup_codes[:50],
        "warnings": warnings,
    }
    return df, meta, preview


def _tmp_master_path(token: str) -> Path:
    return TMP_MASTER_DIR / f"{token}.json"


def _clear_pending_master_upload():
    payload = session.get("pending_master_upload")
    if payload and isinstance(payload, dict):
        token = (payload.get("token") or "").strip()
        if token:
            p = _tmp_master_path(token)
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
    session.pop("pending_master_upload", None)


# =========================
# ✅ 마스터 동기화: 삭제 대신 is_active 처리
# =========================
def _apply_master_sync_is_active(df: pd.DataFrame) -> dict:
    """
    엑셀 기준 동기화:
    - 엑셀에 있는 상품: upsert + is_active=1
    - 엑셀에 없는 상품: is_active=0 (삭제 ❌, photos 삭제 ❌, S3 삭제 ❌)
    """
    df.columns = df.columns.astype(str).str.strip()
    has_remark = "REMARK" in df.columns

    need_cols = ["ITEM_CODE", "ITEM_NAME", "SCAN_CODE"] + (["REMARK"] if has_remark else [])
    df = df[need_cols].copy()
    for c in need_cols:
        df[c] = df[c].fillna("").astype(str).str.strip()
    df = df[df["ITEM_CODE"] != ""].copy()

    keep_codes = set(df["ITEM_CODE"].tolist())

    conn = get_db()
    cur = conn.cursor()

    # 1) 전체 상품 중 마스터에 없는 건 비활성화
    rows = cur.execute("SELECT item_code FROM products").fetchall()
    existing_codes = set([r[0] for r in rows if r and r[0]])
    to_deactivate = sorted(existing_codes - keep_codes)

    deactivated = 0
    if to_deactivate:
        CHUNK = 800
        for i in range(0, len(to_deactivate), CHUNK):
            chunk = to_deactivate[i:i + CHUNK]
            q = ",".join(["?"] * len(chunk))
            cur.execute(f"UPDATE products SET is_active=0 WHERE item_code IN ({q})", chunk)
            deactivated += len(chunk)

    # 2) 마스터에 있는 건 upsert + 활성화
    upserted = 0
    for _, row in df.iterrows():
        item_code = normalize_code(row["ITEM_CODE"])
        item_name = normalize_code(row["ITEM_NAME"])
        scan_code = normalize_code(row["SCAN_CODE"])
        remark = normalize_code(row["REMARK"]) if has_remark else ""

        if has_remark:
            cur.execute(
                """
                INSERT INTO products (item_code, item_name, scan_code, remark, is_active)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(item_code) DO UPDATE SET
                  item_name=excluded.item_name,
                  scan_code=excluded.scan_code,
                  remark=excluded.remark,
                  is_active=1
                """,
                (item_code, item_name, scan_code, remark)
            )
        else:
            cur.execute(
                """
                INSERT INTO products (item_code, item_name, scan_code, is_active)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(item_code) DO UPDATE SET
                  item_name=excluded.item_name,
                  scan_code=excluded.scan_code,
                  is_active=1
                """,
                (item_code, item_name, scan_code)
            )
        upserted += 1

    conn.commit()
    conn.close()

    return {"upserted": upserted, "deactivated": deactivated}


# =========================
# ✅ S3 스캔 → photos 테이블 복구 (S3 원본 유지)
# =========================
def rebuild_photos_from_s3(prefix: str = "products/") -> dict:
    """
    S3의 products/<item_code>/<filename> 를 스캔하여
    photos 테이블에 누락된 레코드를 채운다.
    - 기존 s3_key가 있으면 중복 삽입 안 함
    - products에 해당 item_code가 없어도(비활성 포함) photos는 기록 가능(외래키 강제 안 켬)
    """
    conn = get_db()
    cur = conn.cursor()

    # 기존 s3_key 목록
    existing = set()
    for r in cur.execute("SELECT s3_key FROM photos WHERE s3_key IS NOT NULL AND s3_key <> ''").fetchall():
        existing.add(r[0])

    paginator = s3.get_paginator("list_objects_v2")

    inserted = 0
    scanned = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key") or ""
            if not key.startswith(prefix):
                continue
            # products/<item_code>/<filename>
            parts = key.split("/", 2)
            if len(parts) != 3:
                continue
            _, item_code, filename = parts
            item_code = (item_code or "").strip()
            filename = (filename or "").strip()
            if not item_code or not filename:
                continue

            # 이미지 확장자만
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            if ext not in {"jpg", "jpeg", "png", "webp", "gif"}:
                continue

            scanned += 1

            if key in existing:
                continue

            lm = obj.get("LastModified")
            if lm and hasattr(lm, "astimezone"):
                # S3 LastModified는 tz-aware
                ts = lm.astimezone(timezone.utc).isoformat(timespec="seconds")
            else:
                ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

            cur.execute(
                "INSERT INTO photos (product_item_code, filename, uploaded_at, s3_key) VALUES (?, ?, ?, ?)",
                (item_code, filename, ts, key)
            )
            existing.add(key)
            inserted += 1

            # 너무 큰 트랜잭션 방지(1000개마다 커밋)
            if inserted % 1000 == 0:
                conn.commit()

    conn.commit()
    conn.close()

    return {"scanned": scanned, "inserted": inserted}


# =========================
# ✅ Presigned URL 새로고침 API
# =========================
@app.get("/api/presign/photos")
@require_view_auth
def api_presign_photos():
    item_code = normalize_code(request.args.get("item_code", ""))
    if not item_code:
        return jsonify({"ok": False, "error": "missing_item_code"}), 400

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, filename, s3_key FROM photos WHERE product_item_code = ? ORDER BY id DESC",
        (item_code,)
    )
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        key = r["s3_key"] or s3_key_for(item_code, r["filename"])
        out.append({"id": r["id"], "url": presigned_get_url(key, expires_sec=PRESIGNED_EXPIRES)})

    return jsonify({
        "ok": True,
        "expires_in": PRESIGNED_EXPIRES,
        "refresh_margin": PRESIGNED_REFRESH_MARGIN,
        "photos": out
    })


# =========================
# ✅ 마스터 업로드: 검증 → 미리보기 → 확정반영(is_active 동기화)
# =========================
@app.route("/admin/master_upload", methods=["GET", "POST"])
@require_edit_auth
def admin_master_upload():
    if request.method == "POST":
        f = request.files.get("master")
        if not f or not f.filename:
            flash("업로드할 엑셀 파일을 선택해 주세요.", "warning")
            return redirect(url_for("admin_master_upload"))

        name = f.filename.lower()
        if not (name.endswith(".xlsx") or name.endswith(".xls")):
            flash("엑셀 파일(xlsx/xls)만 업로드할 수 있습니다.", "danger")
            return redirect(url_for("admin_master_upload"))

        try:
            df_raw = pd.read_excel(f)
            df, meta, preview = _validate_master_df(df_raw)

            _clear_pending_master_upload()

            token = secrets.token_urlsafe(16)
            path = _tmp_master_path(token)

            payload = {
                "records": df.to_dict(orient="records"),
                "saved_at": datetime.now().isoformat(timespec="seconds"),
                "uploaded_name": f.filename,
                "meta": meta,
            }
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            session["pending_master_upload"] = {"token": token, "uploaded_name": f.filename}

            return render_template("admin_master_preview.html", meta=meta, preview=preview, uploaded_name=f.filename)

        except Exception as e:
            flash(f"업로드/검증 실패: {e}", "danger")
            return redirect(url_for("admin_master_upload"))

    _clear_pending_master_upload()
    return render_template("admin_master_upload.html")


@app.post("/admin/master_upload/confirm")
@require_edit_auth
def admin_master_upload_confirm():
    action = (request.form.get("_action", "") or "").strip()

    payload = session.get("pending_master_upload") or {}
    token = (payload.get("token") or "").strip()
    if not token:
        flash("검증된 업로드 데이터가 없습니다. 다시 업로드해 주세요.", "warning")
        return redirect(url_for("admin_master_upload"))

    path = _tmp_master_path(token)
    if not path.exists():
        session.pop("pending_master_upload", None)
        flash("업로드 임시 데이터가 만료/삭제되었습니다. 다시 업로드해 주세요.", "warning")
        return redirect(url_for("admin_master_upload"))

    if action == "cancel":
        try:
            path.unlink()
        except Exception:
            pass
        session.pop("pending_master_upload", None)
        flash("마스터 업데이트를 취소했습니다.", "info")
        return redirect(url_for("register_home"))

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        records = raw.get("records", [])
        df = pd.DataFrame(records)

        result = _apply_master_sync_is_active(df)

        try:
            path.unlink()
        except Exception:
            pass
        session.pop("pending_master_upload", None)

        flash(
            f"마스터 동기화 완료: 반영 {result['upserted']}건 / 비활성화 {result['deactivated']}건 (사진/S3 원본 유지)",
            "success"
        )
        return redirect(url_for("register_home"))

    except Exception as e:
        flash(f"마스터 반영 실패: {e}", "danger")
        return redirect(url_for("admin_master_upload"))


# =========================
# ✅ 조회용: is_active=1만 표시
# =========================
@app.route("/view/products", methods=["GET"])
@require_view_auth
def view_products():
    q = normalize_code(request.args.get("q", ""))
    f = (request.args.get("f", "all") or "all").strip().lower()

    conn = get_db()
    cur = conn.cursor()

    base_sql = """
        SELECT
            p.item_code,
            p.item_name,
            p.scan_code,
            p.remark,
            COUNT(ph.id) AS photo_count,
            (
                SELECT
                  CASE
                    WHEN ph2.s3_key IS NOT NULL AND ph2.s3_key <> '' THEN ph2.s3_key
                    ELSE ('products/' || p.item_code || '/' || ph2.filename)
                  END
                FROM photos ph2
                WHERE ph2.product_item_code = p.item_code
                ORDER BY ph2.uploaded_at ASC, ph2.id ASC
                LIMIT 1
            ) AS thumb_key
        FROM products p
        LEFT JOIN photos ph
          ON ph.product_item_code = p.item_code
    """

    where = ["IFNULL(p.is_active,1) = 1"]
    params = []

    if q:
        like = f"%{q}%"
        where.append("(p.item_code LIKE ? OR p.scan_code LIKE ? OR p.item_name LIKE ? OR p.remark LIKE ?)")
        params.extend([like, like, like, like])

    sql = base_sql + " WHERE " + " AND ".join(where)
    sql += " GROUP BY p.item_code, p.item_name, p.scan_code, p.remark "

    if f == "has":
        sql += " HAVING COUNT(ph.id) > 0 "
    elif f == "none":
        sql += " HAVING COUNT(ph.id) = 0 "

    sql += " ORDER BY photo_count DESC, p.item_code ASC LIMIT 2000 "

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        d = dict(r)
        if d.get("thumb_key"):
            d["thumb_url"] = presigned_get_url(d["thumb_key"], expires_sec=PRESIGNED_EXPIRES)
        else:
            d["thumb_url"] = ""
        out.append(d)

    return render_template("products_with_photos.html", rows=out, q=q, f=f)


@app.route("/view/product/<item_code>", methods=["GET"])
@require_view_auth
def view_product_detail(item_code):
    item_code = normalize_code(item_code)
    if not item_code:
        abort(404)

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        "SELECT item_code, item_name, scan_code, remark FROM products WHERE item_code=? AND IFNULL(is_active,1)=1",
        (item_code,)
    )
    product = cur.fetchone()
    if not product:
        conn.close()
        abort(404)

    cur.execute(
        "SELECT id, filename, uploaded_at, s3_key FROM photos WHERE product_item_code=? ORDER BY id DESC",
        (item_code,)
    )
    photos_rows = cur.fetchall()
    conn.close()

    photos = []
    for r in photos_rows:
        key = r["s3_key"] or s3_key_for(item_code, r["filename"])
        photos.append({
            "id": r["id"],
            "filename": r["filename"],
            "uploaded_at": r["uploaded_at"],
            "url": presigned_get_url(key, expires_sec=PRESIGNED_EXPIRES),
        })

    return render_template(
        "view_product_detail.html",
        product=product,
        photos=photos,
        item_code=item_code,
        presigned_expires=PRESIGNED_EXPIRES,
        presigned_refresh_margin=PRESIGNED_REFRESH_MARGIN,
    )


@app.route("/view/export/products.xlsx", methods=["GET"])
@require_view_auth
def view_export_products_xlsx():
    q = normalize_code(request.args.get("q", ""))
    f = (request.args.get("f", "all") or "all").strip().lower()

    conn = get_db()
    cur = conn.cursor()

    base_sql = """
        SELECT
            p.item_name AS 상품명,
            p.item_code AS 상품코드,
            p.scan_code AS 스캔바코드,
            IFNULL(p.remark, '') AS 비고,
            COUNT(ph.id) AS 사진수
        FROM products p
        LEFT JOIN photos ph
          ON ph.product_item_code = p.item_code
        WHERE IFNULL(p.is_active,1)=1
    """

    where = []
    params = []

    if q:
        like = f"%{q}%"
        where.append("(p.item_code LIKE ? OR p.scan_code LIKE ? OR p.item_name LIKE ? OR p.remark LIKE ?)")
        params.extend([like, like, like, like])

    sql = base_sql
    if where:
        sql += " AND " + " AND ".join(where)

    sql += " GROUP BY p.item_name, p.item_code, p.scan_code, p.remark "

    if f == "has":
        sql += " HAVING COUNT(ph.id) > 0 "
    elif f == "none":
        sql += " HAVING COUNT(ph.id) = 0 "

    sql += " ORDER BY 사진수 DESC, 상품코드 ASC "

    cur.execute(sql, params)
    data = cur.fetchall()
    conn.close()

    df = pd.DataFrame([dict(r) for r in data])

    import io
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="상품리스트")
    bio.seek(0)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"상품리스트_{ts}.xlsx"

    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# =========================
# ✅ 등록용: is_active=1만 표시
# =========================
@app.route("/register", methods=["GET"])
@require_edit_auth
def register_home():
    q = normalize_code(request.args.get("q", ""))

    conn = get_db()
    cur = conn.cursor()

    base_sql = """
        SELECT
            p.item_code,
            p.item_name,
            p.scan_code,
            p.remark,
            COUNT(ph.id) AS photo_count
        FROM products p
        LEFT JOIN photos ph
          ON ph.product_item_code = p.item_code
        WHERE IFNULL(p.is_active,1)=1
    """

    where = []
    params = []

    if q:
        like = f"%{q}%"
        where.append("(p.item_code LIKE ? OR p.scan_code LIKE ? OR p.item_name LIKE ? OR p.remark LIKE ?)")
        params.extend([like, like, like, like])

    sql = base_sql
    if where:
        sql += " AND " + " AND ".join(where)

    sql += """
        GROUP BY p.item_code, p.item_name, p.scan_code, p.remark
        ORDER BY photo_count DESC, p.item_code ASC
        LIMIT 2000
    """

    cur.execute(sql, params)
    products = cur.fetchall()
    conn.close()
    return render_template("home.html", products=products, q=q)


@app.route("/register/product/<item_code>", methods=["GET", "POST"])
@require_edit_auth
def register_product_detail(item_code):
    item_code = normalize_code(item_code)
    if not item_code:
        abort(404)

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        "SELECT item_code, item_name, scan_code, remark FROM products WHERE item_code = ? AND IFNULL(is_active,1)=1",
        (item_code,)
    )
    product = cur.fetchone()
    if not product:
        conn.close()
        flash("해당 상품을 찾을 수 없습니다. (마스터에 없거나 비활성화된 상품)", "warning")
        return redirect(url_for("register_home"))

    action = (request.form.get("_action", "") or "").strip()

    if request.method == "POST" and action == "save_remark":
        remark = (request.form.get("remark", "") or "").strip()
        cur.execute("UPDATE products SET remark=? WHERE item_code=?", (remark, item_code))
        conn.commit()
        conn.close()
        flash("비고 저장 완료", "success")
        return redirect(url_for("register_product_detail", item_code=item_code))

    if request.method == "POST" and action in ("", "upload_photos"):
        if "photos" not in request.files:
            flash("업로드할 파일이 없습니다. (폼 enctype='multipart/form-data' 확인)", "danger")
            conn.close()
            return redirect(url_for("register_product_detail", item_code=item_code))

        files = request.files.getlist("photos")
        if not files or all(not f.filename for f in files):
            flash("파일을 선택해 주세요.", "warning")
            conn.close()
            return redirect(url_for("register_product_detail", item_code=item_code))

        saved_count = 0
        fail_count = 0

        for f_ in files:
            if not f_ or not f_.filename:
                continue
            if not allowed_file(f_.filename):
                flash(f"지원하지 않는 파일 형식입니다: {f_.filename}", "warning")
                fail_count += 1
                continue

            final_name = next_photo_filename(cur, item_code)
            key = s3_key_for(item_code, final_name)

            try:
                data = save_low_quality_jpeg_to_bytes(f_)
                s3_put_bytes(key, data, content_type="image/jpeg")
            except Exception as e:
                flash(f"업로드 실패: {f_.filename} → {e}", "danger")
                fail_count += 1
                continue

            cur.execute(
                "INSERT INTO photos (product_item_code, filename, uploaded_at, s3_key) VALUES (?, ?, ?, ?)",
                (item_code, final_name, datetime.now().isoformat(timespec="seconds"), key)
            )
            saved_count += 1

        conn.commit()
        flash(f"사진 업로드: 성공 {saved_count} / 실패 {fail_count} (S3 저장)", "success" if saved_count else "warning")
        conn.close()
        return redirect(url_for("register_product_detail", item_code=item_code))

    # GET
    cur.execute(
        "SELECT item_code, item_name, scan_code, remark FROM products WHERE item_code = ? AND IFNULL(is_active,1)=1",
        (item_code,)
    )
    product = cur.fetchone()

    cur.execute(
        "SELECT id, filename, uploaded_at, s3_key FROM photos WHERE product_item_code = ? ORDER BY id DESC",
        (item_code,)
    )
    photos_rows = cur.fetchall()
    conn.close()

    photos = []
    for r in photos_rows:
        key = r["s3_key"] or s3_key_for(item_code, r["filename"])
        photos.append({
            "id": r["id"],
            "filename": r["filename"],
            "uploaded_at": r["uploaded_at"],
            "url": presigned_get_url(key, expires_sec=PRESIGNED_EXPIRES),
        })

    return render_template(
        "product_detail.html",
        product=product,
        photos=photos,
        item_code=item_code,
        presigned_expires=PRESIGNED_EXPIRES,
        presigned_refresh_margin=PRESIGNED_REFRESH_MARGIN,
    )


@app.route("/register/photo/<int:photo_id>/delete", methods=["POST"])
@require_edit_auth
def register_delete_photo(photo_id: int):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id, product_item_code, filename, s3_key FROM photos WHERE id = ?", (photo_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        abort(404)

    item_code = row["product_item_code"]
    filename = row["filename"]
    key = row["s3_key"] or s3_key_for(item_code, filename)

    cur.execute("DELETE FROM photos WHERE id = ?", (photo_id,))
    conn.commit()
    conn.close()

    # ✅ 버튼 삭제는 S3 삭제
    s3_delete(key)

    flash("사진 삭제 완료", "success")
    return redirect(url_for("register_product_detail", item_code=item_code))


# =========================
# 부트스트랩
# =========================
def bootstrap():
    ensure_dirs()
    init_db()


bootstrap()


def main_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run", "rebuild_photos_from_s3"]
    )
    args = parser.parse_args()

    if args.command == "rebuild_photos_from_s3":
        result = rebuild_photos_from_s3(prefix="products/")
        print(f"[OK] S3 scan done. scanned={result['scanned']} inserted={result['inserted']}")
        return

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        debug=FLASK_DEBUG
    )


if __name__ == "__main__":
    main_cli()

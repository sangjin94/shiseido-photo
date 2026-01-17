import os
import re
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, abort, jsonify, send_from_directory
)

from PIL import Image, ImageOps, ImageEnhance, ImageFilter
import zxingcpp

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


# =========================
# ✅ .env 로드 (로컬용) — 서버에서는 환경변수로 주입해도 됨
# =========================
load_dotenv()

S3_BUCKET = os.environ.get("S3_BUCKET", "").strip()
AWS_REGION = os.environ.get("AWS_REGION", "").strip()
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()

# Presigned 유효시간(초)
PRESIGNED_EXPIRES = int(os.environ.get("PRESIGNED_EXPIRES", "1800"))  # 기본 30분
# 만료 전 자동 새로고침 여유(초)
PRESIGNED_REFRESH_MARGIN = int(os.environ.get("PRESIGNED_REFRESH_MARGIN", "120"))

# 서버 배포 시 Secret Key는 반드시 env로 주입 권장
FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "change-this-to-a-random-secret")

# 마스터 엑셀 자동 로드 여부 (서버에서 매번 로드 싫으면 0)
AUTO_LOAD_MASTER = os.environ.get("AUTO_LOAD_MASTER", "1").strip()  # "1" or "0"

# ✅ 업로드 용량 제한(바이트) - 기본 20MB
MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH", str(20 * 1024 * 1024)))

# ✅ 관리자 삭제 토큰(선택) - 설정하면 /admin/purge_photos 사용 가능
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "").strip()

if not all([S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
    raise SystemExit(
        "필수 환경변수 누락: S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY\n"
        "서버 배포 시에도 환경변수로 반드시 설정하세요."
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

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}

# ✅ 상품 사진 저장(저화질)
MAX_IMAGE_SIZE = (1280, 1280)
JPEG_QUALITY = 65

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

DATA_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# 유틸
# =========================
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def allowed_file(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[-1].lower()
    return ext in ALLOWED_EXTENSIONS


def normalize_code(s: str) -> str:
    if s is None:
        return ""
    return str(s).strip()


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _fix_exif_orientation(img: Image.Image) -> Image.Image:
    """iPhone EXIF 방향 보정"""
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
    """
    상품 사진 저장은 저화질 JPG로 변환(용량 절감)
    로컬 저장 대신 bytes로 만들어 S3에 올린다.
    """
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
    """상품코드_001.jpg 방식"""
    cur.execute("SELECT filename FROM photos WHERE product_item_code = ?", (item_code,))
    rows = cur.fetchall()

    pattern = re.compile(rf"^{re.escape(item_code)}_(\d+)\.jpg$", re.IGNORECASE)
    max_n = 0
    for r in rows:
        fn = r["filename"]
        m = pattern.match(fn)
        if m:
            try:
                n = int(m.group(1))
                max_n = max(max_n, n)
            except Exception:
                pass

    return f"{item_code}_{max_n + 1:03d}.jpg"


def s3_key_for(item_code: str, filename: str) -> str:
    """S3 오브젝트 키: products/<item_code>/<filename>"""
    item_code = normalize_code(item_code)
    filename = filename.strip().replace("\\", "_").replace("/", "_")
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
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
    except ClientError:
        pass


def s3_delete_prefix(prefix: str):
    """
    prefix 아래 객체 전부 삭제 (최대 1000개 단위로 반복)
    """
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue
        objects = [{"Key": obj["Key"]} for obj in contents]
        for i in range(0, len(objects), 1000):
            chunk = objects[i:i + 1000]
            try:
                s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": chunk})
            except ClientError as e:
                raise RuntimeError(f"S3 prefix 삭제 실패: {e}")


def presigned_get_url(key: str, expires_sec: int = None) -> str:
    if not expires_sec:
        expires_sec = PRESIGNED_EXPIRES
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires_sec,
    )


# =========================
# DB 초기화 + 마이그레이션
# =========================
def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols


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
# 마스터 적재
# =========================
def load_master_excel():
    if not Path(MASTER_XLSX_PATH).exists():
        raise FileNotFoundError(f"마스터 파일이 없습니다: {MASTER_XLSX_PATH}")

    df = pd.read_excel(MASTER_XLSX_PATH)
    df.columns = df.columns.astype(str).str.strip()

    REQUIRED_COLUMNS = ["ITEM_CODE", "ITEM_NAME", "SCAN_CODE"]
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"마스터 엑셀에 필수 컬럼이 없습니다: {missing}\n현재 컬럼: {list(df.columns)}")

    df = df[REQUIRED_COLUMNS].copy()
    for col in REQUIRED_COLUMNS:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df = df[df["ITEM_CODE"] != ""]

    conn = get_db()
    cur = conn.cursor()

    for _, row in df.iterrows():
        item_code = normalize_code(row["ITEM_CODE"])
        item_name = normalize_code(row["ITEM_NAME"])
        scan_code = normalize_code(row["SCAN_CODE"])

        cur.execute(
            """
            INSERT OR REPLACE INTO products (item_code, item_name, scan_code)
            VALUES (?, ?, ?)
            """,
            (item_code, item_name, scan_code)
        )

    conn.commit()
    conn.close()


# =========================
# ✅ 전체 사진 정리(DB + S3)
# =========================
def purge_all_photos(delete_s3_objects: bool = True):
    """
    photos 테이블 전부 삭제 + (옵션) S3 products/ 아래 객체 전부 삭제
    """
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM photos")
    rows = cur.fetchall()

    cur.execute("DELETE FROM photos")
    conn.commit()
    conn.close()

    if delete_s3_objects:
        s3_delete_prefix("products/")

    return len(rows)


# =========================
# ✅ zxing-cpp 바코드 인식(서버 API)
# =========================
def _variants_for_decode(img: Image.Image):
    base = _fix_exif_orientation(img)
    if base.mode != "RGB":
        base = base.convert("RGB")

    base.thumbnail((2400, 2400))

    def crop_center(im: Image.Image, rw: float, rh: float):
        w, h = im.size
        cw, ch = int(w * rw), int(h * rh)
        x0 = max(0, (w - cw) // 2)
        y0 = max(0, (h - ch) // 2)
        return im.crop((x0, y0, x0 + cw, y0 + ch))

    def crop_bottom(im: Image.Image, rh: float):
        w, h = im.size
        ch = int(h * rh)
        return im.crop((0, h - ch, w, h))

    g = ImageOps.grayscale(base)
    a = ImageOps.autocontrast(g)
    c2 = ImageEnhance.Contrast(a).enhance(2.0)
    c24 = ImageEnhance.Contrast(a).enhance(2.4)
    sharp = c24.filter(ImageFilter.UnsharpMask(radius=2, percent=180, threshold=3))

    variants = [
        base, a, c2, c24, sharp,
        crop_center(base, 0.85, 0.55),
        crop_center(a, 0.85, 0.55),
        crop_center(c24, 0.75, 0.45),
        crop_bottom(base, 0.45),
        crop_bottom(a, 0.45),
        crop_bottom(c24, 0.45),
        crop_bottom(sharp, 0.45),
    ]
    return variants


def decode_barcode_zxing(img: Image.Image):
    candidates = []
    debug = []

    opts = zxingcpp.ReaderOptions()
    opts.try_harder = True
    opts.try_rotate = True
    opts.formats = (
        zxingcpp.BarcodeFormat.EAN13
        | zxingcpp.BarcodeFormat.EAN8
        | zxingcpp.BarcodeFormat.UPCA
        | zxingcpp.BarcodeFormat.UPCE
        | zxingcpp.BarcodeFormat.CODE128
        | zxingcpp.BarcodeFormat.CODE39
    )

    for i, v in enumerate(_variants_for_decode(img), start=1):
        if v.mode not in ("RGB", "L"):
            v = v.convert("RGB")

        try:
            results = zxingcpp.read_barcodes(v, opts)
        except Exception as e:
            debug.append(f"variant#{i}: exception={e}")
            continue

        debug.append(f"variant#{i}: results={len(results)}")
        for r in results:
            txt = (r.text or "").strip()
            if txt:
                d = re.sub(r"\D", "", txt)
                if d:
                    candidates.append(d)

        if candidates:
            break

    def score(s: str):
        if len(s) == 13:
            return 100
        if len(s) == 12:
            return 90
        if 8 <= len(s) <= 14:
            return 80
        return 10

    uniq = sorted(set(candidates), key=score, reverse=True)
    best = uniq[0] if uniq else None
    return best, uniq, debug


@app.route("/api/decode_barcode", methods=["POST"])
def api_decode_barcode():
    if "image" not in request.files:
        return jsonify({"ok": False, "error": "no_file"}), 400

    f = request.files["image"]
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "empty_file"}), 400

    try:
        img = Image.open(f.stream)
    except Exception:
        return jsonify({"ok": False, "error": "cannot_open_image"}), 400

    best, candidates, debug = decode_barcode_zxing(img)

    if not best:
        return jsonify({
            "ok": False,
            "code": None,
            "candidates": candidates,
            "error": "not_detected",
            "debug": debug[-6:],
        }), 200

    return jsonify({
        "ok": True,
        "code": best,
        "candidates": candidates,
        "error": None,
        "debug": debug[-6:],
    }), 200


# =========================
# ✅ Presigned URL 새로고침 API (자동 갱신용)
# =========================
@app.get("/api/presign/photos")
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
        out.append({
            "id": r["id"],
            "url": presigned_get_url(key, expires_sec=PRESIGNED_EXPIRES),
        })

    return jsonify({
        "ok": True,
        "expires_in": PRESIGNED_EXPIRES,
        "refresh_margin": PRESIGNED_REFRESH_MARGIN,
        "photos": out
    })


# =========================
# ✅ 관리자 전체 삭제 엔드포인트(선택)
# =========================
@app.get("/admin/purge_photos")
def admin_purge_photos():
    if not ADMIN_TOKEN:
        return jsonify({"ok": False, "error": "ADMIN_TOKEN_not_set"}), 403

    token = (request.args.get("token", "") or "").strip()
    if token != ADMIN_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    try:
        deleted = purge_all_photos(delete_s3_objects=True)
        return jsonify({"ok": True, "deleted_rows": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================
# ✅ 템플릿 호환용 라우트 (중요)
# - product_detail.html 이 url_for('uploaded_file', item_code=..., filename=...) 를 사용해도 500 안 나게 함
# =========================
@app.route("/uploads/<item_code>/<path:filename>")
def uploaded_file(item_code, filename):
    """
    템플릿 호환용:
    - DB에서 s3_key를 찾아 presigned URL로 redirect
    - s3_key가 없으면 로컬 파일(/static/uploads/<item_code>/<filename>)로 서빙 (폴백)
    """
    item_code = normalize_code(item_code)
    filename = (filename or "").strip()
    if not item_code or not filename:
        abort(404)

    conn = get_db()
    cur = conn.cursor()
    row = cur.execute(
        "SELECT s3_key, filename FROM photos WHERE product_item_code=? AND filename=? ORDER BY id DESC LIMIT 1",
        (item_code, filename),
    ).fetchone()
    conn.close()

    if not row:
        abort(404)

    s3_key = row["s3_key"]

    if s3_key:
        return redirect(presigned_get_url(s3_key, expires_sec=PRESIGNED_EXPIRES))

    # 로컬 폴백(혹시 로컬에 파일을 저장하는 구조를 병행할 경우 대비)
    uploads_dir = BASE_DIR / "static" / "uploads" / item_code
    return send_from_directory(str(uploads_dir), filename)


# =========================
# 라우트
# =========================
@app.route("/", methods=["GET"])
def home():
    q = normalize_code(request.args.get("q", ""))

    conn = get_db()
    cur = conn.cursor()

    base_sql = """
        SELECT
            p.item_code,
            p.item_name,
            p.scan_code,
            COUNT(ph.id) AS photo_count
        FROM products p
        LEFT JOIN photos ph
          ON ph.product_item_code = p.item_code
    """

    if q:
        like = f"%{q}%"
        sql = base_sql + """
        WHERE p.item_code LIKE ?
           OR p.scan_code LIKE ?
           OR p.item_name LIKE ?
        GROUP BY p.item_code, p.item_name, p.scan_code
        ORDER BY p.item_code
        LIMIT 200
        """
        cur.execute(sql, (like, like, like))
    else:
        sql = base_sql + """
        GROUP BY p.item_code, p.item_name, p.scan_code
        ORDER BY p.item_code
        LIMIT 200
        """
        cur.execute(sql)

    products = cur.fetchall()
    conn.close()
    return render_template("home.html", products=products, q=q)


@app.route("/product/<item_code>", methods=["GET", "POST"])
def product_detail(item_code):
    item_code = normalize_code(item_code)
    if not item_code:
        abort(404)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT item_code, item_name, scan_code FROM products WHERE item_code = ?", (item_code,))
    product = cur.fetchone()
    if not product:
        conn.close()
        flash("해당 상품을 찾을 수 없습니다. (마스터에 없는 상품코드)", "warning")
        return redirect(url_for("home"))

    if request.method == "POST":
        if "photos" not in request.files:
            flash("업로드할 파일이 없습니다. (폼 enctype='multipart/form-data' 확인)", "danger")
            return redirect(url_for("product_detail", item_code=item_code))

        files = request.files.getlist("photos")
        if not files or all(not f.filename for f in files):
            flash("파일을 선택해 주세요.", "warning")
            return redirect(url_for("product_detail", item_code=item_code))

        saved_count = 0
        fail_count = 0

        for f in files:
            if not f or not f.filename:
                continue
            if not allowed_file(f.filename):
                flash(f"지원하지 않는 파일 형식입니다: {f.filename}", "warning")
                fail_count += 1
                continue

            final_name = next_photo_filename(cur, item_code)
            key = s3_key_for(item_code, final_name)

            try:
                data = save_low_quality_jpeg_to_bytes(f)
                s3_put_bytes(key, data, content_type="image/jpeg")
            except Exception as e:
                flash(f"업로드 실패: {f.filename} → {e}", "danger")
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
        return redirect(url_for("product_detail", item_code=item_code))

    # GET: 사진 목록 + presigned
    cur.execute(
        "SELECT id, filename, uploaded_at, s3_key FROM photos WHERE product_item_code = ? ORDER BY id DESC",
        (item_code,)
    )
    photos_rows = cur.fetchall()

    photos = []
    for r in photos_rows:
        key = r["s3_key"] or s3_key_for(item_code, r["filename"])
        photos.append({
            "id": r["id"],
            "filename": r["filename"],
            "uploaded_at": r["uploaded_at"],
            "url": presigned_get_url(key, expires_sec=PRESIGNED_EXPIRES),
        })

    conn.close()

    return render_template(
        "product_detail.html",
        product=product,
        photos=photos,
        item_code=item_code,  # ✅ 템플릿/JS에서 편하게 사용
        presigned_expires=PRESIGNED_EXPIRES,
        presigned_refresh_margin=PRESIGNED_REFRESH_MARGIN,
    )


@app.route("/photo/<int:photo_id>/delete", methods=["POST"])
def delete_photo(photo_id: int):
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

    s3_delete(key)

    flash("사진 삭제 완료", "success")
    return redirect(url_for("product_detail", item_code=item_code))


def bootstrap():
    ensure_dirs()
    init_db()

    if AUTO_LOAD_MASTER == "1":
        try:
            load_master_excel()
            print("[OK] Master loaded:", MASTER_XLSX_PATH)
        except Exception as e:
            print("[WARN] Master load skipped:", e)


bootstrap()


def main_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", nargs="?", default="run", choices=["run", "purge_photos", "load_master"])
    args = parser.parse_args()

    if args.command == "purge_photos":
        deleted = purge_all_photos(delete_s3_objects=True)
        print(f"[OK] purge_photos done. deleted_rows={deleted}")
        return

    if args.command == "load_master":
        load_master_excel()
        print("[OK] master loaded.")
        return

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)


if __name__ == "__main__":
    main_cli()


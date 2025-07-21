import re
import unicodedata
import pandas as pd
from pathlib import Path

###############################################################################
# 0. 共通ユーティリティ
###############################################################################
def normalize_str(s: str | float | int | None) -> str | None:
    """前後空白除去 + 全角→半角（NFKC 正規化）"""
    if pd.isna(s):
        return None
    s = str(s).strip()
    s = unicodedata.normalize("NFKC", s)
    return s or None                     # 空文字は None 扱い

def normalize_phone(phone: str | None) -> str | None:
    """数字11桁（携帯）or10桁（固定）のみに整形"""
    if phone is None:
        return None
    digits = re.sub(r"\D", "", phone)    # 数字以外を除去
    if len(digits) in (10, 11):
        return digits
    return None                          # 桁数不正は欠損扱い

def normalize_date(date_str: str | None) -> pd.Timestamp | None:
    """文字列→Timestamp。変換不能なら NaT"""
    if date_str is None:
        return pd.NaT
    # pandas が自動判定できるよう errors="coerce"
    return pd.to_datetime(date_str, errors="coerce", dayfirst=False)

###############################################################################
# 1. データ読み込み
###############################################################################
DATA_DIR = Path(r"D:\Python\データクレンジング練習用")        # 生成済みファイルの場所
df_a = pd.read_csv(DATA_DIR / "customers_A.csv", dtype=str)
df_b = pd.read_csv(DATA_DIR / "customers_B.csv", dtype=str)

###############################################################################
# 2. 列名統一
###############################################################################
df_a.columns = ["customer_id", "name", "email", "phone",
                "address", "join_date"]                      # A は 6列

df_b = df_b.rename(columns={
    "顧客ID": "customer_id",
    "氏名": "name",
    "メールアドレス": "email",
    "電話番号": "phone",
    "住所": "address",
    "登録日": "join_date",
    "ポイント": "points"
})

# A には points 列が無いので追加しておく
df_a["points"] = None

###############################################################################
# 3. 正規化（文字・電話・日付）
###############################################################################
def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 文字列列の一括 NFKC 正規化
    for col in ["customer_id", "name", "email", "phone", "address"]:
        df[col] = df[col].apply(normalize_str)

    # 電話番号
    df["phone"] = df["phone"].apply(normalize_phone)

    # customer_id は int64 に
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")

    # join_date を Timestamp → ISO8601 文字列へ
    df["join_date"] = df["join_date"].apply(normalize_date)
    df["join_date"] = df["join_date"].dt.strftime("%Y-%m-%d")

    # points 数値化（欠損は 0）
    if "points" in df.columns:
        df["points"] = pd.to_numeric(df["points"], errors="coerce").fillna(0).astype("Int64")

    return df

df_a_clean = clean(df_a)
df_b_clean = clean(df_b)

###############################################################################
# 4. マージ & 重複解決
###############################################################################
# まず縦結合
merged = pd.concat([df_a_clean, df_b_clean], ignore_index=True)

# 重複判定キーをどう設計するかは業務要件次第。
# ここでは「customer_id + email」で完全一致した行は重複とみなして最初の1件を残す。
merged = (
    merged
    .sort_values(["customer_id", "points"], ascending=[True, False])  # 例: ポイントが高い方を優先
    .drop_duplicates(subset=["customer_id", "email"], keep="first")
    .reset_index(drop=True)
)

###############################################################################
# 5. 結果確認 & 出力
###############################################################################
print("=== Preview ===")
print(merged.head())

# DB アップロード用に新CSVを書き出すなら↓
OUTPUT_PATH = DATA_DIR / "customers_merged.csv"
merged.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
print(f"\nMerged file saved to: {OUTPUT_PATH}")

# append_batch.py
"""
追加データフォルダ内の未処理 CSV をクレンジングして customers_merged.csv に追記
  1. 追加データフォルダを走査
  2. processed_files.txt に存在しないファイルだけ取り込み
  3. クレンジング → マージ → 出力
Usage:
    python append_batch.py
"""

import re
import unicodedata
from pathlib import Path
import pandas as pd

# ───────────────────────────────────────────
# パス設定
# ───────────────────────────────────────────
BASE_DIR      = Path(r"D:/Python/データクレンジング練習用")
MASTER_CSV    = BASE_DIR / "customers_merged.csv"
ADD_DIR       = BASE_DIR / "追加データ"
PROCESSED_LOG = BASE_DIR / "processed_files.txt"
OUTPUT_CSV    = BASE_DIR / "customers_merged_updated.csv"

# ───────────────────────────────────────────
# 共通クレンジング関数
# ───────────────────────────────────────────
def normalize_str(s):
    if pd.isna(s):
        return None
    return unicodedata.normalize("NFKC", str(s).strip()) or None

def normalize_phone(p):
    if pd.isna(p):
        return None
    digits = re.sub(r"\D", "", str(p))
    return digits if len(digits) in (10, 11) else None

def normalize_date(d):
    ts = pd.to_datetime(d, errors="coerce")
    return ts.strftime("%Y-%m-%d") if not pd.isna(ts) else None

def clean(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "顧客ID": "customer_id",
        "氏名": "name",
        "メールアドレス": "email",
        "電話番号": "phone",
        "住所": "address",
        "登録日": "join_date",
        "ポイント": "points",
    }
    df = df.rename(columns=rename).copy()

    if "points" not in df.columns:
        df["points"] = None

    for col in ["customer_id", "name", "email", "phone", "address"]:
        df[col] = df[col].apply(normalize_str)

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df["phone"]       = df["phone"].apply(normalize_phone)
    df["join_date"]   = df["join_date"].apply(normalize_date)
    df["points"]      = pd.to_numeric(df["points"], errors="coerce").fillna(0).astype("Int64")
    return df

# ───────────────────────────────────────────
# メイン処理
# ───────────────────────────────────────────
def main():
    # ① 取り込み済みファイル一覧を読む
    already = set()
    if PROCESSED_LOG.exists():
        already = set(x.strip() for x in PROCESSED_LOG.read_text(encoding="utf-8").splitlines())

    # ② 追加データフォルダから未処理CSVを抽出
    add_files = [p for p in sorted(ADD_DIR.glob("*.csv")) if p.name not in already]
    if not add_files:
        print("新規ファイルがありません。処理を終了します。")
        return

    print(f"新規 CSV: {len(add_files)} 件 → {', '.join(p.name for p in add_files)}")

    # ③ マスターと追加分を読み込み・クレンジング
    master = clean(pd.read_csv(MASTER_CSV, dtype=str))
    add_df = pd.concat([clean(pd.read_csv(p, dtype=str)) for p in add_files], ignore_index=True)

    # ④ マージ ＋ 重複排除
    merged = (
        pd.concat([master, add_df], ignore_index=True)
          .sort_values(["customer_id", "points"], ascending=[True, False])
          .drop_duplicates(subset=["customer_id", "email"], keep="first")
          .reset_index(drop=True)
    )

    # ⑤ 保存
    merged.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"追記後レコード数: {len(merged)}  → {OUTPUT_CSV}")

    # ⑥ 取り込み済みログを更新
    with PROCESSED_LOG.open("a", encoding="utf-8") as log:
        for p in add_files:
            log.write(p.name + "\n")

    print("processed_files.txt を更新しました。")

if __name__ == "__main__":
    main()

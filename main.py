import os
import glob
import pandas as pd
from datetime import datetime
from tqdm import tqdm

print('自動集計を開始します')
now = datetime.now().strftime('%Y%m%d_%H:%M')
now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
# --- 設定 ---
data_dir = '../data'
result_dir = '../result'

# フォルダが存在しない場合は作成
os.makedirs(result_dir, exist_ok=True)

# 処理対象のファイルリスト取得
data_files = glob.glob(os.path.join(data_dir, '*.xlsx'))

# 今回の実行結果を格納するリスト
all_results_list = []


if not data_files:
    print("エラー: ../data フォルダ内に Excelファイルが見つかりません。")
else:
    for data_path in tqdm(data_files, desc="全体進捗"):
        try:
            # 検索・抽出条件の設定
            target_keyword = '欠点巻込連絡票'
            exclude_keyword = '原本' 
            new_columns = [
                '欠点No.', '発生面', '発生面1', '欠点位置（長手）', '幅位置(WS基準)',
                '発生条件1', '発生条件2', '発生条件3', '発生条件4', '欠点長さ', 
                'PH', 'ASI', '表面欠点1', '表面欠点2','表面欠点3','表面欠点4',
                'マーキング個数','欠点コード', '略号'
            ]

            # Excelファイルを読み込み
            with pd.ExcelFile(data_path, engine='openpyxl') as xls:
                # 対象シートの抽出
                target_sheets = [s for s in xls.sheet_names if (target_keyword in s) and (exclude_keyword not in s)]

                if not target_sheets:
                    print(f'\nSKIP: {os.path.basename(data_path)} に対象シートがありません。')
                    continue

                # --- 1. 明細データの集計 (略号カウント) ---
                dfs = []
                for sheet_name in target_sheets:
                    df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=20)
                    df = df.iloc[:, :19]
                    df.columns = new_columns
                    dfs.append(df)

                df_merge = pd.concat(dfs, ignore_index=True)
                df_counts = df_merge[['略号']].dropna()
                df_counts = df_counts['略号'].value_counts().to_frame().T

                # 指定項目のカウント（存在しない場合は0）
                target_cols = ['粗化欠け', '誤検出', '黒色汚れ', 'スリップ疵']
                df_new = pd.DataFrame(0, index=['count'], columns=target_cols)

                other_cols = [c for c in df_counts.columns if c not in target_cols]
                df_new['その他'] = df_counts[other_cols].sum(axis=1)
                
                for col in target_cols:
                    if col in df_counts.columns:
                        df_new[col] = df_counts[col].values
                
                # --- 2. ヘッダー情報の取得 ---
                df_info = pd.read_excel(
                    xls, 
                    usecols='A:D', 
                    skiprows=1, 
                    nrows=17, 
                    sheet_name=target_sheets[0],
                    dtype=str
                )
                
                df_info = df_info[["Unnamed: 0", "Unnamed: 3"]]
                df_info.columns = ["columnName", "data"]
                df_info["data"] = df_info["data"].str.rstrip()

                df_table = df_info.set_index("columnName").T
                # 各行に実行時のタイムスタンプを付与
                df_table["プログラム起動日時"] = now

                target_info_cols = ["生産番号", "品質番号", "工程番号", "トータル長さ", "欠点合計長さ","プログラム起動日時"]
                available_cols = [c for c in target_info_cols if c in df_table.columns]
                df_table = df_table[available_cols]

                # ヘッダーと集計結果を結合してリストに追加
                df_final_row = pd.concat([df_table.reset_index(drop=True), df_new.reset_index(drop=True)], axis=1)
                all_results_list.append(df_final_row)

        except Exception as e:
            print(f"\n【エラー発生】ファイル: {os.path.basename(data_path)}")
            print(f"内容: {e}")
            continue

    # --- 3. 全データの結合と保存 ---
    if all_results_list:
        # リストに溜まった全データ(現在のdataフォルダ分のみ)を結合
        df_combined = pd.concat(all_results_list, ignore_index=True)

        # 列順の整理
        final_columns = ["生産番号", "品質番号", "工程番号", "トータル長さ", "欠点合計長さ", '粗化欠け', '誤検出', '黒色汚れ', 'スリップ疵', 'その他', 'プログラム起動日時']
        
        # 存在する列のみでフィルタリング（念のため）
        existing_cols = [c for c in final_columns if c in df_combined.columns]
        df_combined = df_combined[existing_cols]
        
        if '工程番号' in df_combined.columns:
            df_combined['工程番号'] = df_combined['工程番号'].astype(str)

        # 保存ファイル名の生成
        result_file = os.path.join(result_dir, f'merged_result_{now_str}.xlsx')

        # 重複を排除してExcel保存
        df_combined.drop_duplicates().to_excel(result_file, index=False)
        print(f"\n集計が完了しました。")
        print(f"作成ファイル: {os.path.basename(result_file)}")
    else:
        print("\n対象となるデータが見つからなかったため、ファイルは作成されませんでした。")

print("\nすべての処理が終了しました。")

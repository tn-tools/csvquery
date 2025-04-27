import os
import json
import sys
import pandas as pd
import re

sep = ","

# 引数を受け取る関数
def process_files(arg, quote_type='"'):  # quote_typeを引数として追加（デフォルトはダブルクォート）
    def_folder_path = os.path.join(os.path.dirname(__file__), 'def')
    src_folder_path = os.path.join(os.path.dirname(__file__), 'src')

    if arg == "all":
        # allの場合、defディレクトリ内のすべてのJSONファイルを取得
        try:
            files = os.listdir(def_folder_path)
            json_files = [file for file in files if file.endswith(".json")]
            print("defフォルダ内のJSONファイル一覧:")
            for json_file in json_files:
                print(f"ファイル名: {json_file}")
                # JSONファイルの内容を表示
                json_file_path = os.path.join(def_folder_path, json_file)
                try:
                    with open(json_file_path, 'r') as f:
                        json_data = json.load(f)
                        # print(f"JSONファイルの内容: {json_data}")
                        process_query(json_data, src_folder_path, quote_type)  # クエリ処理関数を呼び出し
                except json.JSONDecodeError:
                    print(f"{json_file_path} の内容にエラーがあります。")
                check_csv_file(json_file, src_folder_path)
        except FileNotFoundError:
            print("defフォルダが見つかりませんでした。")
    else:
        # 第一引数がファイル名の場合、該当するJSONファイルを取得
        json_file_name = f"{arg}.json"
        json_file_path = os.path.join(def_folder_path, json_file_name)
        if os.path.exists(json_file_path):
            # print(f"該当するJSONファイル: {json_file_path}")
            try:
                with open(json_file_path, 'r') as f:
                    json_data = json.load(f)
                    # print(f"JSONファイルの内容:\n{json_data}")  # json_data の内容を表示
                    process_query(json_data, src_folder_path, quote_type)  # クエリ処理関数を呼び出し
            except json.JSONDecodeError:
                print(f"{json_file_path} の内容にエラーがあります。")
        else:
            print(f"{json_file_name} が {def_folder_path} に見つかりませんでした。")

# CSV存在確認
def check_csv_file(json_file_name, src_folder_path):
    # JSONファイル名から拡張子を取り除き、.csvに変更
    base_name = os.path.splitext(json_file_name)[0]
    csv_file_name = f"{base_name}.csv"
    # srcフォルダ内で該当するCSVファイルを確認
    csv_file_path = os.path.join(src_folder_path, csv_file_name)
    print(f"チェックするCSVファイルのパス: {csv_file_path}")  # 追加のデバッグ用出力
    if os.path.exists(csv_file_path):
        try:
            # pandasを使ってCSVファイルを読み込む
            df = pd.read_csv(csv_file_path)
            print(f"対応するCSVファイル: {csv_file_name}")
            print(f"CSVの内容:\n{df.head()}")  # 最初の5行を表示
        except Exception as e:
            print(f"CSVファイルの読み込み中にエラーが発生しました: {e}")
    else:
        print(f"{csv_file_name} が {src_folder_path} に見つかりませんでした。")

# クエリ処理
def process_query(json_data, src_folder_path, quote_type):
    # 'from' セクションから最初のテーブルを読み込む
    from_table = json_data.get("from", {}).get("t_name")
    from_alias = json_data.get("from", {}).get("as")
    from_csv = os.path.join(src_folder_path, f"{from_table}.csv")
    if not os.path.exists(from_csv):
        print(f"CSVファイル {from_csv} が見つかりません。")
        return
    df_from = pd.read_csv(from_csv)
    # カラム名を alias_カラム名 に変更
    df_from = df_from.rename(columns={col: f"{from_alias}_{col}" for col in df_from.columns})
    # テーブル別名マッピング
    table_alias = {from_alias: df_from}

    # join
    joins = json_data.get("join", [])
    for join in joins:
        join_type = join.get("join", "inner").lower()  # joinタイプ取得、デフォルトはinner
        join_table = join.get("t_name")
        join_alias = join.get("as")
        join_on = join.get("on")
        join_csv = os.path.join(src_folder_path, f"{join_table}.csv")
        if not os.path.exists(join_csv):
            print(f"CSVファイル {join_csv} が見つかりません。")
            return
        df_join = pd.read_csv(join_csv)
        df_join = df_join.rename(columns={col: f"{join_alias}_{col}" for col in df_join.columns})
        table_alias[join_alias] = df_join

        # 結合
        left_key, right_key = map(str.strip, join_on.split("="))
        left_table_alias, left_column = left_key.split(".")
        right_table_alias, right_column = right_key.split(".")

        left_column_renamed = f"{left_table_alias}_{left_column}"
        right_column_renamed = f"{right_table_alias}_{right_column}"

        df_left = table_alias[left_table_alias]
        df_right = table_alias[right_table_alias]

        # JOINタイプに応じた結合
        if join_type in ("outer", "left", "right", "inner"):
            how_type = join_type
        else:
            print(f"未知のjoinタイプです: {join_type}")
            return
        print("■■■" + join_type)
        # 結合前のデータフレームの内容を表示
        print("左テーブル（df_left）の内容:")
        print(df_left.head())  # head()で最初の数行を表示
        print("右テーブル（df_right）の内容:")
        print(df_right.head())  # head()で最初の数行を表示
        df_merged = pd.merge(
            df_left, df_right,
            left_on=left_column_renamed,
            right_on=right_column_renamed,
            how=how_type
        )
        # 結合後のデータを表示
        # print("結合後の結果（df_merged）:")
        # print(df_merged.head())  # ここでデータの最初の数行を表示
        table_alias[left_table_alias] = df_merged

    result_df = list(table_alias.values())[0]
    # 全部のJOIN後の結果
    result_df = list(table_alias.values())[0]

    # where
    if "where" in json_data:
        where_condition = json_data["where"]
        where_condition = convert_where_condition(where_condition)  # <= これを通す！
        result_df = result_df.query(where_condition)

    # select
    select_columns = []  # 結果に含めるカラム
    dst_c_name = []  # カラム名の実名
    dst_c_alias = []  # カラム名のエイリアス

    if "select" in json_data:
        for col in json_data["select"]:
            match = re.search(r'\s+as\s+', col, re.IGNORECASE)
            if match:
                c_name, c_alias = re.split(r'\s+as\s+', col, flags=re.IGNORECASE)
                c_name = c_name.strip()
                c_alias = c_alias.strip()
            else:
                c_name = col.strip()
                c_alias = col.strip()

            dst_c_name.append(c_name)
            dst_c_alias.append(c_alias)

            # table_alias でカラム名を特定して select_columns に追加
            for alias in table_alias:
                possible_name = f"{alias}_{c_name}"
                if possible_name in result_df.columns:
                    select_columns.append(possible_name)
                    break

    # select_columns にカラムがあれば、それに対応するデータを抽出
    if select_columns:
        result_df = result_df[select_columns]

    # 必要なカラムだけ拾い出し
    _underbar = [col.replace('.', '_') for col in dst_c_name]
    result_df = result_df[_underbar]
        
    # dst_c_alias に合わせてカラム名を変更
    if len(dst_c_alias) == len(result_df.columns):
        result_df.columns = dst_c_alias
    else:
        print(f"警告: dst_c_alias の長さ {len(dst_c_alias)} と result_df のカラム数 {len(result_df.columns)} が一致しません")

    # 出力
    print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
    print("クエリ結果: " + from_table)
    pd.set_option("display.max_columns", None)  # 全てのカラムを表示
    pd.set_option("display.width", None)  # 横幅の制限をなくす
    pd.set_option("display.max_rows", None)  # 行数の制限をなくす
    print(result_df)

    # 出力先のディレクトリが存在しない場合は作成
    output_dir = "./dst/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # カンマ区切りでCSVとして出力
    output_csv_path = output_dir + from_table + ".csv"
    result_df.to_csv(output_csv_path, sep=',', index=False, quotechar='"', quoting=1)  # 1 は CSV.QUOTE_MINIMAL 相当

# WHERE句の条件変換
def convert_where_condition(where_condition):
    where_condition = re.sub(r'(\b\w+)\.(\w+\b)', r'\1_\2', where_condition)
    where_condition = re.sub(r'(\w+)(?=\s*(==|!=|<|>|<=|>=)\s*[^0-9\s])', r'"\1"', where_condition)
    where_condition = where_condition.replace('is not', '__IS_NOT__')
    where_condition = where_condition.replace('is', '__IS__')
    where_condition = re.sub(r'(?<![<>=!])=(?![<>=])', '==', where_condition)
    where_condition = where_condition.replace('__IS_NOT__', 'is not')
    where_condition = where_condition.replace('__IS__', 'is')
    
    # AND, ORや()の表現に未対応
    # print(where_condition)    

    return where_condition

# メイン処理
def main():
    if len(sys.argv) != 2:
        print("引数が不足しています。ファイル名または'all'を指定してください。")
        sys.exit(1)

    # 引数を取得してカンマで分割(複数CSV対応)
    args = sys.argv[1].split(",")

    quote_type = '"'  # デフォルトはダブルクォート
    for arg in args:
        arg = arg.strip()  # 念のため余分な空白を除去
        process_files(arg, quote_type)

if __name__ == '__main__':
    main()

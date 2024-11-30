import pandas as pd
import os
import zipfile

def split_csv_custom(input_file, output_dir, lines_per_file_list):
    # 入力CSVの読み込み
    df = pd.read_csv(input_file)
    total_rows = len(df)
    start_index = 0

    # 出力先ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)

    # ファイル分割処理
    for file_num, lines_per_file in enumerate(lines_per_file_list, start=1):
        if start_index >= total_rows:
            break  # すでに全行を処理済みの場合は終了

        # 終了インデックスを計算
        end_index = min(start_index + lines_per_file, total_rows)
        chunk = df.iloc[start_index:end_index]

        # ファイルの保存
        output_file = os.path.join(output_dir, f"output_{file_num}.csv")
        chunk.to_csv(output_file, index=False)
        print(f"File {output_file} has been created.")

        # 次の開始インデックスを更新
        start_index = end_index

    # 残りの行がある場合、追加のファイルを作成
    if start_index < total_rows:
        chunk = df.iloc[start_index:]
        output_file = os.path.join(output_dir, f"output_{len(lines_per_file_list) + 1}.csv")
        chunk.to_csv(output_file, index=False)
        print(f"Remaining rows saved in {output_file}.")

def zip_output_files(output_dir):
    # ZIPファイルを作成
    zip_path = os.path.join(output_dir, 'split_files.zip')
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, _, files in os.walk(output_dir):
            for file in files:
                if file != 'split_files.zip':  # ZIP自身を含めない
                    zipf.write(os.path.join(root, file), file)
    return zip_path

# 使用例
if __name__ == '__main__':
    input_file = "input.csv"       # 元のCSVファイル
    output_dir = "output_files"    # 分割ファイルの保存先
    lines_per_file_list = [3, 5, 2]  # 各ファイルの行数（例）

    split_csv_custom(input_file, output_dir, lines_per_file_list)
    zip_path = zip_output_files(output_dir)
    print(f"All split files are zipped at: {zip_path}")

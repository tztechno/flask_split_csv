import pandas as pd
import os
import zipfile
from flask import Flask, request, render_template, send_from_directory

app = Flask(__name__)

# アップロードファイルを保存するフォルダ
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'output_files'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

# フォルダが存在しない場合、作成
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def split_csv_custom(input_file, output_dir, lines_per_file_list):
    # 入力CSVの読み込み
    df = pd.read_csv(input_file, encoding='shift-jis')
    total_rows = len(df)
    start_index = 0

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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return 'No file part'
    file = request.files['file']
    if file.filename == '':
        return 'No selected file'
    if file and file.filename.endswith('.csv'):
        # アップロードされたファイルを保存
        input_file = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(input_file)
        
        # フォームで指定された行数リストを取得（カンマ区切りで処理）
        lines_per_file_input = request.form['lines_per_file']
        lines_per_file_list = list(map(int, lines_per_file_input.split(',')))
        
        # CSVの分割処理
        split_csv_custom(input_file, app.config['OUTPUT_FOLDER'], lines_per_file_list)
        
        # 出力ファイルをZIP化
        zip_path = zip_output_files(app.config['OUTPUT_FOLDER'])
        
        # ZIPファイルをダウンロードリンクで返す
        return send_from_directory(app.config['OUTPUT_FOLDER'], 'split_files.zip', as_attachment=True)

    return 'Invalid file type'

if __name__ == '__main__':
    app.run(debug=True)

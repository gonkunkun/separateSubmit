# formatLog.py
import os
import glob
import datetime
import shutil
import pandas as pd
import csv
import pprint
from pathlib import Path

ROOT_DIR = "/Users/yutasasaki/Desktop/work/submit/root/"  # 作業ディレクトリ
INPUT_DIR = "./EAST-APS-*/domains/app*/submit/"           # 読み込むログが格納されているディレクトリ
SEPARATED_DIR = "./separatedSubmit/"                      # サブミットID毎のログを格納するディレクトリ
FILE_NAME = "{}submit_2020021?.csv"                       # 収集対象となるログのファイル名
ALL_DATA_FILE = "all_files.csv"                           # 上記ファイルのマージ先
AVERAGE_FILE = "average.csv"                              # 各サブミット毎の平均処理時間出力先


def main():
    os.chdir(ROOT_DIR)

    # 複数ファイルを1つのファイルにマージする
    merge_files()

    # サブミットID毎にログをまとめてファイルに出力する
    separate_file_by_submit()

    # サブミットID毎の平均処理時間を出力する
    # 外れ値を除外する
    calc_transaction_average()


# 複数ファイルを1つのファイルにマージする
def merge_files():
    print("merge_log_file()")
    print("collect_log_file()")

    # フォルダ中のパスを取得
    DATA_PATH = INPUT_DIR
    all_files = glob.glob(FILE_NAME.format(DATA_PATH))

    # 他にもファイル探索条件があるなら、上手く追記する
    all_files.extend(glob.glob("{}submit_20200220.csv".format(DATA_PATH)))

    pprint.pprint(all_files)

    # 該当する日付のファイルのみ検索

    # 対象となるフォルダパスのcsvを全てマージする
    list = []
    for file in all_files:
        list.append(pd.read_csv(file, header=None))
    df = pd.concat(list, sort=False)
    # サブミットIDでソートする
    df = df.sort_values(3)
    # print(df)

    # 作業用ディレクトリにファイルを出力する
    df.to_csv(ALL_DATA_FILE, encoding='utf_8', index=False)


# サブミットID毎にログをまとめてファイルに出力する
def separate_file_by_submit():
    print("separate_file_by_submit()")
    df = pd.read_csv(ALL_DATA_FILE)
    # print(df)
    # submitidでグループ分けする
    submit_groupby = df.groupby("3")
    submit_groupby.groups

    # グループ出力
    for member in submit_groupby:
        print(member[0])
        # print(member[1])
        member[1].to_csv(SEPARATED_DIR + str(member[0]) + ".csv",
                         encoding='utf_8', index=False)


# サブミットID毎の平均処理時間を出力する
# 外れ値を除外する
def calc_transaction_average():
    print("calc_transaction_average()")

    # 読み込み対象ファイル一覧を取得
    DATA_PATH = SEPARATED_DIR
    all_files = glob.glob("{}*.csv".format(DATA_PATH))

    # 1サブミット分のログを読み込む
    for file in all_files:
        df = pd.read_csv(file)

        # 外れ値を除外する（タイル値で）
        q10 = df.quantile(0.1)
        q90 = df.quantile(0.9)

        percentileDf = df[(df['4'] > q10[1]) & (df['4'] < q90[1])]

        print(percentileDf)
        # print(q10[1])
        # print(q90[1])

        # 2番目にサブミット処理時間が格納されているので、これを使用する
        mean = percentileDf.mean()
        average = mean[1]
        # サブミットIDはcsvの一要素から持ってくる
        submitId = df.loc[0][3]

        # csvに出力する
        with open(AVERAGE_FILE, 'a') as f:
            writer = csv.writer(f)
            writer.writerow([submitId, average])


if __name__ == '__main__':
    main()

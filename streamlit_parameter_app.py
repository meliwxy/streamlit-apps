import streamlit as st
import pandas as pd
import snowflake.connector
from io import BytesIO

st.set_page_config(page_title="Snowflake Parameter Tool", layout="wide", initial_sidebar_state="expanded")

# --- Sidebar: Snowflake credentials ---
st.sidebar.header("Snowflake 接続情報")
account = st.sidebar.text_input("Account Identifier", value="", placeholder="例：abc-xy12345")
user = st.sidebar.text_input("User Name", value="")
password = st.sidebar.text_input("Password", type="password")
st.sidebar.markdown("\uff0a個人アカウントをご利用の場合、Duo認証による承認が必要です。ご確認ください。")

if st.sidebar.button("接続"):
    try:
        conn = snowflake.connector.connect(user=user, password=password, account=account)
        st.session_state["conn"] = conn
        st.sidebar.success("接続成功")
    except Exception as e:
        st.sidebar.error(f"接続失敗: {e}")

st.title("Snowflake パラメータ確認ツール")

with st.expander("ツールの目的と概要", expanded=True):
    st.markdown("""
    本ツールは、Snowflake環境における各種設定パラメータを手軽に一括確認・出力するためのアプリです。

    `SHOW PARAMETERS` コマンドを個別に実行する必要なく、選択した対象をまとめて取得・確認・Excel出力できます。

    **活用例：**
    - 開発者・管理者による設定確認
    - クライアント説明用の資料作成
    - トラブル対応時の環境設定把握
    """)

# --- Parameter Retrieval ---
if "conn" in st.session_state:
    conn = st.session_state["conn"]
    cursor = conn.cursor()

    st.header("取得対象の選択")
    levels = st.multiselect("取得したいレベルを選んでください", ["ACCOUNT", "SESSION", "DATABASE", "WAREHOUSE"], default=["ACCOUNT", "SESSION"])

    database_list, warehouse_list = [], []

    if "DATABASE" in levels:
        cursor.execute("SHOW DATABASES")
        database_list = [row[1] for row in cursor.fetchall()]
        selected_dbs = st.multiselect("対象データベース", ["ALL"] + database_list, default="ALL")
    else:
        selected_dbs = []

    if "WAREHOUSE" in levels:
        cursor.execute("SHOW WAREHOUSES")
        raw_warehouses = cursor.fetchall()
        warehouse_list = [row[0] for row in raw_warehouses if row[0].isidentifier()]
        invalid_whs = [row[0] for row in raw_warehouses if not row[0].isidentifier()]
    
        selected_whs = st.multiselect("対象ウェアハウス（複数選択可）", ["ALL"] + warehouse_list, default=["ALL"])
    else:
        selected_whs = []

    def run_show_and_fetch(sql):
        cursor.execute(sql)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        rename_dict = {
            "key": "key / キー",
            "value": "value / 値",
            "default": "default / デフォルト",
            "level": "level / レベル",
            "description": "description / 説明",
            "type": "type / タイプ"
        }
        return df.rename(columns={col: rename_dict.get(col, col) for col in df.columns})

    def to_excel_multi_sheet(df_dict):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet, df in df_dict.items():
                df.to_excel(writer, sheet_name=sheet[:31], index=False, startrow=1, header=False)
                ws = writer.sheets[sheet[:31]]
                for col_num, value in enumerate(df.columns.values):
                    ws.write(0, col_num, value, writer.book.add_format({'bold': True}))
                col_widths = [50, 20, 30, 10, 80, 10]
                for i, width in enumerate(col_widths):
                    ws.set_column(i, i, width)
        output.seek(0)
        return output

    if st.button("パラメータを取得"):
        result_dict = {}
        failed_whs = []

        if "ACCOUNT" in levels:
            df = run_show_and_fetch("SHOW PARAMETERS IN ACCOUNT")
            result_dict["ACCOUNT"] = df

        if "SESSION" in levels:
            df = run_show_and_fetch("SHOW PARAMETERS IN SESSION")
            result_dict["SESSION"] = df

        if "DATABASE" in levels:
            targets = database_list if "ALL" in selected_dbs else selected_dbs
            for db in targets:
                df = run_show_and_fetch(f"SHOW PARAMETERS IN DATABASE {db}")
                result_dict[f"DATABASE_{db}"] = df

        if "WAREHOUSE" in levels:
            targets = warehouse_list if "ALL" in selected_whs else selected_whs
            failed_whs = []
            for wh in targets:
                try:
                    cursor.execute(f"ALTER WAREHOUSE {wh} RESUME")
                    cursor.execute(f"USE WAREHOUSE {wh}")
                    params = cursor.execute("SHOW PARAMETERS IN WAREHOUSE").fetchall()
        
                    st.subheader(f"パラメータ一覧: {wh}")
                    for param in params:
                        st.write(param)
        
                except Exception as e:
                    failed_whs.append((wh, str(e)))
        
            if failed_whs:
                st.warning("以下のウェアハウスのパラメータを取得できませんでした:")
                for wh, err in failed_whs:
                    st.text(f"{wh}: {err}")
        
            if invalid_whs:
                st.warning("無効な名前（SQL識別子ではない）として除外されたWAREHOUSE:")
                for wh in invalid_whs:
                    st.text(wh)

        
        if result_dict:
            st.success("パラメータ取得完了")
            excel_file = to_excel_multi_sheet(result_dict)
            st.download_button(
                "Excelとしてダウンロード",
                data=excel_file,
                file_name="snowflake_parameters.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download-excel"
            )
            for name, df in result_dict.items():
                st.subheader(name)
                st.dataframe(df, use_container_width=True)
            st.info("ダウンロードが完了しました")
        else:
            st.warning("選択された対象のパラメータを取得できませんでした")

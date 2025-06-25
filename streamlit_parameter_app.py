import streamlit as st
import pandas as pd
import snowflake.connector
from io import BytesIO

# --- Sidebar: Snowflake credentials ---
st.sidebar.header("Snowflake 接続情報")
st.sidebar.markdown("※ 個人アカウントをご利用の場合、Duo認証による承認が必要です。ご確認ください。")
st.sidebar.markdown("※ Account は Snowflake の **Account Identifier（例：abc-xy12345）** を入力してください。")

account = st.sidebar.text_input("Account Identifier", value="")
user = st.sidebar.text_input("User Name", value="")
password = st.sidebar.text_input("Password", type="password")

if st.sidebar.button("接続"):
    try:
        conn = snowflake.connector.connect(user=user, password=password, account=account)
        st.session_state["conn"] = conn
        st.sidebar.success("✅ 接続成功！")
    except Exception as e:
        st.sidebar.error(f"接続失敗: {e}")

# --- Title & Introduction ---
st.title("Snowflake パラメータ確認ツール")

with st.expander("🔍 ツールの目的と概要", expanded=True):
    st.markdown("""
    本ツールは、**Snowflake環境における各種設定パラメータ**を手軽に一括確認するためのアプリです。  
    通常は `SHOW PARAMETERS` コマンドを手動実行する必要がありますが、対象を選んで一括取得・出力できます。

    **用途例：**
    - 管理者や開発者による現在の環境設定の確認
    - クライアント説明資料としての活用
    - トラブルシューティングや監査時の設定確認
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
        selected_dbs = st.multiselect("対象データベース（複数選択可）", ["ALL"] + database_list, default="ALL")
    else:
        selected_dbs = []

    if "WAREHOUSE" in levels:
        cursor.execute("SHOW WAREHOUSES")
        warehouse_list = [row[1] for row in cursor.fetchall()]
        selected_whs = st.multiselect("対象ウェアハウス（複数選択可）", ["ALL"] + warehouse_list, default="ALL")
    else:
        selected_whs = []

    def run_show_and_fetch(sql):
        cursor.execute(sql)
        return pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

    def to_excel_multi_sheet(df_dict):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet, df in df_dict.items():
                df.to_excel(writer, sheet_name=sheet[:31], index=False, startrow=1, header=False)
                ws = writer.sheets[sheet[:31]]
                for col_num, value in enumerate(df.columns.values):
                    ws.write(0, col_num, value)
        output.seek(0)
        return output

    if st.button("パラメータを取得"):
        result_dict = {}

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
            for wh in targets:
                df = run_show_and_fetch(f"SHOW PARAMETERS IN WAREHOUSE {wh}")
                result_dict[f"WAREHOUSE_{wh}"] = df

        if result_dict:
            st.success("✅ パラメータ取得完了")
            for name, df in result_dict.items():
                st.subheader(f"{name}")
                st.dataframe(df, use_container_width=True, height=400)

            excel_file = to_excel_multi_sheet(result_dict)
            st.download_button("Excelとしてダウンロード", data=excel_file, file_name="snowflake_parameters.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("⚠️ 選択された対象のパラメータを取得できませんでした。")

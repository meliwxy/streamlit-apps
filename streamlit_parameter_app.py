import streamlit as st
import pandas as pd
import snowflake.connector
from io import BytesIO

# --- Sidebar: Snowflake credentials ---
st.sidebar.header("Snowflake アカウント接続")
account = st.sidebar.text_input("Account", value="your_account")
user = st.sidebar.text_input("User", value="your_username")
password = st.sidebar.text_input("Password", type="password")

# --- Main UI ---
st.title("Snowflake パラメータ出力ツール")

# Connect to Snowflake after button click
if st.sidebar.button("接続"):
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
        )
        st.session_state["conn"] = conn
        st.success("接続成功！")
    except Exception as e:
        st.error(f"接続失敗: {e}")

if "conn" in st.session_state:
    conn = st.session_state["conn"]
    cursor = conn.cursor()

    st.header("取得対象の選択")

    levels = st.multiselect("取得したいレベルを選んでください", ["ACCOUNT", "SESSION", "DATABASE", "WAREHOUSE"], default=["ACCOUNT", "SESSION"])

    database_list, warehouse_list = [], []

    if "DATABASE" in levels:
        cursor.execute("SHOW DATABASES")
        database_list = [row[1] for row in cursor.fetchall()]
        selected_dbs = st.multiselect("対象データベースを選択（複数選択可）", ["ALL"] + database_list, default="ALL")
    else:
        selected_dbs = []

    if "WAREHOUSE" in levels:
        cursor.execute("SHOW WAREHOUSES")
        warehouse_list = [row[1] for row in cursor.fetchall()]
        selected_whs = st.multiselect("対象ウェアハウスを選択（複数選択可）", ["ALL"] + warehouse_list, default="ALL")
    else:
        selected_whs = []

    def run_show_and_fetch(sql):
        cursor.execute(sql)
        return pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

    def to_excel_multi_sheet(df_dict):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet, df in df_dict.items():
                df.to_excel(writer, sheet_name=sheet[:31], index=False, startrow=2)
                ws = writer.sheets[sheet[:31]]
                ws.write("B1", "Parameter")
                header_format = writer.book.add_format({'bold': True})
                ws.write_row("B2", df.columns.tolist(), header_format)
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
            st.success("取得完了！")
            for name, df in result_dict.items():
                st.subheader(name)
                st.dataframe(df, use_container_width=True)

            excel_file = to_excel_multi_sheet(result_dict)
            st.download_button("Excelとしてダウンロード", data=excel_file, file_name="snowflake_parameters.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("選択された対象のパラメータを取得できませんでした。")

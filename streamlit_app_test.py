import streamlit as st
import pandas as pd
import re
import snowflake.connector
from io import BytesIO

# ---- Sidebar: Snowflake credentials ----
st.set_page_config(page_title="Snowflake Information Tool", layout="wide", initial_sidebar_state="expanded")

# --- Sidebar: Snowflake credentials ---
st.sidebar.header("Snowflake 接続設定")
account = st.sidebar.text_input("Account Identifier", value="", placeholder="例：abc-xy12345")
user = st.sidebar.text_input("User Name", value="")
password = st.sidebar.text_input("Password", type="password")
st.sidebar.markdown("\uff0a個人アカウントをご利用の場合、Duo認証による承認が必要です。ご確認ください。")

if "conn" not in st.session_state:
    st.session_state.conn = None

if st.sidebar.button("接続"):
    try:
        conn = snowflake.connector.connect(user=user, password=password, account=account)
        st.session_state["conn"] = conn
        st.sidebar.success("接続成功")
    except Exception as e:
        st.sidebar.error(f"接続失敗: {e}")

# ---- Main UI: Tabs ----
st.title("Snowflake 情報統合ツール")

with st.expander("ツールの目的と概要", expanded=True):
    st.markdown("""
    本ツールは、Snowflake環境における各種設定パラメータを手軽に一括確認・出力するためのアプリです。

    `SHOW PARAMETERS` コマンドを個別に実行する必要なく、選択した対象をまとめて取得・確認・Excel出力できます。

    **活用例：**
    - 開発者・管理者による設定確認
    - クライアント説明用の資料作成
    - トラブル対応時の環境設定把握
    """)

if st.session_state.conn:
    tabs = st.tabs(["パラメータ", "テーブル定義", "ロールと権限"])

    with tabs[0]:
        cursor = st.session_state["conn"].cursor()
        st.header("取得対象の選択")
        levels = st.multiselect("取得したいレベルを選んでください", ["ACCOUNT", "SESSION", "DATABASE", "WAREHOUSE"], default=["ACCOUNT", "SESSION"])

        database_list, warehouse_list = [], []

        if "DATABASE" in levels:
            cursor.execute("SHOW DATABASES")
            database_list = [row[1] for row in cursor.fetchall()]
            selected_dbs = st.multiselect("対象データベース", ["ALL"] + database_list, default="ALL")
        else:
            selected_dbs = []

        def escape_identifier(name):
            return f'"{name.replace("\"", "\"\"")}"'
        
        def run_show_and_fetch(sql):
            cursor.execute(sql)
            return cursor.fetchall()
        
        if "WAREHOUSE" in levels:
            cursor.execute("SHOW WAREHOUSES")
            raw_warehouses = cursor.fetchall()
            warehouse_list = [row[0] for row in raw_warehouses]
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
            
            failed_dbs = []
            if "DATABASE" in levels:
                targets = database_list if "ALL" in selected_dbs else selected_dbs
                for db in targets:
                    try:
                        safe_db = escape_identifier(db)
                        df = run_show_and_fetch(f"SHOW PARAMETERS IN DATABASE {safe_db}")
                        result_dict[f"DATABASE_{db}"] = df
                    except Exception as e:
                        failed_dbs.append((db, str(e)))
            if failed_dbs:
                st.warning("以下のデータベースのパラメータを取得できませんでした:")
                for db, err in failed_dbs:
                    st.text(f"{db}: {err}")

            if "WAREHOUSE" in levels:
                targets = warehouse_list if "ALL" in selected_whs else selected_whs
                for wh in targets:
                    try:
                        safe_wh = escape_identifier(wh)
                        try:
                            cursor.execute(f'ALTER WAREHOUSE {safe_wh} RESUME')
                        except:
                            pass
            
                        df = run_show_and_fetch(f'SHOW PARAMETERS IN WAREHOUSE {safe_wh}')
                        if df.empty:
                            raise ValueError("No parameter data returned")
            
                        result_dict[f"WAREHOUSE_{wh}"] = df
            
                    except Exception as e:
                        failed_whs.append((wh, str(e)))
            
                if failed_whs:
                    st.warning("以下のウェアハウスのパラメータを取得できませんでした:")
                    for wh, err in failed_whs:
                        st.text(f"{wh}: {err}")


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
            else:
                st.warning("選択された対象のパラメータを取得できませんでした")

    with tabs[1]:
        from snowflake.snowpark import Session
        import io

        # Set up Snowpark Session
        if "snowpark_session" not in st.session_state:
            try:
                connection_parameters = {
                    "account": account,
                    "user": user,
                    "password": password,
                    "role": "ACCOUNTADMIN",  
                    "warehouse": "default",  
                }
                st.session_state.snowpark_session = Session.builder.configs(connection_parameters).create()
            except Exception as e:
                st.error(f"Snowpark セッション作成失敗: {e}")
                st.stop()

        session = st.session_state.snowpark_session

        st.markdown("### 出力形式の選択")

        option = st.radio(
            "出力形式を選択してください",
            ("プレビュー表示", "Excelとしてダウンロード（全テーブル）", "Excelとしてダウンロード（選択テーブルのみ）"),
            index=0
        )

        if st.button("取得する"):
            with st.spinner("テーブル情報を取得中です..."):
                # 1. Collect all DB, exclude sample
                df_dbs = session.sql("SHOW DATABASES").to_pandas()
                df_dbs.columns = [str(col) for col in df_dbs.columns]
                db_name_col = df_dbs.columns[1] 
                database_names = df_dbs[db_name_col].tolist()
                database_names = [db for db in database_names if db.upper() != "SNOWFLAKE_SAMPLE_DATA"]

                # 2. Collect all tables
                df_all_tables = pd.DataFrame()
                for db in database_names:
                    try:
                        df = session.sql(f"""
                            SELECT table_catalog, table_schema, table_name
                            FROM {db}.information_schema.tables
                            WHERE table_type = 'BASE TABLE'
                        """).to_pandas()
                        df["table_catalog"] = db
                        df_all_tables = pd.concat([df_all_tables, df], ignore_index=True)
                    except:
                        st.warning(f"⚠️ データベース {db} の取得に失敗しました。")

                df_all_tables.columns = [col.lower() for col in df_all_tables.columns]
                table_entries = df_all_tables.to_dict("records")

                tables = []
                df_def_all = pd.DataFrame()

                # 3. Collect table definication & sample data
                for entry in table_entries:
                    db = entry["table_catalog"]
                    schema = entry["table_schema"]
                    tbl = entry["table_name"]
                    full_name = f"{db}.{schema}.{tbl}"

                    # Definition
                    try:
                        quoted_name = f'"{db}"."{schema}"."{tbl}"'
                        df_desc = session.sql(f"DESCRIBE TABLE {quoted_name}").to_pandas()
                        df_desc.columns = [str(i) for i in range(df_desc.shape[1])]
                        df_desc = df_desc.rename(columns={
                            "0": "column_name",
                            "1": "data_type",
                            "3": "nullable",
                            "5": "primary_key",
                            "9": "comment"
                        })
                        df_desc = df_desc[["column_name", "data_type", "nullable", "primary_key", "comment"]]

                        df_desc["database_name"] = db
                        df_desc["schema_name"] = schema
                        df_desc["table_name"] = tbl
                        df_def_all = pd.concat([df_def_all, df_desc], ignore_index=True)
                    except Exception as e:
                        df_columns = pd.DataFrame([["取得失敗"]], columns=["Error"])
                        st.warning(f"⚠️ 定義取得エラー（{full_name}）: {e}")

                    # Sample data
                    try:
                        df_sample = session.sql(f"SELECT * FROM {full_name} LIMIT 10").to_pandas()
                    except:
                        df_sample = pd.DataFrame([["取得失敗"]], columns=["Error"])

                    tables.append({
                        "full_name": full_name,
                        "sample_df": df_sample
                    })

                st.session_state["df_def_all"] = df_def_all
                st.session_state["tables"] = tables

                # 4. Output
        if "df_def_all" in st.session_state and not st.session_state["df_def_all"].empty:
            df_def_all = st.session_state["df_def_all"]
            tables = st.session_state["tables"]
            if option == "プレビュー表示":        
                st.success("✅ データベース構造と各テーブルの定義情報を以下に表示します。")
            
                # --- Tree view
                
                dot_lines = ["digraph G {", "rankdir=LR;", 'node [shape=box];']
                grouped = df_def_all.groupby(["database_name", "schema_name", "table_name"])
                
                db_schema_edges = set()
                schema_table_edges = set()
                
                for (db, schema, tbl), _ in grouped:
                    db_node = db
                    schema_node = f"{db}.{schema}"
                    table_node = f"{db}.{schema}.{tbl}"
                
                    if (db_node, schema_node) not in db_schema_edges:
                        dot_lines.append(f'"{db_node}" -> "{schema_node}"')
                        db_schema_edges.add((db_node, schema_node))
                
                    if (schema_node, table_node) not in schema_table_edges:
                        dot_lines.append(f'"{schema_node}" -> "{table_node}"')
                        schema_table_edges.add((schema_node, table_node))
                
                dot_lines.append("}")
                dot_source = "\n".join(dot_lines)

                with st.expander("### データベース構造（DB → スキーマ → テーブル）"):
                    st.graphviz_chart(dot_source)
            
                # --- Tabke definition
                with st.expander("### 各テーブルの定義書"):
                    for (db, schema, tbl), df_group in grouped:
                        full_name = f"{db}.{schema}.{tbl}"
                        df_show = df_group[["column_name", "data_type", "nullable", "primary_key", "comment"]].reset_index(drop=True)
                        st.markdown(f"#### {full_name}")
                        st.dataframe(df_show, use_container_width=True)
                # --- Sample data
                with st.expander("### 各テーブルのサンプルデータ"):
                    for table in tables:
                        st.markdown(f"#### テーブル： {table['full_name']}")
                        st.dataframe(table["sample_df"], use_container_width=True)

                st.markdown("""
                <style>
                .back-to-top-button {
                    position: fixed;
                    bottom: 40px;
                    right: 30px;
                    z-index: 100;
                }
                .back-to-top-button button {
                    padding: 10px 18px;
                    font-size: 14px;
                    background-color: #f0f0f0;
                    border: 1px solid #ccc;
                    border-radius: 6px;
                    cursor: pointer;
                }
                .back-to-top-button button:hover {
                    background-color: #e0e0e0;
                }
                </style>

                <div class="back-to-top-button">
                    <a href="#top">
                        <button>🔝 Back to Top</button>
                    </a>
                </div>
                """, unsafe_allow_html=True)


            elif option == "Excelとしてダウンロード（全テーブル）":
                output = io.BytesIO()

                with pd.ExcelWriter(output, engine='openpyxl') as writer:

                    # Sheet1
                    df_def_all_csv = df_def_all[
                        ["database_name", "schema_name", "table_name", "column_name", "data_type", "nullable", "primary_key", "comment"]
                    ].sort_values(["database_name", "schema_name", "table_name", "column_name"])

                    df_def_all_csv.to_excel(writer, index=False, sheet_name="All_Tables_Overview")

                    # Sheet2〜
                    grouped = df_def_all.groupby(["database_name", "schema_name", "table_name"])
                    for (db, schema, tbl), df_group in grouped:
                        sheet_name = f"{tbl}"[:31] 
                        df_table_def = df_group[
                            ["column_name", "data_type", "nullable", "primary_key", "comment"]
                        ]
                        df_table_def.to_excel(writer, index=False, sheet_name=sheet_name)

                st.download_button(
                    label="ダウンロード",
                    data=output.getvalue(),
                    file_name="table_definitions.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            elif option == "Excelとしてダウンロード（選択テーブルのみ）":
                # 👇 multiselect 的数据源也从 session 取
                all_table_names = df_def_all[["database_name", "schema_name", "table_name"]].drop_duplicates()
                all_table_names["full_name"] = (
                    all_table_names["database_name"] + "." +
                    all_table_names["schema_name"] + "." +
                    all_table_names["table_name"]
                )

                selected_tables = st.multiselect(
                    "出力対象のテーブルを選択してください",
                    options=all_table_names["full_name"].tolist(),
                    key="selected_tables_key"
                )

                if selected_tables:
                    output = io.BytesIO()
                    df_def_all["full_name"] = (
                        df_def_all["database_name"] + "." +
                        df_def_all["schema_name"] + "." +
                        df_def_all["table_name"]
                    )
                    df_def_selected = df_def_all[df_def_all["full_name"].isin(selected_tables)]

                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_def_selected_csv = df_def_selected[
                            ["database_name", "schema_name", "table_name", "column_name", "data_type", "nullable", "primary_key", "comment"]
                        ].sort_values(["database_name", "schema_name", "table_name", "column_name"])
                        df_def_selected_csv.to_excel(writer, index=False, sheet_name="Selected_Tables_Overview")

                        grouped = df_def_selected.groupby(["database_name", "schema_name", "table_name"])
                        for (db, schema, tbl), df_group in grouped:
                            sheet_name = f"{tbl}"[:31]
                            df_table_def = df_group[["column_name", "data_type", "nullable", "primary_key", "comment"]]
                            df_table_def.to_excel(writer, index=False, sheet_name=sheet_name)

                    st.download_button(
                        label="選択テーブルをダウンロード",
                        data=output.getvalue(),
                        file_name="selected_table_definitions.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

    with tabs[2]:
        st.header("ロールと権限の一覧")

else:
    st.warning("まず左のサイドバーでSnowflakeに接続してください。")

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
    本ツールは、**Snowflakeアカウント内の構成情報・パラメータ・ロール権限をGUIで統合的に取得・出力**できる管理支援ツールです。

    ### 主な機能

    - **パラメータ出力**  
      アカウント・セッション・データベース（全体または選択）・ウェアハウス（全体または選択）のパラメータ設定を確認・Excel出力可能。

    - **テーブル定義出力**  
      アカウント内のすべてのデータベース構造を横断的に確認。各テーブルの定義情報（カラム名、データ型、NULL可否、主キー、コメント）およびサンプルデータを取得し、Excel出力可能。

    - **ロール権限一覧**  
      各データベース・スキーマ・テーブルに対するロール権限の付与状況を階層別に可視化。対象オブジェクトを選択して、詳細を確認・Excel出力可能。

    ※ すべての出力内容は、画面プレビューまたはExcel形式でダウンロード可能です。
    """)

if "conn" not in st.session_state:
    st.session_state.conn = None

if st.session_state.conn:
    tabs = st.tabs(["パラメータ設定", "テーブル定義書", "ロール権限一覧"])

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
            with st.spinner("⏳ パラメータ情報を取得中..."):

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
        cursor = session._conn._cursor

        st.markdown("### テーブル定義の取得と出力")

        # Step 1: データベース選択
        df_dbs = session.sql("SHOW DATABASES").to_pandas()
        all_dbs = df_dbs[df_dbs.columns[1]].tolist()
        db_options = ["ALL"] + all_dbs
        selected_dbs = st.multiselect("データベースを選択", db_options, default=["ALL"])
        active_dbs = all_dbs if "ALL" in selected_dbs or not selected_dbs else selected_dbs

        # Step 2: スキーマ選択
        schema_display = []
        schema_map = {}
        for db in active_dbs:
            try:
                df_schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {db}").to_pandas()
                schemas = df_schemas[df_schemas.columns[1]].tolist()
                for s in schemas:
                    full = f"{db}.{s}"
                    schema_display.append(full)
                    schema_map[full] = (db, s)
            except:
                st.warning(f"{db} のスキーマ取得失敗")

        schema_options = ["ALL"] + schema_display
        selected_schemas = st.multiselect("スキーマを選択", schema_options, default=["ALL"])
        active_schemas = list(schema_map.values()) if "ALL" in selected_schemas else [schema_map[s] for s in selected_schemas if s in schema_map]

        # Step 3: テーブル選択
        table_display = []
        for db, schema in active_schemas:
            try:
                df_tbls = session.sql(f"SHOW TABLES IN SCHEMA {db}.{schema}").to_pandas()
                tables = df_tbls[df_tbls.columns[1]].tolist()
                for t in tables:
                    table_display.append(f"{db}.{schema}.{t}")
            except:
                st.warning(f"{db}.{schema} のテーブル取得失敗")

        table_options = ["ALL"] + table_display
        selected_tables = st.multiselect("テーブルを選択", table_options, default=["ALL"])
        active_tables = [t for t in table_display if t != "ALL"] if "ALL" in selected_tables else selected_tables

        # Step 4: 取得実行
        if st.button("定義情報を取得"):
            with st.spinner("⏳ テーブル定義を取得中..."):
            df_def_all = pd.DataFrame()
            for tbl in active_tables:
                try:
                    db, schema, tbl_name = tbl.split(".")
                    quoted = f'"{db}"."{schema}"."{tbl_name}"'
                    df_desc = session.sql(f"DESCRIBE TABLE {quoted}").to_pandas()
                    df_desc.columns = [str(i) for i in range(df_desc.shape[1])]
                    df_desc = df_desc.rename(columns={
                        "0": "column_name",
                        "1": "data_type",
                        "3": "nullable",
                        "5": "primary_key",
                        "9": "comment"
                    })[["column_name", "data_type", "nullable", "primary_key", "comment"]]
                    df_desc["database"] = db
                    df_desc["schema"] = schema
                    df_desc["table"] = tbl_name
                    df_def_all = pd.concat([df_def_all, df_desc], ignore_index=True)
                except Exception as e:
                    st.warning(f"{tbl} の定義取得失敗: {e}")

            if not df_def_all.empty:
                st.dataframe(df_def_all)

                output = io.BytesIO()
                df_def_all.astype(str).to_excel(output, index=False)
                st.download_button("Excelでダウンロード", data=output.getvalue(), file_name="table_definitions.xlsx")

    with tabs[2]:
        st.markdown("### データベース・スキーマ・テーブルの権限一覧")

        conn = st.session_state.get("conn")
        if not conn:
            st.warning("Snowflake に接続してください。")
            st.stop()

        cursor = conn.cursor()

        # ---- データベース選択 ----
        cursor.execute("SHOW DATABASES")
        all_dbs = [row[1] for row in cursor.fetchall()]
        dbs_display = ["ALL"] + all_dbs
        selected_dbs = st.multiselect("データベースを選択", dbs_display, default=["ALL"])
        active_dbs = all_dbs if "ALL" in selected_dbs or not selected_dbs else selected_dbs

        # ---- スキーマ選択 ----
        schema_display = []
        schema_map = {}
        for db in active_dbs:
            try:
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
                schemas = [row[1] for row in cursor.fetchall()]
                for schema in schemas:
                    full = f"{db}.{schema}"
                    schema_display.append(full)
                    schema_map[full] = (db, schema)
            except:
                st.warning(f"{db} のスキーマ取得に失敗しました。")

        schema_display = ["ALL"] + schema_display
        selected_schemas = st.multiselect("スキーマを選択", schema_display)
        active_schemas = list(schema_map.values()) if "ALL" in selected_schemas else [
            schema_map[s] for s in selected_schemas if s in schema_map
        ]

        # ---- テーブル選択 ----
        table_display = []
        for db, schema in active_schemas:
            try:
                cursor.execute(f"SHOW TABLES IN SCHEMA {db}.{schema}")
                tables = [row[1] for row in cursor.fetchall()]
                for tbl in tables:
                    table_display.append(f"{db}.{schema}.{tbl}")
            except:
                st.warning(f"{db}.{schema} のテーブル取得に失敗しました。")

        table_display = ["ALL"] + table_display
        selected_tables = st.multiselect("テーブルを選択", table_display)
        active_tables = [t for t in table_display if t != "ALL"] if "ALL" in selected_tables else selected_tables

        # ---- 実行 & 表示 ----
        if st.button("権限情報を取得"):
            with st.spinner("⏳ 権限情報を取得中..."):
            grant_results = {}

            # DBレベル
            for db in active_dbs:
                try:
                    cursor.execute(f"SHOW GRANTS ON DATABASE {db}")
                    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                    grant_results[f"{db} [DATABASE]"] = df
                except:
                    st.warning(f"⚠️ DATABASE {db} のGRANT取得失敗")

            # SCHEMAレベル
            for db, schema in active_schemas:
                try:
                    cursor.execute(f"SHOW GRANTS ON SCHEMA {db}.{schema}")
                    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                    grant_results[f"{db}.{schema} [SCHEMA]"] = df
                except:
                    st.warning(f"⚠️ SCHEMA {db}.{schema} のGRANT取得失敗")

            # TABLEレベル
            for tbl_full in active_tables:
                try:
                    cursor.execute(f"SHOW GRANTS ON TABLE {tbl_full}")
                    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                    grant_results[f"{tbl_full} [TABLE]"] = df
                except:
                    st.warning(f"⚠️ TABLE {tbl_full} のGRANT取得失敗")

            # 表示 & ダウンロード
            for name, df in grant_results.items():
                st.subheader(f"{name}")
                st.dataframe(df)

            if grant_results:
                from io import BytesIO
                import re
                used_sheet_names = set()

                def safe_sheet_name(name):
                    name = re.sub(r'[:\\/?*\[\]]', '_', name)
                    name = name[:31]
                    base = name
                    i = 1
                    while name in used_sheet_names:
                        name = f"{base[:28]}_{i}"
                        i += 1
                    used_sheet_names.add(name)
                    return name

                excel_io = BytesIO()
                with pd.ExcelWriter(excel_io, engine="openpyxl") as writer:
                    for name, df in grant_results.items():
                        sheet_name = safe_sheet_name(name)
                        try:
                            df.astype(str).to_excel(writer, index=False, sheet_name=sheet_name)
                        except Exception as e:
                            st.warning(f"❌ Excel出力エラー（{sheet_name}）: {e}")
                st.download_button("Excelでダウンロード", data=excel_io.getvalue(), file_name="object_grants_by_level.xlsx")

else:
    st.warning("まず左のサイドバーでSnowflakeに接続してください。")

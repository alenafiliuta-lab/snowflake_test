import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(page_title="Snowflake DB Clone Tool", layout="centered")
st.title("🏁 Snowflake DB Clone Tool")


with st.form("clone_form"):
    source_db = st.text_input("Clone from DB (source)", value="MY_DB").upper()
    target_db = st.text_input("Target DB name", value="DEV_CLONE").upper()
    owner_role = st.text_input("Owner role (development)", value="DEVELOPER_ROLE").upper()
    readonly_role = st.text_input("Read-only role", value="READONLY_ROLE").upper()
    
    submitted = st.form_submit_button("Clone & Setup Permissions")
if submitted:
    if not source_db or not target_db or not owner_role or not readonly_role:
        st.error("All fields are required.")
        st.stop()

    try:
        st.info("Cloning database...")
        clone_sql = f"""
            CREATE DATABASE IF NOT EXISTS {target_db}
            CLONE {source_db};
        """
        session.sql(clone_sql).collect()
        st.success(f"✅ Database '{target_db}' cloned from '{source_db}'")

        st.info("Granting all privileges to Developer role...")
        session.sql(f"GRANT OWNERSHIP ON DATABASE {target_db} TO ROLE {owner_role} COPY CURRENT GRANTS").collect()
        session.sql(f"GRANT ALL PRIVILEGES ON DATABASE {target_db} TO ROLE {owner_role}").collect()
        session.sql(f"GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE {target_db} TO ROLE {owner_role}").collect()
        session.sql(f"GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE {target_db} TO ROLE {owner_role}").collect()
        session.sql(f"GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE {target_db} TO ROLE {owner_role}").collect()
        session.sql(f"GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE {target_db} TO ROLE {owner_role}").collect()
        session.sql(f"GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE {target_db} TO ROLE {owner_role}").collect()
        session.sql(f"GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE {target_db} TO ROLE {owner_role}").collect()
        st.success(f"🔑 Full privileges granted to {owner_role}")

    
        st.info("Granting READONLY privileges to Analyst role...")
        session.sql(f"GRANT USAGE ON DATABASE {target_db} TO ROLE {readonly_role}").collect()
        session.sql(f"GRANT USAGE ON ALL SCHEMAS IN DATABASE {target_db} TO ROLE {readonly_role}").collect()
        session.sql(f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {target_db} TO ROLE {readonly_role}").collect()
        session.sql(f"GRANT SELECT ON ALL TABLES IN DATABASE {target_db} TO ROLE {readonly_role}").collect()
        session.sql(f"GRANT SELECT ON ALL VIEWS IN DATABASE {target_db} TO ROLE {readonly_role}").collect()
        session.sql(f"GRANT SELECT ON FUTURE TABLES IN DATABASE {target_db} TO ROLE {readonly_role}").collect()
        session.sql(f"GRANT SELECT ON FUTURE VIEWS IN DATABASE {target_db} TO ROLE {readonly_role}").collect()
        st.success(f"🔐 Read-only permissions granted to {readonly_role}")

    except Exception as e:
        st.error(f"Error: {e}")
# SE363 – Phát triển ứng dụng trên nền tảng dữ liệu lớn
# Khoa Công nghệ Phần mềm – Trường Đại học Công nghệ Thông tin (FSE-UIT)

import time

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

# ------------------------
# Cấu hình kết nối PostgreSQL
# ------------------------
DB_CONFIG = {
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",  # dùng tên service Docker
    "port": 5432,
    "database": "airflow",
}

ASPECTS = [
    "Price",
    "Shipping",
    "Outlook",
    "Quality",
    "Size",
    "Shop_Service",
    "General",
    "Others",
]
SENTIMENTS = ["POS", "NEU", "NEG"]


# ------------------------
# Hàm load dữ liệu an toàn
# ------------------------
@st.cache_data(ttl=5)
def load_data():
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    try:
        conn = engine.raw_connection()
        try:
            df = pd.read_sql(
                "SELECT * FROM absa_results ORDER BY RANDOM() LIMIT 300", conn
            )
        finally:
            conn.close()
        return df
    except Exception as e:
        st.warning(f"⚠️ Không thể kết nối đến PostgreSQL: {e}")
        return pd.DataFrame()


# ------------------------
# Giao diện chính
# ------------------------
st.set_page_config(page_title="ABSA Streaming Dashboard", layout="wide")
st.title("📊 Real-time ABSA Social Listening Dashboard")
st.caption("Minh hoạ pipeline Kafka → Spark → PostgreSQL → Streamlit (CNPM – UIT)")

# ========================
# Auto-refresh mỗi 5 giây
# ========================
st_autorefresh(interval=5 * 1000, limit=None, key="auto_refresh")

# ------------------------
# Lấy dữ liệu
# ------------------------
df = load_data()

if df.empty:
    st.warning(
        "⏳ Chưa có dữ liệu trong bảng `absa_results`. Hãy đảm bảo producer và consumer đang chạy."
    )
else:
    st.subheader("📝 Dữ liệu gần đây")
    st.dataframe(df.tail(10), use_container_width=True)

    st.subheader("📈 Thống kê cảm xúc theo khía cạnh")

    # ✅ Checkbox cho sentiment "NONE"
    include_none = st.checkbox("🔹 Thống kê thêm sentiment 'NONE'", value=False)

    # ------------------------
    # Tính thống kê cảm xúc
    # ------------------------
    aspect_counts = []
    for asp in ASPECTS:
        if asp not in df.columns:
            continue

        # Cập nhật danh sách sentiments
        sentiments = SENTIMENTS + ["NONE"] if include_none else SENTIMENTS

        # Nếu cột có giá trị None / NaN thì replace bằng "NONE"
        counts = df[asp].fillna("NONE").value_counts().reindex(sentiments, fill_value=0)

        for sent, cnt in counts.items():
            aspect_counts.append({"Aspect": asp, "Sentiment": sent, "Count": cnt})

    df_stats = pd.DataFrame(aspect_counts)

    # ------------------------
    # Vẽ biểu đồ
    # ------------------------
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🔹 Biểu đồ tổng hợp cảm xúc theo khía cạnh")
        fig_bar = px.bar(
            df_stats,
            x="Aspect",
            y="Count",
            color="Sentiment",
            color_discrete_map={
                "POS": "#33cc33",
                "NEU": "#cccc00",
                "NEG": "#ff5050",
                "NONE": "#999999",
            },
            barmode="group",
            text_auto=True,
        )
        st.plotly_chart(fig_bar, use_container_width=True, key="bar_chart")

    with col2:
        st.markdown("#### 🔹 Tỉ lệ cảm xúc tích cực / trung tính / tiêu cực")
        df_total = df_stats.groupby("Sentiment")["Count"].sum().reset_index()
        fig_pie = px.pie(
            df_total,
            names="Sentiment",
            values="Count",
            color="Sentiment",
            color_discrete_map={
                "POS": "#33cc33",
                "NEU": "#cccc00",
                "NEG": "#ff5050",
                "NONE": "#999999",
            },
            hole=0.3,
        )
        st.plotly_chart(fig_pie, use_container_width=True, key="pie_chart")

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import folium
from streamlit_folium import st_folium
import math

# ==============================
# CONFIGURAÇÕES BÁSICAS
# ==============================
st.set_page_config(page_title="Boletim Meteorológico", page_icon="🌦️", layout="wide")

st.title("🌦️ Boletim Meteorológico - Previsão GEFS")
st.caption("Dados processados automaticamente a partir do modelo GEFS (NOAA/S3)")

# ==============================
# LEITURA DOS DADOS
# ==============================
df_prev = pd.read_csv("data/previsao_diaria.csv")
df_mun = pd.read_csv("config/municipios.csv", sep="|")

# Corrige precipitação
df_prev["tp"] = df_prev["tp"] / 1000  # converte para mm

# Junta coordenadas
df = df_prev.merge(df_mun, on=["municipio", "uf"], how="left")

# Calcula velocidade e direção do vento
df["vento_vel"] = np.sqrt(df["u10"]**2 + df["v10"]**2)
df["vento_dir_rad"] = np.arctan2(df["u10"], df["v10"])
df["vento_dir_deg"] = (df["vento_dir_rad"] * 180 / np.pi + 180) % 360  # converte para graus (0–360)

# ==============================
# FILTRO DE MUNICÍPIO
# ==============================
muni = st.selectbox("🏙️ Escolha o município", sorted(df["municipio"].unique()))
df_sel = df[df["municipio"] == muni].copy()

# ==============================
# MÉTRICAS PRINCIPAIS
# ==============================
col1, col2, col3 = st.columns(3)

col1.metric("🌡️ Temperatura Média (°C)", f"{df_sel.t2m.mean():.1f}")
col2.metric("☔ Chuva Total (mm)", f"{df_sel.tp.sum():.1f}")
col3.metric("💨 Vel. Média do Vento (m/s)", f"{df_sel.vento_vel.mean():.2f}")

# ==============================
# GRÁFICO DE TEMPERATURA
# ==============================
st.markdown("### 📈 Variação da Temperatura Diária")
fig_temp = px.line(
    df_sel, x="data_dia", y="t2m", markers=True,
    labels={"data_dia": "Data", "t2m": "Temperatura (°C)"},
    title="Temperatura Diária (°C)"
)
fig_temp.update_traces(line_color="#FF6347", fill="tozeroy")
st.plotly_chart(fig_temp, use_container_width=True)

# ==============================
# GRÁFICO DE PRECIPITAÇÃO
# ==============================
st.markdown("### ☔ Precipitação Diária")
fig_tp = px.bar(
    df_sel, x="data_dia", y="tp",
    labels={"data_dia": "Data", "tp": "Precipitação (mm)"},
    title="Precipitação Diária (mm)"
)
fig_tp.update_traces(marker_color="#1E90FF")
st.plotly_chart(fig_tp, use_container_width=True)

# ==============================
# GRÁFICO COMBINADO (Temp x Chuva)
# ==============================
st.markdown("### 🌡️☔ Temperatura x Precipitação")
fig_combo = px.bar(
    df_sel, x="data_dia", y="tp", labels={"tp": "Chuva (mm)"}
)
fig_combo.add_scatter(
    x=df_sel["data_dia"], y=df_sel["t2m"], mode="lines+markers",
    name="Temperatura (°C)", yaxis="y2", line=dict(color="#FF6347")
)
fig_combo.update_layout(
    title="Comparativo: Temperatura e Chuva",
    yaxis=dict(title="Precipitação (mm)"),
    yaxis2=dict(title="Temperatura (°C)", overlaying="y", side="right")
)
st.plotly_chart(fig_combo, use_container_width=True)

# ==============================
# 🌬️ GRÁFICOS DE VENTO
# ==============================
st.markdown("### 🌬️ Dinâmica dos Ventos (U10/V10)")

col_v1, col_v2 = st.columns(2)

# Gráfico de velocidade do vento
fig_vel = px.line(
    df_sel, x="data_dia", y="vento_vel", markers=True,
    labels={"data_dia": "Data", "vento_vel": "Velocidade (m/s)"},
    title="Velocidade Média do Vento (m/s)"
)
fig_vel.update_traces(line_color="#00BFFF", fill="tozeroy")
col_v1.plotly_chart(fig_vel, use_container_width=True)

# Gráfico de direção do vento
fig_dir = px.scatter_polar(
    df_sel,
    r="vento_vel",
    theta="vento_dir_deg",
    color="vento_vel",
    color_continuous_scale="icefire",
    title="Rosa dos Ventos (Direção e Intensidade)"
)
fig_dir.update_layout(
    polar=dict(
        radialaxis=dict(visible=True, range=[0, df_sel["vento_vel"].max() + 1])
    )
)
col_v2.plotly_chart(fig_dir, use_container_width=True)

# ==============================
# MAPA INTERATIVO
# ==============================
st.markdown("### 🗺️ Localização do Município")

lat, lon = df_sel["lat"].iloc[0], df_sel["lon"].iloc[0]
m = folium.Map(location=[lat, lon], zoom_start=7, tiles="CartoDB Positron")

tooltip = (
    f"<b>{muni} ({df_sel['uf'].iloc[0]})</b><br>"
    f"Temp. média: {df_sel.t2m.mean():.1f} °C<br>"
    f"Chuva acumulada: {df_sel.tp.sum():.1f} mm<br>"
    f"Vento médio: {df_sel.vento_vel.mean():.2f} m/s"
)

folium.CircleMarker(
    location=[lat, lon],
    radius=12,
    color="red",
    fill=True,
    fill_opacity=0.7,
    tooltip=tooltip
).add_to(m)

st_folium(m, width=700, height=400)

# ==============================
# DOWNLOAD DOS DADOS
# ==============================
st.markdown("### 💾 Baixar dados da previsão")

col_dl1, col_dl2 = st.columns(2)

# CSV do município selecionado
csv_filtered = df_sel.to_csv(index=False).encode("utf-8")
col_dl1.download_button(
    label=f"⬇️ Baixar dados de {muni}",
    data=csv_filtered,
    file_name=f"previsao_{muni.lower().replace(' ', '_')}.csv",
    mime="text/csv"
)

# CSV completo (todos os municípios)
csv_all = df.to_csv(index=False).encode("utf-8")
col_dl2.download_button(
    label="⬇️ Baixar todos os municípios",
    data=csv_all,
    file_name="previsao_completa.csv",
    mime="text/csv"
)



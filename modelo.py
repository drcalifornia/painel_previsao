import xarray as xr
import pandas as pd
import numpy as np
import s3fs
import tempfile
import shutil
import os
from datetime import datetime, timedelta
from tqdm import tqdm
import sys

# ==============================
# CONFIGURAÇÕES BÁSICAS
# ==============================
RUN_DATE = "20251210"         # data do ciclo (ex.: hoje)
HOUR = "00"                   # ciclo 00Z
PREFIX = f"gefs.{RUN_DATE}/{HOUR}/atmos/pgrb2ap5"
MUNICIPIOS_CSV = "config/municipios.csv"  # lat, lon, nome, uf

# ==============================
# FUNÇÕES AUXILIARES
# ==============================

def abrir_subset(local_path, filtro):
    """Abre apenas uma variável do GRIB conforme o filtro especificado."""
    try:
        return xr.open_dataset(local_path, engine="cfgrib",
                               backend_kwargs={"filter_by_keys": filtro})
    except Exception:
        return None

def abrir_gefs_grib(forecast_hour):
    """Baixa e abre um arquivo GRIB do GEFS via S3, retornando um Dataset com t2m, tp, u10 e v10."""
    fs = s3fs.S3FileSystem(anon=True)
    path_s3 = f"s3://noaa-gefs-pds/{PREFIX}/geavg.t{HOUR}z.pgrb2a.0p50.f{forecast_hour:03d}"

    with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as tmp:
        local_path = tmp.name
        try:
            with fs.open(path_s3, "rb") as fsrc:
                shutil.copyfileobj(fsrc, tmp)
        except Exception as e:
            print(f"❌ Falha ao baixar f{forecast_hour:03d}: {e}")
            return None

    try:
        ds_t2m = abrir_subset(local_path, {"shortName": "2t", "typeOfLevel": "heightAboveGround", "level": 2})
        ds_tp  = abrir_subset(local_path, {"shortName": "tp", "typeOfLevel": "surface"})
        ds_u10 = abrir_subset(local_path, {"shortName": "10u", "typeOfLevel": "heightAboveGround", "level": 10})
        ds_v10 = abrir_subset(local_path, {"shortName": "10v", "typeOfLevel": "heightAboveGround", "level": 10})

        dsets = [d for d in [ds_t2m, ds_tp, ds_u10, ds_v10] if d is not None]
        if not dsets:
            print(f"⚠️ Nenhum dado encontrado em f{forecast_hour:03d}")
            return None

        ds = xr.merge(dsets, compat="override")

        rename_map = {"2t": "t2m", "10u": "u10", "10v": "v10"}
        ds = ds.rename({k: v for k, v in rename_map.items() if k in ds})

        return ds

    except Exception as e:
        print(f"Erro ao abrir f{forecast_hour:03d}: {e}")
        return None

def extrair_para_municipios(ds, municipios):
    """Interpola bilinearmente o dataset (lat, lon) para as coordenadas dos municípios."""
    if ds.latitude[0] > ds.latitude[-1]:
        ds = ds.sortby("latitude")

    municipios = municipios.copy()
    municipios["lon_360"] = municipios["lon"].apply(lambda x: x + 360 if x < 0 else x)

    dados = {}
    for v in ["t2m", "tp", "u10", "v10"]:
        if v not in ds:
            continue
        valores = ds[v].interp(
            latitude=xr.DataArray(municipios["lat"], dims="points"),
            longitude=xr.DataArray(municipios["lon_360"], dims="points"),
        )
        dados[v] = valores.values

    df = pd.DataFrame(dados)
    df["municipio"] = municipios["municipio"]
    df["uf"] = municipios["uf"]
    return df

# ==============================
# PIPELINE PRINCIPAL
# ==============================

def gerar_boletim_diario():
    municipios = pd.read_csv(MUNICIPIOS_CSV, sep='|')
    os.makedirs("data_processed", exist_ok=True)

    # 0–192h de 3 em 3, depois 198–834h de 6 em 6
    horas = list(range(0, 192+3, 3)) + list(range(198, 834+6, 6))

    registros = []
    for fhr in tqdm(horas, desc="Baixando e processando GRIBs GEFS"):
        ds = abrir_gefs_grib(fhr)
        if ds is None:
            continue

        data_prev = datetime.strptime(RUN_DATE + HOUR, "%Y%m%d%H") + timedelta(hours=fhr)
        df = extrair_para_municipios(ds, municipios)
        df["data_previsao"] = data_prev
        registros.append(df)
        ds.close()

    if not registros:
        print("❌ Nenhuma previsão válida foi processada.")
        return

    previsoes = pd.concat(registros)
    previsoes["data_previsao"] = pd.to_datetime(previsoes["data_previsao"])

    # Corrige precipitação acumulada -> incremental
    previsoes = previsoes.sort_values(["municipio", "data_previsao"])
    previsoes["tp_dif"] = previsoes.groupby("municipio")["tp"].diff().fillna(previsoes["tp"])
    previsoes["tp_dif"] = previsoes["tp_dif"].clip(lower=0)

    # Reamostra para diário (soma ou média)
    previsoes["data_dia"] = previsoes["data_previsao"].dt.floor("D")
    df_diario = previsoes.groupby(["municipio", "uf", "data_dia"]).agg({
        "t2m": "mean",
        "tp_dif": "sum",
        "u10": "mean",
        "v10": "mean"
    }).reset_index().rename(columns={"tp_dif": "tp"})

    # Conversões de unidade
    df_diario["t2m"] = df_diario["t2m"] - 273.15  # K → °C
    #df_diario["tp"] = df_diario["tp"] / 1000      # m → mm

    # Salva resultado
    df_diario.to_csv("data_processed/previsao_diaria.csv", index=False)
    print("✅ Arquivo salvo em data_processed/previsao_diaria.csv")

# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    gerar_boletim_diario()

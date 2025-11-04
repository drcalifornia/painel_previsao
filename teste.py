import xarray as xr, s3fs, tempfile, shutil

fs = s3fs.S3FileSystem(anon=True)
path_s3 = "s3://noaa-gefs-pds/gefs.20251104/00/atmos/pgrb2ap5/geavg.t00z.pgrb2a.0p50.f000"

# baixa localmente (necessário pro cfgrib)
with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as tmp:
    local_path = tmp.name
    with fs.open(path_s3, "rb") as fsrc:
        shutil.copyfileobj(fsrc, tmp)

def abrir_subset(filtros):
    return xr.open_dataset(local_path, engine="cfgrib", backend_kwargs={"filter_by_keys": filtros})

# Temperatura 2 m
ds_t2m = abrir_subset({"shortName": "2t", "typeOfLevel": "heightAboveGround", "level": 2})
# Precipitação total
ds_tp = abrir_subset({"shortName": "tp", "typeOfLevel": "surface"})
# Ventos 10 m
ds_u10 = abrir_subset({"shortName": "10u", "typeOfLevel": "heightAboveGround", "level": 10})
ds_v10 = abrir_subset({"shortName": "10v", "typeOfLevel": "heightAboveGround", "level": 10})

# Une tudo num único dataset
ds = xr.merge([ds_t2m, ds_tp, ds_u10, ds_v10], compat="override")
print(ds)

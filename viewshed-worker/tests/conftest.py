
import sys, types
osgeo = types.ModuleType("osgeo")
gdal_mod = types.ModuleType("osgeo.gdal")
gdal_mod.UseExceptions = lambda: None
gdal_mod.Open = lambda *a, **k: None
gdal_mod.InvGeoTransform = lambda *a, **k: None
gdal_mod.ApplyGeoTransform = lambda *a, **k: (0.0, 0.0)
osgeo.gdal = gdal_mod
sys.modules.setdefault("osgeo", osgeo)
sys.modules.setdefault("osgeo.gdal", gdal_mod)
import psycopg2 as _real
try:
    _real.sql
except AttributeError:
    _real.sql = types.SimpleNamespace()

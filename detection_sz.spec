hidden_imports = [
    'dotenv',
    'rioxarray',
    'rasterio',
    'rasterio.libs',
    'pyproj',
    'numpy'
]

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('config', 'config'),
        ('api', 'api'),
        ('processing', 'processing'),
        ('db', 'db'),
        ('dotenv', 'dotenv'),
        ('D:/projects/projects_2024/venvs/detection_sz/Lib/site-packages/rasterio', 'rasterio'),
        ('D:/projects/projects_2024/venvs/detection_sz/Lib/site-packages/rasterio.libs', 'rasterio.libs'),
        ('D:/projects/projects_2024/venvs/detection_sz/Lib/site-packages/rioxarray', 'rioxarray'),
    ],
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['config'],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='detection_sz',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['ico/radar.ico'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='detection_blooms',
)

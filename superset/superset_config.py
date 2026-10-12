# Минимальный конфиг для локального стенда (метаданные Superset — во встроенном SQLite)
SECRET_KEY = "lakehouse-local-demo-secret-key-change-me"
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
WTF_CSRF_ENABLED = False   # чтобы скрипт мог создавать объекты через REST API

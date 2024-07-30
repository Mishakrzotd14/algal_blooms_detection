import os

log_file_path = os.path.join(os.getcwd(), "logs")

if not os.path.exists(log_file_path):
    os.makedirs(log_file_path)

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "root": {
        "level": "INFO",
        "handlers": ["console", "log_file"],
    },
    "formatters": {
        "verbose": {
            "format": "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d %(module)s] %(message)s",
        }
    },
    "handlers": {
        "console": {"level": "DEBUG", "class": "logging.StreamHandler", "formatter": "verbose"},
        "log_file": {
            "level": "INFO",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(log_file_path, "detection_sz.log"),
            "formatter": "verbose",
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 5,
            "encoding": "utf-8",
        },
    },
    "loggers": {
        "": {
            "level": "INFO",
            "propagate": True,
        },
    },
}

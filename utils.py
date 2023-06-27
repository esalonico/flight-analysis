import os
import logging
import logging.config

# logging
LOGS_PATH = os.path.join(os.path.dirname(__file__), "logs")
LOGGING_CONFIG_DICT = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'standard',
            'filename': os.path.join(LOGS_PATH, "logs.log"),
            "when": "S",  # TODO: CHANGE
            'backupCount': 8
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO'
    }
}


def create_logs_folder():
    if not os.path.isdir(LOGS_PATH):
        os.mkdir(LOGS_PATH)
        
def setup_logger(logger_name):
    create_logs_folder()
    logging.getLogger('WDM').setLevel(logging.NOTSET) # suppress WDM (Webdrive Manager) logs
    logging.config.dictConfig(LOGGING_CONFIG_DICT)
    return logging.getLogger(logger_name)
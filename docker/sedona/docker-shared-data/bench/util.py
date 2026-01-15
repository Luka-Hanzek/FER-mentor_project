import os
import logging

from pyspark.logger import PySparkLogger


def get_filename_without_extension(file_path):
    filename = os.path.basename(file_path)
    return filename.replace(get_filename_extension(file_path), "")

def get_filename_extension(file_path):
    filename = os.path.basename(file_path)
    
    parts = os.path.splitext(filename)
    ext = ""
    while parts[1]:
        ext = f"{parts[1]}{ext}"
        parts = os.path.splitext(parts[0])

    return ext


class LoggingMixin:
    """
    A mixin class that provides logging functionality.

    The logger's log level is set based on the value of the `custom.logging.level` Spark configuration property.
    If the property is not set, the default log level is used.
    
    This class requires the `_spark_session` attribute to be set on the class instance.
    The `_logger` attribute is assigned to `self.logger` and can be used to log messages.
    
    This class is intended to be used as a mixin class in other classes that require logging functionality.
    To use this class, simply extend it in your class definition.
    
    Example:
    class MyClass(LoggingMixin):
        def __init__(self, *args, **kwargs):
            self._spark_session = ...
    
            super().__init__(*args, **kwargs)
    
    """

    SPARK_CONF_LOG_LEVEL_KEY = "custom.logging.level"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._logger = PySparkLogger.getLogger(self.__class__.__name__)
        if (
            log_level := self._spark_session.conf.get(
                LoggingMixin.SPARK_CONF_LOG_LEVEL_KEY, "INFO"
            )
        ) is not None:
            self._logger.setLevel(logging.getLevelNamesMapping()[log_level])

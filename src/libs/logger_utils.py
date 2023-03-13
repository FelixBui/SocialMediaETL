import logging

class MyLogger:

    def __init__(self, logger_name, log_file_path=None, loggers=None, file_log_level=logging.DEBUG, console_log_level=logging.INFO):
        self.logger_name = logger_name
        self.log_file_path = log_file_path
        self.loggers = loggers or {}
        self.file_log_level = file_log_level
        self.console_log_level = console_log_level

    def get_logger(self):
        """
            Create a logger object with the given name
        """
        if self.logger_name in self.loggers:
            logger = self.loggers[self.logger_name] 
        else:
            logger = logging.getLogger(self.logger_name)
            logger.setLevel(logging.DEBUG)

            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            # Log to file
            file_handler = logging.FileHandler(self.log_file_path)
            file_handler.setLevel(self.file_log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            # Log to console
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(self.console_log_level)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

            self.loggers[self.logger_name] = logger

        return logger


    def log(self, level, message):
        """
            Log a message with the given log level using the specified logger
        """
        logger = self.get_logger()
        level = level.lower()
        if level == 'debug':
            logger.debug(message)
        elif level == 'info':
            logger.info(message)
        elif level == 'warning':
            logger.warning(message)
        elif level == 'error':
            logger.error(message)
        elif level == 'critical':
            logger.critical(message)
        else:
            raise ValueError("Invalid log level: {}".format(level))


# my_logger = MyLogger(logger_name='my_logger', log_file_path="define/your/path", file_log_level=logging.DEBUG, console_log_level=logging.INFO)
# my_logger.log('debug', 'This is a debug message')
# my_logger.log('warning', 'This is a warning message')

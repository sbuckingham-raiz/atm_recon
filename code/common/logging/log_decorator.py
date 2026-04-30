import functools
import logging
import time
from collections.abc import Callable

def log_decorator_args(function_type:str = ['pipeline', 'automation'], my_logger:logging.Logger = None, timer:bool=False):
    def log_decorator(func:Callable):

        if not my_logger:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)

            stream_handler = logging.StreamHandler()
            file_handler = logging.FileHandler(function_type + '.log')

            stream_handler.setLevel(logging.DEBUG)
            file_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            stream_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)

            logger.addHandler(stream_handler)
            logger.addHandler(file_handler)
        else:
            logger = my_logger
            
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            try:
                start_time = time.time()
                args_repr = [f'{a = }' for a in args]
                kwargs_repr = [f'{k}={v!r}' for k,v in kwargs.items()]
                signature = "\n".join(args_repr + kwargs_repr)
                
                if signature:
                    logger.debug(f'Function {func.__name__} called with args: \n{signature}')
                else:
                    logger.debug(f'Function {func.__name__} was called with no arguments...')
                
                result = func(*args, **kwargs)
                end_time = time.time()
                duration = end_time - start_time

                if timer:
                    logger.info(f'Execution Time for {func.__name__}: {duration: .2f} seconds.')

                return result

            except Exception as e:
                end_time = time.time()
                duration = end_time - start_time
                logger.error(f'Error running {func.__name__}. Error Message: {str(e)}')
                raise e

        return wrapper
    return log_decorator

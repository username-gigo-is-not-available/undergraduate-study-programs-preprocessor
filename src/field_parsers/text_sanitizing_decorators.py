from functools import wraps


def replace_nulls(func: callable) -> callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> str:
        result = func(*args, **kwargs)
        return result if result != 'nan' else 'нема'

    return wrapper


def clean_whitespace(func: callable) -> callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> str:
        result = str(func(*args, **kwargs))
        cleaned_result: str = ' '.join(result.split())
        return cleaned_result

    return wrapper


def clean_newlines(func: callable) -> callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> str:
        result: str = str(func(*args, **kwargs))
        cleaned_result: str = '|'.join(map(str.strip, filter(lambda x: x, result.split('\n'))))
        return cleaned_result

    return wrapper


def sentence_case(func: callable) -> callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> str:
        result: str = str(func(*args, **kwargs))
        return ''.join([result[0].capitalize(), result[1:]])

    return wrapper


def process_multivalued_field(func: callable) -> callable:
    @replace_nulls
    @clean_whitespace
    @clean_newlines
    @wraps(func)
    def wrapper(*args, **kwargs) -> str:
        return func(*args, **kwargs)

    return wrapper

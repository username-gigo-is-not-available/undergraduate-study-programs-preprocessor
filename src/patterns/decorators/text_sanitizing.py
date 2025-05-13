from functools import wraps


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
        return result.capitalize()

    return wrapper


def process_multivalued_field(func: callable) -> callable:
    @clean_whitespace
    @clean_newlines
    @wraps(func)
    def wrapper(*args, **kwargs) -> str:
        return func(*args, **kwargs)

    return wrapper

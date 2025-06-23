from dagshub.auth import get_token


def get_token_exists(host):
    token = get_token(host=host)
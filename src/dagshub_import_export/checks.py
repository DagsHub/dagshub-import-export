from dagshub.auth import get_token


def get_token_exists(host):
    token = get_token(host=host)

# TODO: check that the destination repo has all storage integrations of the original repo
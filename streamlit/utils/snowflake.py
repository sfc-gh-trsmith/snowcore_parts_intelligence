from snowflake.snowpark.context import get_active_session


def get_session():
    return get_active_session()

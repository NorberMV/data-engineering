import sys
import os
import pprint

sys.path.insert(
    0,
    os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),  # ./dags
            os.pardir,                  # ./
            "dag_callables",            # ./dag_callables
        )
    ),
)

# Entry point
if __name__ == "__main__":
    pprint.pprint(sys.path)
    try:
        from callables import insert_to_redshift # worked!
        from utils import (
            CREATE_DB_SQL_PATH,
            FULL_SCHEMA,
        )
        # from dag_callables import (
        #     CREATE_DB_SQL_PATH,
        #     FULL_SCHEMA,
        #     insert_to_redshift
        # )
    except ImportError as e:
        print(
            f"Error importing callables! Error:\n\n{str(sys.exc_info())}"
        )
    else:
        print(
            f"CREATE_DB_SQL_PATH: {CREATE_DB_SQL_PATH}\nFULL_SCHEMA: {FULL_SCHEMA}"
        )

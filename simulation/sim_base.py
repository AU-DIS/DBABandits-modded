import logging
from importlib import reload
import bandits.bandit_helper_v2 as bandit_helper
import shared.helper as helper
import database.sql_connection as sql_connection
from typing import Union
import constants


class BaseSimulator:
    def __init__(self) -> None:
        # configuring the logger
        logging.basicConfig(
            filename=helper.get_experiment_folder_path(configs.experiment_id) + configs.experiment_id + ".log",  # type: ignore
            filemode="w",
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        logging.getLogger().setLevel(logging.INFO)

        # Get the query List
        self.queries: list[dict[str, Union[int, str, dict[str,list[str]]]]] = helper.get_queries_v2()
        self.connection = sql_connection.get_sql_connection()  # type: ignore
        self.query_obj_store = {}
        reload(bandit_helper)

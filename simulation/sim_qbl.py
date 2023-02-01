import bandits.bandit_qbl as qbl
from importlib import reload
import constants as constants
import shared.configs_v2 as configs
import shared.helper as helper

import database.sql_connection as sql_connection
import database.sql_helper_v2 as sql_helper

from sim_base import BaseSimulator
import logging


class QBLSimulator(BaseSimulator):

    def run(self) -> None: #TODO: correct return type
        reload(configs)
        logging.info("Logging configs...\n")
        helper.log_configs(logging, configs)
        logging.info("Logging constants...\n")
        helper.log_configs(logging, constants)
        logging.info("Starting MAB...\n")

        # Get all the columns from the database
        all_columns: dict[str,list[str]]
        all_columns, number_of_columns = sql_helper.get_all_columns(self.connection)
        #context_size: int = (
        #    number_of_columns * (1 + constants.CONTEXT_UNIQUENESS + constants.CONTEXT_INCLUDES)
        #    + constants.STATIC_CONTEXT_SIZE
        #)

        
         # Running the bandit for T rounds and gather the reward
        arm_selection_count = {}
        chosen_arms_last_round = {}
        next_workload_shift = 0
        queries_start = configs.queries_start_list[next_workload_shift]
        queries_end = configs.queries_end_list[next_workload_shift]
        query_obj_additions = []
        total_time = 0.0

        #Create Arms
        # New set of queries in this batch, required for query execution
        queries_current_batch = self.queries
        t = 0
        # Adding new queries to the query store
        query_obj_list_current = []
        for n in range(len(queries_current_batch)):
            query = queries_current_batch[n]
            query_id = query["id"]
            if query_id in self.query_obj_store:
                query_obj_in_store = self.query_obj_store[query_id]
                query_obj_in_store.frequency += 1
                query_obj_in_store.last_seen = t
                query_obj_in_store.query_string = query["query_string"]
                if query_obj_in_store.first_seen == -1:
                    query_obj_in_store.first_seen = t
            else:
                print("New query ID: " + str(query_id))
                print(type(query_id))
                query = Query(
                    self.connection,
                    query_id,
                    query["query_string"],
                    query["predicates"],
                    query["payload"],
                    t,
                )
                query.context = bandit_helper.get_query_context_v1(
                    query, all_columns, number_of_columns
                )
                self.query_obj_store[query_id] = query
            query_obj_list_current.append(self.query_obj_store[query_id])

        index_arms = {}
        query_obj_list_past = query_obj_list_current
        for i in range(len(query_obj_list_past)):
            bandit_arms_tmp = bandit_helper.gen_arms_from_predicates_v2(
                self.connection, query_obj_list_past[i]
            )
            for key, index_arm in bandit_arms_tmp.items():
                if key not in index_arms:
                    index_arm.query_ids = set()
                    index_arm.query_ids_backup = set()
                    index_arm.clustered_index_time = 0
                    index_arms[key] = index_arm
                index_arm.clustered_index_time += (
                    max(query_obj_list_past[i].table_scan_times[index_arm.table_name])
                    if query_obj_list_past[i].table_scan_times[index_arm.table_name]
                    else 0
                )
                index_arms[key].query_ids.add(index_arm.query_id)
                index_arms[key].query_ids_backup.add(index_arm.query_id)
        print("Number of arms generated from entire workload: " + str(len(index_arms.keys)))
        

        #Create Bandit
        bandit = qbl.QBLBandit()
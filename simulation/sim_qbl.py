import bandits.bandit_qbl as qbl
import bandits.bandit_helper_v2 as bandit_helper

from importlib import reload
import constants as constants
import shared.configs_v2 as configs
import shared.helper as helper

from bandits.query_v5 import Query

import database.sql_connection as sql_connection
import database.sql_helper_v2 as sql_helper

from simulation.sim_base import BaseSimulator
import logging
import datetime
import pprint
import operator

class Simulator(BaseSimulator):

    def run(self,exp_report_list, version, exp_id_list) -> None: #TODO: correct return type
        pp = pprint.PrettyPrinter()
        results = []
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
        queries_start = 0 #configs.queries_start_list[next_workload_shift]
        queries_end = configs.queries_per_round#configs.queries_end_list[next_workload_shift]
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

        # This list contains all past queries, we don't include new queries seen for the first time.
            query_obj_list_past = []
            query_obj_list_new = []
            for key, obj in self.query_obj_store.items():
                if t - obj.last_seen <= constants.QUERY_MEMORY and 0 <= obj.first_seen < t:
                    query_obj_list_past.append(obj)
                elif t - obj.last_seen > constants.QUERY_MEMORY:
                    obj.first_seen = -1
                elif obj.first_seen == t:
                    query_obj_list_new.append(obj)

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
        print("Number of arms generated from entire workload: " + str(len(index_arms.keys())))
        
        index_arm_list = list(index_arms.values())
        #Create Bandit
        bandit = qbl.QBLBandit(index_arm_list)
        #print(len(index_arms_list))
        #print(index_arms_list[0])
        #exit()
        for t in range((configs.rounds)):
            print("Started new round")
            logging.info(f"round: {t}")
            start_time_round = datetime.datetime.now()

            # check if workload shift is required
            #if t == configs.workload_shifts[next_workload_shift]:
            #    queries_start = configs.queries_start_list[next_workload_shift]
            #    queries_end = configs.queries_end_list[next_workload_shift]
            #    if len(configs.workload_shifts) > next_workload_shift + 1:
            #        next_workload_shift += 1
            queries_start = queries_end
            queries_end += configs.queries_per_round 

            # New set of queries in this batch, required for query execution
            queries_current_batch = self.queries[queries_start:queries_end]

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

            # This list contains all past queries, we don't include new queries seen for the first time.
            #query_obj_list_past = []
            query_obj_list_new = []
            for key, obj in self.query_obj_store.items():
                if t - obj.last_seen <= constants.QUERY_MEMORY and 0 <= obj.first_seen < t:
                    query_obj_list_past.append(obj)
                elif t - obj.last_seen > constants.QUERY_MEMORY:
                    obj.first_seen = -1
                elif obj.first_seen == t:
                    query_obj_list_new.append(obj)

            # this rounds new will be the additions for the next round
            query_obj_additions = query_obj_list_new

            index_arms = {}
            print("Length_of_past: " + str(len(query_obj_list_past)))
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


            chosen_arm_ids = bandit.select_arms(8,t)
            logging.info(f"Chosen arm ids: {chosen_arm_ids}")


            ######################THIS IS STUFF#####################
             # get objects for the chosen set of arm ids
            chosen_arms = {}
            used_memory = 0
            if chosen_arm_ids:
                chosen_arms = {}
                for arm in chosen_arm_ids:
                    if not used_memory + index_arm_list[arm].memory <= configs.max_memory:
                        logging.info(f"Skipped arm: {chosen_arm_ids}. Memory needed: {index_arm_list[arm].memory}. Memory available: {configs.max_memory-used_memory}")
                        continue
                    index_name = index_arm_list[arm].index_name
                    chosen_arms[index_name] = index_arm_list[arm]
                    used_memory = used_memory + index_arm_list[arm].memory
                    if index_name in arm_selection_count:
                        arm_selection_count[index_name] += 1
                    else:
                        arm_selection_count[index_name] = 1

            # clean everything at start of actual rounds
            if configs.hyp_rounds != 0 and t == configs.hyp_rounds:
                sql_helper.bulk_drop_index(
                    self.connection, constants.SCHEMA_NAME, chosen_arms_last_round
                )
                chosen_arms_last_round = {}

            # finding the difference between last round and this round
            keys_last_round = set(chosen_arms_last_round.keys())
            keys_this_round = set(chosen_arms.keys())
            key_intersection = keys_last_round & keys_this_round
            key_additions = keys_this_round - key_intersection
            key_deletions = keys_last_round - key_intersection
            logging.info(f"Selected: {keys_this_round}")
            logging.debug(f"Added: {key_additions}")
            logging.debug(f"Removed: {key_deletions}")

            added_arms = {}
            deleted_arms = {}
            for key in key_additions:
                added_arms[key] = chosen_arms[key]
            for key in key_deletions:
                deleted_arms[key] = chosen_arms_last_round[key]

            start_time_create_query = datetime.datetime.now()
            if t < configs.hyp_rounds:
                (
                    time_taken,
                    creation_cost_dict,
                    arm_rewards,
                ) = sql_helper.hyp_create_query_drop_v2(
                    self.connection,
                    constants.SCHEMA_NAME,
                    chosen_arms,
                    added_arms,
                    deleted_arms,
                    query_obj_list_current,
                )
            else:
                time_taken, creation_cost_dict, arm_rewards = sql_helper.create_query_drop_v3(
                    self.connection,
                    constants.SCHEMA_NAME,
                    chosen_arms,
                    added_arms,
                    deleted_arms,
                    query_obj_list_current,
                )
            end_time_create_query = datetime.datetime.now()
            creation_cost = sum(creation_cost_dict.values())
            if t == configs.hyp_rounds and configs.hyp_rounds != 0:
                # logging arm usage counts
                logging.info(
                    "\n\nIndex Usage Counts:\n"
                    + pp.pformat(
                        sorted(
                            arm_selection_count.items(),
                            key=operator.itemgetter(1),
                            reverse=True,
                        )
                    )
                )
                arm_selection_count = {}


            #####################THIS IS STUFF END##################

            bandit.update(chosen_arm_ids, arm_rewards)


            #####################THIS IS MORE STUFF#################
            """super_arm_id = frozenset(chosen_arm_ids)
            if t >= configs.hyp_rounds:
                if super_arm_id in super_arm_scores:
                    super_arm_scores[super_arm_id] = (
                        super_arm_scores[super_arm_id] * super_arm_counts[super_arm_id]
                        + time_taken
                    )
                    super_arm_counts[super_arm_id] += 1
                    super_arm_scores[super_arm_id] /= super_arm_counts[super_arm_id]
                else:
                    super_arm_counts[super_arm_id] = 1
                    super_arm_scores[super_arm_id] = time_taken
            """
            # keeping track of queries that we saw last time
            chosen_arms_last_round = chosen_arms

            if t == (configs.rounds + configs.hyp_rounds - 1):
                sql_helper.bulk_drop_index(self.connection, constants.SCHEMA_NAME, chosen_arms)

            end_time_round = datetime.datetime.now()
            current_config_size = float(sql_helper.get_current_pds_size(self.connection))
            logging.info("Size taken by the config: " + str(current_config_size) + "MB")
            # Adding information to the results array
            if t >= configs.hyp_rounds:
                actual_round_number = t - configs.hyp_rounds
                recommendation_time = (end_time_round - start_time_round).total_seconds() - (
                    end_time_create_query - start_time_create_query
                ).total_seconds()
                total_round_time = creation_cost + time_taken + recommendation_time
                results.append(
                    [actual_round_number, constants.MEASURE_BATCH_TIME, total_round_time]
                )
                results.append(
                    [actual_round_number, constants.MEASURE_INDEX_CREATION_COST, creation_cost]
                )
                results.append(
                    [actual_round_number, constants.MEASURE_QUERY_EXECUTION_COST, time_taken]
                )
                results.append(
                    [
                        actual_round_number,
                        constants.MEASURE_INDEX_RECOMMENDATION_COST,
                        recommendation_time,
                    ]
                )
                results.append(
                    [actual_round_number, constants.MEASURE_MEMORY_COST, current_config_size]
                )
            else:
                total_round_time = (end_time_round - start_time_round).total_seconds() - (
                    end_time_create_query - start_time_create_query
                ).total_seconds()
                results.append([t, constants.MEASURE_HYP_BATCH_TIME, total_round_time])
            total_time += total_round_time

            #if t >= configs.hyp_rounds:
            #    best_super_arm = min(super_arm_scores, key=super_arm_scores.get)

            print(f"current total {t}: ", total_time)

        logging.info(
            "Time taken by bandit for " + str(configs.rounds) + " rounds: " + str(total_time)
        )
        logging.info(
            "\n\nIndex Usage Counts:\n"
            + pp.pformat(
                sorted(arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)
            )
        )
        sql_helper.restart_sql_server()
        return results, total_time
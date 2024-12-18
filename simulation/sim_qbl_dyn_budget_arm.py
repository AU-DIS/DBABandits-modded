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
        #queries_current_batch = self.queries
        t = 0
        # Adding new queries to the query store
        # query_obj_list_current = []
        # for n in range(len(queries_current_batch)):
        #     query = queries_current_batch[n]
        #     query_id = query["id"]
        #     if query_id in self.query_obj_store:
        #         query_obj_in_store = self.query_obj_store[query_id]
        #         query_obj_in_store.frequency += 1
        #         query_obj_in_store.last_seen = t
        #         query_obj_in_store.query_string = query["query_string"]
        #         if query_obj_in_store.first_seen == -1:
        #             query_obj_in_store.first_seen = t
        #     else:
        #         print("New query ID: " + str(query_id))
        #         print(type(query_id))
        #         query = Query(
        #             self.connection,
        #             query_id,
        #             query["query_string"],
        #             query["predicates"],
        #             query["payload"],
        #             t,
        #         )
        #         query.context = bandit_helper.get_query_context_v1(
        #             query, all_columns, number_of_columns
        #         )
        #         self.query_obj_store[query_id] = query
        #     query_obj_list_current.append(self.query_obj_store[query_id])

        # # This list contains all past queries, we don't include new queries seen for the first time.
        #     query_obj_list_past = []
        #     query_obj_list_new = []
        #     for key, obj in self.query_obj_store.items():
        #         if t - obj.last_seen <= constants.QUERY_MEMORY and 0 <= obj.first_seen < t:
        #             query_obj_list_past.append(obj)
        #         elif t - obj.last_seen > constants.QUERY_MEMORY:
        #             obj.first_seen = -1
        #         elif obj.first_seen == t:
        #             query_obj_list_new.append(obj)

        index_arms = {}
        query_obj_list_past = []# query_obj_list_current
        # for i in range(len(query_obj_list_past)):
        #     bandit_arms_tmp = bandit_helper.gen_arms_from_predicates_v2(
        #         self.connection, query_obj_list_past[i]
        #     )
        #     for key, index_arm in bandit_arms_tmp.items():
        #         if key not in index_arms:
        #             index_arm.query_ids = set()
        #             index_arm.query_ids_backup = set()
        #             index_arm.clustered_index_time = 0
        #             index_arms[key] = index_arm
        #         index_arm.clustered_index_time += (
        #             max(query_obj_list_past[i].table_scan_times[index_arm.table_name])
        #             if query_obj_list_past[i].table_scan_times[index_arm.table_name]
        #             else 0
        #         )
        #         index_arms[key].query_ids.add(index_arm.query_id)
        #         index_arms[key].query_ids_backup.add(index_arm.query_id)
        # print("Number of arms generated from entire workload: " + str(len(index_arms.keys())))

        # Discount allowed memory.
        pds = int(sql_helper.get_current_pds_size(self.connection))
        logging.info(f"PDS SIZE: {pds}")
        configs.max_memory -= pds #int(sql_helper.get_current_pds_size(self.connection))
        logging.info(f"Allowed Memory left for indexes: {configs.max_memory}")

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
            
            if index_arms:
                bandit.expand(list(index_arms.values()))
                #index_arm_list = list(index_arms.values())
            
            #Over select arms. Then later we only keep the top arms within the budget.
            chosen_arm_ids = []
            if t != 0:
                chosen_arm_ids = bandit.select_arms_budget(configs.max_memory)
                logging.info(f"Chosen arm ids: {chosen_arm_ids}")


            ######################THIS IS STUFF#####################
             # get objects for the chosen set of arm ids
            chosen_arms = {}
            used_memory = 0
            
            
            
            #Filter the selection given memory budget and translate id to arm
            #We do not skip costly arms to look for cheaper ones with lower priority.
            #Doing so would either have to reward an unexplored arm 0 or keep it untouched in the queue. 
            #Both which are not ideal for finding a true ranking. 
            
            #Add a buffer to include a final index if it is just a bit over the configure memory budget
            allowed_deviance = -1000 
            #When deviance is negative we lower the allowed budget. When the adjusted budget is broken, we add one final index and stop. As such the final memory use may deviate
            # from the budget. Negative deviance covers for big indexes that could give an unfair advantage by going well beyond the real memory budget.  
            
            if chosen_arm_ids:
                #temp_chosen_arm_ids = []
                chosen_arms = {}
                #stop = False
                for arm in chosen_arm_ids:
                    #if stop:
                    #    break
                    #if index_arm_list[arm].memory > configs.max_memory+allowed_deviance:
                    #    logging.info(f"Ignoring arm as it is too big: {arm}. Memory needed: {index_arm_list[arm].memory}. Memory budget available: {configs.max_memory-used_memory}. With allowed deviance: {configs.max_memory+allowed_deviance-used_memory}")
                    #    continue
                    #if used_memory + index_arm_list[arm].memory > configs.max_memory+allowed_deviance:
                    #    logging.info(f"Skipping after arm: {arm}. Memory needed: {index_arm_list[arm].memory}. Memory budget available: {configs.max_memory-used_memory}. With allowed deviance: {configs.max_memory+allowed_deviance-used_memory}")
                    #    stop = True #We add this arm as the final arm
                    #temp_chosen_arm_ids.append(arm) 
                    print(bandit.arms[arm])
                    index_name = bandit.arms[arm].index_name
                    chosen_arms[index_name] = bandit.arms[arm]
                    used_memory = used_memory + bandit.arms[arm].memory
                    if index_name in arm_selection_count:
                        arm_selection_count[index_name] += 1
                    else:
                        arm_selection_count[index_name] = 1
                #chosen_arm_ids = temp_chosen_arm_ids
            logging.info(f"Memory used for indexes: {used_memory}. Budget deviance: {configs.max_memory-used_memory}")      
            # clean up if using hyperrounds from framework
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
                time_taken, creation_cost_dict, arm_rewards, index_use, index_use_rows = sql_helper.create_query_drop_v3(
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
            if t != 0:
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
                results.append([actual_round_number, constants.MEASURE_INDEX_USAGE, index_use])
                results.append([actual_round_number, constants.MEASURE_INDEX_USAGE_ROWS, index_use_rows])
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
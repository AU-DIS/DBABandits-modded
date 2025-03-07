import datetime
import logging
import operator
import pprint
from importlib import reload
from pathlib import Path
import pickle

import numpy
from pandas import DataFrame

import bandits.bandit_c3ucb_v2 as bandits
import bandits.bandit_random as randombandits
import bandits.bandit_helper_v2 as bandit_helper
import constants as constants
import database.sql_connection as sql_connection
import database.sql_helper_v2 as sql_helper
import shared.configs_v2 as configs
import shared.helper as helper
from bandits.experiment_report import ExpReport
from bandits.oracle_v2 import OracleV7 as Oracle
from bandits.query_v5 import Query


# Simulation built on vQ to collect the super arm performance


class BaseSimulator:
    def __init__(self):
        # configuring the logger
        logging.basicConfig(
            filename=helper.get_experiment_folder_path(configs.experiment_id)
            + configs.experiment_id
            + ".log",
            filemode="w",
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        logging.getLogger().setLevel(logging.INFO)

        # Get the query List
        self.queries = helper.get_queries_v2()
        self.connection = sql_connection.get_sql_connection()
        self.query_obj_store = {}
        reload(bandit_helper)


class Simulator(BaseSimulator):
    def run(self,exp_report_list: list, version, exp_id):
        first_save=True
        pp = pprint.PrettyPrinter()
        reload(configs)
        results = []
        super_arm_scores = {}
        super_arm_counts = {}
        best_super_arm = set()
        logging.info("Logging configs...\n")
        helper.log_configs(logging, configs)
        logging.info("Logging constants...\n")
        helper.log_configs(logging, constants)
        logging.info("Starting MAB...\n")

        # Get all the columns from the database
        all_columns, number_of_columns = sql_helper.get_all_columns(self.connection)
        context_size = (
            number_of_columns * (1 + constants.CONTEXT_UNIQUENESS + constants.CONTEXT_INCLUDES)
            + constants.STATIC_CONTEXT_SIZE
        )

        # Create oracle and the bandit
        pds = int(sql_helper.get_current_pds_size(self.connection))
        logging.info(f"PDS SIZE: {pds}")
        #configs.max_memory -= pds #int(sql_helper.get_current_pds_size(self.connection))
        logging.info(f"Allowed Memory left for indexes: {configs.max_memory}")

        oracle = Oracle(configs.max_memory)
        c3ucb_bandit = bandits.C3UCB(
            context_size, configs.input_alpha, configs.input_lambda, oracle
        )
        # c3ucb_bandit = randombandits.RandomBandit()

        # Running the bandit for T rounds and gather the reward
        arm_selection_count = {}
        chosen_arms_last_round = {}
        next_workload_shift = 0
        queries_start = 0#configs.queries_start_list[next_workload_shift]
        queries_end = configs.queries_per_round#configs.queries_end_list[next_workload_shift]
        query_obj_additions = []
        total_time = 0.0

        # bandit_helper.max_arms_counter(self.connection)

        ###########################
       

        for t in range((configs.rounds + configs.hyp_rounds)):
            print("Started new round")
            logging.info(f"round: {t}")
            start_time_round = datetime.datetime.now()
            # At the start of the round we will read the applicable set for the current round. This is a workaround
            # used to demo the dynamic query flow. We read the queries from the start and move the window each round

            # check if workload shift is required
            #if t - configs.hyp_rounds == configs.workload_shifts[next_workload_shift]:
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
            query_obj_list_past = []
            query_obj_list_new = []
            for key, obj in self.query_obj_store.items():
                if t - obj.last_seen <= constants.QUERY_MEMORY and 0 <= obj.first_seen < t:
                    query_obj_list_past.append(obj)
                elif t - obj.last_seen > constants.QUERY_MEMORY:
                    obj.first_seen = -1
                elif obj.first_seen == t:
                    query_obj_list_new.append(obj)

            # We don't want to reset in the first round, if there is new additions or removals we identify a
            # workload change
            if t > 0 and len(query_obj_additions) > 0:
                workload_change = len(query_obj_additions) / len(query_obj_list_past)
                c3ucb_bandit.workload_change_trigger(workload_change)

            # this rounds new will be the additions for the next round
            query_obj_additions = query_obj_list_new

            # Get the predicates for queries and Generate index arms for each query
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

            # set the index arms at the bandit
            if t == configs.hyp_rounds and configs.hyp_rounds != 0:
                index_arms = {}
            index_arm_list = list(index_arms.values())
            logging.info(f"Generated {len(index_arm_list)} arms")
            print("Generated arms: " + str(len(index_arm_list)))
            c3ucb_bandit.set_arms(index_arm_list)
            if t==1:
                print(index_arm_list[0])
                print(type(index_arm_list[0]))

            # creating the context, here we pass all the columns in the database
            context_vectors_v1 = bandit_helper.get_name_encode_context_vectors_v2(
                index_arms,
                all_columns,
                number_of_columns,
                constants.CONTEXT_UNIQUENESS,
                constants.CONTEXT_INCLUDES,
            )
            context_vectors_v2 = bandit_helper.get_derived_value_context_vectors_v3(
                self.connection,
                index_arms,
                query_obj_list_past,
                chosen_arms_last_round,
                not constants.CONTEXT_INCLUDES,
            )
            context_vectors = []
            for i in range(len(context_vectors_v1)):
                context_vectors.append(
                    numpy.array(
                        list(context_vectors_v2[i]) + list(context_vectors_v1[i]), ndmin=2
                    )
                )
            # getting the super arm from the bandit
            chosen_arm_ids = c3ucb_bandit.select_arm_v2(context_vectors, t)
            print(chosen_arm_ids)
            
            # chosen_arm_ids = c3ucb_bandit.select_arm(index_arm_list, current_round=t)
            print("Chosen arms: " + str(len(chosen_arm_ids)))

            if (
                t >= configs.hyp_rounds
                and t - configs.hyp_rounds > constants.STOP_EXPLORATION_ROUND
            ):
                chosen_arm_ids = list(best_super_arm)

            # get objects for the chosen set of arm ids
            chosen_arms = {}
            used_memory = 0
            if chosen_arm_ids:
                chosen_arms = {}
                for arm in chosen_arm_ids:
                    if arm >= len(index_arm_list):
                        logging.warn(f'Chosen arm id {arm} is not in index_arm_list with length {len(index_arm_list)}. Skipping it.')
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

            c3ucb_bandit.update_v4(chosen_arm_ids, arm_rewards)
            # c3ucb_bandit.update(chosen_arm_ids, arm_rewards)
            super_arm_id = frozenset(chosen_arm_ids)
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

            if t >= configs.hyp_rounds:
                best_super_arm = min(super_arm_scores, key=super_arm_scores.get)

            print(f"current total {t}: ", total_time)


            #Save
            if t%50 == 0:
                exp_report_list_save = exp_report_list.copy()
                exp_report_mab = ExpReport(configs.experiment_id,
                                           constants.COMPONENT_MAB + version + exp_id, configs.reps,
                                           configs.rounds)
                temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                       constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_time])
                temp[constants.DF_COL_REP] = 1
                exp_report_mab.add_data_list(temp)
                if first_save:
                    exp_report_list_save.append(exp_report_mab)
                
                path = (Path(__file__).parent.parent / f"experiments\\savepointc3ucb{t}\\reports.pickle").resolve()
                with path.open("wb") as f:
                    pickle.dump(exp_report_list_save, f)



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


if __name__ == "__main__":
    # Running MAB
    exp_report_mab = ExpReport(
        configs.experiment_id, constants.COMPONENT_MAB, configs.reps, configs.rounds
    )
    for r in range(configs.reps):
        simulator = Simulator()
        sim_results, total_workload_time = simulator.run()
        temp = DataFrame(
            sim_results,
            columns=[
                constants.DF_COL_BATCH,
                constants.DF_COL_MEASURE_NAME,
                constants.DF_COL_MEASURE_VALUE,
            ],
        )
        temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
        temp[constants.DF_COL_REP] = r
        exp_report_mab.add_data_list(temp)

    # plot line graphs
    helper.plot_exp_report(
        configs.experiment_id,
        [exp_report_mab],
        (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST),
    )

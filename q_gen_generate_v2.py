import json
import os
import random
import subprocess
import string


# set parameters
dirname = os.path.dirname(__file__)
dbgen_root = os.path.join(dirname, '../TPC-H-Dataset-Generator-MS-SQL-Server/dbgen')
solution_root = os.path.join(dirname, './generate_solution')


# query_list = list(range(1, 23))
# select subset of queries
query_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22]
output_folder = "resources/workloads/self_gen/"
file_name_prefix = "tpc_h"
file_extension = ".json"
file_open_mood = 'w+'    # write 'w', append 'a+'
workload_type = 'full_dynamic_random'    # 'static', 'random', 'full_dynamic', 'full_dynamic_random'

# static parameters
number_of_batches = 100

# random parameters
number_of_queries = 1000
number_of_samples = 500

# dynamic parameters
number_of_samples_2 = 10000
number_of_shifts = 600
shift_length_queries = 5
shift_length_rounds = 1
number_of_files = 1

payloads = {1: {"LINEITEM": ["L_RETURNFLAG", "L_LINESTATUS", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX"]},
            2: {"SUPPLIER": ["S_ACCTBAL", "S_NAME", "S_ADDRESS", "S_PHONE", "S_COMMENT"], "NATION": ["N_NAME"], "PART":
                ["P_PARTKEY", "P_MFGR"], "PARTSUPP": ["PS_SUPPLYCOST"]},
            3: {"LINEITEM": ["L_ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT"], "ORDERS": ["O_ORDERDATE",
                                                                                        "O_SHIPPRIORITY"]},
            4: {"ORDERS": ["O_ORDERPRIORITY"]},
            5: {"NATION": ["N_NAME"], "LINEITEM": ["L_EXTENDEDPRICE", "L_DISCOUNT"]},
            6: {"LINEITEM": ["L_EXTENDEDPRICE", "L_DISCOUNT"]},
            7: {"LINEITEM": ["L_SHIPDATE", "L_EXTENDEDPRICE", "L_DISCOUNT"], "NATION": ["N_NAME"]},
            8: {"ORDERS": ["O_ORDERDATE"], "LINEITEM": ["L_EXTENDEDPRICE", "L_DISCOUNT"], "NATION": ["N_NAME"]},
            9: {"ORDERS": ["O_ORDERDATE"], "LINEITEM": ["L_EXTENDEDPRICE", "L_DISCOUNT", "L_QUANTITY"], "PARTSUPP":
                ["PS_SUPPLYCOST"], "NATION": ["N_NAME"]},
            10: {"CUSTOMER": ["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_ADDRESS", "C_PHONE", "C_COMMENT"], "LINEITEM":
                ["L_EXTENDEDPRICE", "L_DISCOUNT"], "NATION": ["N_NAME"]},
            11: {"PARTSUPP": ["PS_PARTKEY", "PS_SUPPLYCOST", "PS_AVAILQTY"]},
            12: {"LINEITEM": ["L_SHIPMODE"], "ORDERS": ["O_ORDERPRIORITY"]},
            13: {"CUSTOMER": ["C_CUSTKEY"], "ORDERS": ["O_ORDERKEY"]},
            14: {"PART": ["P_TYPE"], "LINEITEM": ["L_EXTENDEDPRICE", "L_DISCOUNT"]},
            16: {"PART": ["P_BRAND", "P_TYPE", "P_SIZE"]},
            17: {"LINEITEM": ["L_EXTENDEDPRICE", "L_QUANTITY"]},
            18: {"CUSTOMER": ["C_NAME"], "ORDERS": ["O_ORDERDATE", "O_TOTALPRICE"], "LINEITEM": ["L_QUANTITY"]},
            19: {"LINEITEM": ["L_EXTENDEDPRICE", "L_DISCOUNT"]},
            20: {"SUPPLIER": ["S_NAME", "S_ADDRESS"], "LINEITEM": ["L_QUANTITY"]},
            21: {"SUPPLIER": ["S_NAME"]},
            22: {"CUSTOMER": ["C_PHONE", "C_ACCTBAL"]}}

predicates = {1: {"LINEITEM": {"L_SHIPDATE": "r"}},
              2: {"PART": {"P_SIZE": "e", "P_TYPE": "c"}, "PARTSUPP": {"PS_SUPPKEY": "j", "PS_PARTKEY": "j"},
                  "SUPPLIER": {"S_NATIONKEY": "j"}, "NATION": {"N_REGIONKEY": "j"}, "REGION": {"R_NAME": "e"}},
              3: {"ORDERS": {"O_CUSTKEY": "j", "O_ORDERDATE": "r"}, "CUSTOMER": {"C_MKTSEGMENT": "e"}, "LINEITEM":
                  {"L_SHIPDATE": "r", "L_ORDERKEY": "j"}},
              4: {"ORDERS": {"O_ORDERKEY": "jpk", "O_ORDERDATE": "r"}, "LINEITEM":
                  {"L_ORDERKEY": "jpk", "L_COMMITDATE": "c", "L_RECEIPTDATE": "c"}},
              5: {"ORDERS": {"O_CUSTKEY": "j", "O_ORDERKEY": "jpk", "O_ORDERDATE": "r"},
                  "LINEITEM": {"L_ORDERKEY": "jpk", "L_SUPPKEY": "j"},
                  "CUSTOMER": {"C_CUSTKEY": "jpk", "C_NATIONKEY": "j"},
                  "SUPPLIER": {"S_NATIONKEY": "j", "S_SUPPKEY": "jpk"},
                  "NATION": {"N_NATIONKEY": "jpk", "N_REGIONKEY": "j"},
                  "REGION": {"R_REGIONKEY": "jpk", "R_NAME": "e"}},
              6: {"LINEITEM": {"L_SHIPDATE": "r", "L_DISCOUNT": "r", "L_QUANTITY": "r"}},
              7: {"ORDERS": {"O_CUSTKEY": "j", "O_ORDERKEY": "jpk"},
                  "LINEITEM": {"L_ORDERKEY": "jpk", "L_SUPPKEY": "j", "L_SHIPDATE": "r"},
                  "CUSTOMER": {"C_CUSTKEY": "jpk", "C_NATIONKEY": "j"},
                  "SUPPLIER": {"S_NATIONKEY": "j", "S_SUPPKEY": "jpk"},
                  "NATION": {"N_NATIONKEY": "jpk", "N_NAME": "e"}},
              8: {"ORDERS": {"O_CUSTKEY": "j", "O_ORDERKEY": "jpk", "O_ORDERDATE": "r"},
                  "LINEITEM": {"L_ORDERKEY": "jpk", "L_SUPPKEY": "j", "L_PARTKEY": "j"},
                  "CUSTOMER": {"C_CUSTKEY": "jpk", "C_NATIONKEY": "j"},
                  "SUPPLIER": {"S_NATIONKEY": "j", "S_SUPPKEY": "jpk"},
                  "NATION": {"N_NATIONKEY": "jpk", "N_REGIONKEY": "j"},
                  "REGION": {"R_REGIONKEY": "jpk", "R_NAME": "e"}, "PART": {"P_PARTKEY": "jpk", "P_TYPE": "e"}},
              9: {"PART": {"P_PARTKEY": "jpk", "P_NAME": "c"}, "SUPPLIER": {"S_NATIONKEY": "j", "S_SUPPKEY": "jpk"},
                  "LINEITEM": {"L_ORDERKEY": "jpk", "L_SUPPKEY": "j", "L_PARTKEY": "j"},
                  "PARTSUPP": {"PS_SUPPKEY": "jpk2", "PS_PARTKEY": "jpk"}, "ORDERS": {"O_ORDERKEY": "jpk"},
                  "NATION": {"N_NATIONKEY": "jpk"}},
              10: {"ORDERS": {"O_CUSTKEY": "j", "O_ORDERKEY": "jpk", "O_ORDERDATE": "r"},
                   "LINEITEM": {"L_ORDERKEY": "jpk", "L_RETURNFLAG": "e"},
                   "CUSTOMER": {"C_CUSTKEY": "jpk", "C_NATIONKEY": "j"}, "NATION": {"N_NATIONKEY": "jpk"}},
              11: {"PARTSUPP": {"PS_SUPPKEY": "jpk2"}, "SUPPLIER": {"S_SUPPKEY": "jpk", "S_NATIONKEY": "j"},
                   "NATION": {"N_NATIONKEY": "jpk", "N_NAME": "e"}},
              12: {"ORDERS": {"O_ORDERKEY": "jpk"},
                   "LINEITEM": {"L_ORDERKEY": "jpk", "L_SHIPMODE": "e", "L_RECEIPTDATE": "r",
                                "L_SHIPDATE": "r", "L_COMMITDATE": "r"}},
              13: {"CUSTOMER": {"C_CUSTKEY": "jpk"}, "ORDERS": {"O_COMMENT": "c", "O_CUSTKEY": "j"}},
              14: {"LINEITEM": {"L_SHIPDATE": "r", "L_PARTKEY": "j"}, "PART": {"P_PARTKEY": "jpk"}},
              16: {"PARTSUPP": {"PS_PARTKEY":"jpk", "PS_SUPPKEY":"jpk2"},
                   "PART": {"P_PARTKEY": "jpk", "P_SIZE": "e", "P_BRAND": "r", "P_TYPE": "c"},
                   "SUPPLIER": {"S_COMMENT":"c"}},
              17: {"PART": {"P_PARTKEY": "jpk", "P_CONTAINER": "e", "P_BRAND": "e"}, "LINEITEM": {"L_PARTKEY": "j"}},
              18: {"ORDERS": {"O_CUSTKEY": "j", "O_ORDERKEY": "jpk"}, "CUSTOMER": {"C_CUSTKEY": "jpk"},
                   "LINEITEM": {"L_ORDERKEY": "jpk"}},
              19: {"LINEITEM": {"L_SHIPMODE": "e", "L_PARTKEY": "j", "L_QUANTITY": "r", "L_SHIPINSTRUCT": "e"},
                   "PART": {"P_SIZE": "r", "P_PARTKEY": "jpk", "P_BRAND": "e", "P_CONTAINER": "e"}},
              20: {"SUPPLIER": {"S_SUPPKEY": "jpk", "S_NATIONKEY": "j"},
                   "PARTSUPP": {"PS_PARTKEY": "jpk", "PS_AVAILQTY": "r", "PS_SUPPKEY": "jpk2"},
                   "LINEITEM": {"L_SUPPKEY": "j", "L_PARTKEY": "j", "L_SHIPDATE": "r"},
                   "NATION": {"N_NATIONKEY": "jpk", "N_NAME": "e"}, "PART": {"P_NAME": "r"}},
              21: {"SUPPLIER": {"S_SUPPKEY": "jpk", "S_NATIONKEY": "j"},
                   "ORDERS": {"O_ORDERKEY": "jpk", "O_ORDERSTATUS": "e"},
                   "LINEITEM": {"L_SUPPKEY": "j", "L_ORDERKEY": "jpk", "L_RECEIPTDATE": "c", "L_COMMITDATE": "c"},
                   "NATION": {"N_NATIONKEY": "jpk", "N_NAME": "e"}},
              22: {"CUSTOMER": {"C_ACCTBAL": "r", "C_PHONE": "c", "C_CUSTKEY": "jpk"}, "ORDERS": {"O_CUSTKEY": "j"}}}

order_bys = {1: {"LINEITEM": [("L_RETURNFLAG", "ASC"), ("L_LINESTATUS", "ASC")]},
             2: {"SUPPLIER": [("S_ACCTBAL", "DESC"), ("S_NAME", "ASC")], "NATION": [("N_NAME", "ASC")], "PART": [("P_PARTKEY", "ASC")]},
             3: {"ORDERS": [("O_ORDERDATE", "ASC")]},
             4: {"ORDERS": [("O_ORDERPRIORITY", "ASC")]},
             5: {},
             6: {},
             7: {},
             8: {},
             9: {},
             10: {},
             11: {},
             12: {"LINEITEM": [("L_SHIPMODE", "ASC")]},
             13: {},
             14: {},
             16: {},
             17: {"LINEITEM": [("L_EXTENDEDPRICE", "ASC")]},
             18: {"ORDERS": [("O_TOTALPRICE", "DESC"), ("O_ORDERDATE", "ASC")]},
             19: {},
             20: {"SUPPLIER": [("S_NAME", "ASC")]},
             21: {"SUPPLIER": [("S_NAME", "ASC")]},
             22: {}}


class BasicQuery:
    def __init__(self, query_id, query_string, predicates, payloads, order_by):
        self.id = query_id
        self.query_string = query_string
        self.predicates = predicates
        self.payload = payloads
        self.group_by = {}
        self.order_by = order_by


os.chdir(solution_root)
# Generate queries single file
if workload_type == 'static':
    file_name_temp = file_name_prefix + "_" + workload_type + "_" + str(number_of_batches) + file_extension
    with open(output_folder + file_name_temp, file_open_mood) as o_file:
        os.chdir(dbgen_root)
        for i in range(number_of_batches):
            seed = random.randint(1, 1000000)
            command2 = f"./qgen -r {seed}"
            output = subprocess.check_output(command2, shell=True)
            queries = output.decode("utf-8").split('\r\n\r\n\r\n')
            for query_id in query_list:
                # queries index start with 1 as there is a comment in the 0th index
                query = BasicQuery(query_id, queries[query_id], predicates[query_id], payloads[query_id], order_bys[query_id])
                json.dump(query.__dict__, o_file)
                o_file.write('\n')

elif workload_type == 'random':
    queries = {}
    file_name_temp = file_name_prefix + "_" + workload_type + "_" + str(number_of_queries) + file_extension
    with open(output_folder + file_name_temp, file_open_mood) as o_file:
        os.chdir(dbgen_root)
        for i in range(number_of_samples):
            seed = random.randint(1, 1000000)
            command2 = f"Debug\qgen.exe -r {seed}"
            output = subprocess.check_output(command2, shell=True)
            queries[i] = output.decode("utf-8").split('\r\n\r\n\r\n')
        for i in range(number_of_queries):
            random_query_id = random.choice(query_list)
            random_sample_id = random.randint(0, number_of_samples - 1)
            query = BasicQuery(random_query_id, queries[random_sample_id][random_query_id], predicates[random_query_id], payloads[random_query_id], order_bys[random_query_id])
            json.dump(query.__dict__, o_file)
            o_file.write('\n')

if workload_type == 'full_dynamic':
    queries = {}
    queries_per_shift = int(len(query_list)/number_of_shifts)
    os.chdir(dbgen_root)
    for i in range(number_of_samples_2):
        seed = random.randint(1, 1000000)
        command2 = f"./qgen -r {seed}"
        output = subprocess.check_output(command2, shell=True)
        #print(output.decode("utf-8").split('\r\nset showplan_all on;\r\ngo\r\n\r\n')[1])
        #exit()
        queries[i] = [s.replace('\r\ngo', '') for s in  output.decode("utf-8").split('\r\nset showplan_all on;\r\ngo\r\n\r\n')]#
    os.chdir(solution_root)
    for k in range(number_of_files):
        new_query_set = set(query_list)
        last_round_query_set = set()
        file_name_temp = file_name_prefix + "_" + workload_type + "_" + str(number_of_shifts) + "_" + str(shift_length_rounds) \
            + "_" + str(k) + file_extension
        with open(dirname+"/" + output_folder + file_name_temp, file_open_mood) as o_file:
            for i in range(number_of_shifts):
                random_query_ids = random.sample(tuple(new_query_set), k=queries_per_shift)
                last_round_query_set = set(random_query_ids)
                print(f"shift {i}: {random_query_ids}")
                new_query_set = new_query_set - last_round_query_set
                for j in range(shift_length_rounds):
                    for random_query_id in random_query_ids:
                        random_sample_id = random.randint(0, number_of_samples_2 - 1)
                        #print(queries[random_sample_id])
                        query = BasicQuery(random_query_id, queries[random_sample_id][random_query_id],
                                           predicates[random_query_id], payloads[random_query_id], order_bys[random_query_id])
                        json.dump(query.__dict__, o_file)
                        o_file.write('\n')


if workload_type == 'full_dynamic_random':
    queries = {}
    queries_per_shift = 5 # int(len(query_list)/number_of_shifts)
    os.chdir(dbgen_root)
    for i in range(number_of_samples_2):
        seed = random.randint(1, 1000000)
        command2 = f"./qgen -r {seed}"
        output = subprocess.check_output(command2, shell=True)
        queries[i] = [s.replace('\r\ngo', '') for s in  output.decode("utf-8").split('\r\nset showplan_all on;\r\ngo\r\n\r\n')]
    os.chdir(solution_root)
    for k in range(number_of_files):
        new_query_set = set(query_list)
        last_round_query_set = set()
        file_name_temp = file_name_prefix + "_" + workload_type + "_" + str(number_of_shifts) + "_" + str(shift_length_queries) \
            + "_" + str(k) + file_extension
        with open(dirname+"/"+output_folder + file_name_temp, file_open_mood) as o_file:
            for i in range(number_of_shifts):
                random_query_ids = random.sample(tuple(new_query_set), k=queries_per_shift)
                last_round_query_set = set(random_query_ids)
                print(f"shift {i}: {random_query_ids}")
                #new_query_set = new_query_set - last_round_query_set
                for j in range(shift_length_queries):
                    random_query_id = random.choice(random_query_ids)
                    random_sample_id = random.randint(0, number_of_samples_2 - 1)
                    query = BasicQuery(random_query_id, queries[random_sample_id][random_query_id],
                                       predicates[random_query_id], payloads[random_query_id], order_bys[random_query_id])
                    json.dump(query.__dict__, o_file)
                    o_file.write('\n')

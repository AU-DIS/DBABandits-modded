from bandits.bandit import Bandit
from updatable_priority_queue import PriorityQueue
import random
from bandits.bandit_arm import BanditArm
import logging


class QBLBandit(Bandit):
    def __init__(self, arms: list[BanditArm], weighted: bool = False) -> None:
        self.arms: list[BanditArm] = arms
        #random.shuffle(self.arms)
        for i, arm in enumerate(self.arms):
            arm.id = i
        self.arm_names = {}
        self.k: int = len(self.arms)
        
        self.in_active_term: dict[str, bool] = {}
        self.last_term_reward: dict[str, float] = {}
        self.last_term_length: dict[str, int] = {}
        self.total_last_term_reward: float = 0.0
        self.total_last_term_length: int = len(self.arms)
        
        for arm in self.arms:
            self.in_active_term[arm.index_name] = False
            self.last_term_reward[arm.index_name] = float(sum(arm.arm_value.values())) if weighted else 1.0
            self.last_term_length[arm.index_name] = 1
            self.total_last_term_reward += self.last_term_reward[arm.index_name]
        
        self.queue: PriorityQueue = PriorityQueue(
            [(i, arm) for i, arm in enumerate(self.arms)]
        )
        self.priority: dict[str, int] = {arm.index_name: i for i, arm in enumerate(self.arms)}
        
    def expand(self, arms: list[BanditArm], weighted: bool = False) -> None:
        #Filter list to only include new arms
        _arms = []
        for arm in arms:
            if arm.index_name in self.arm_names:
                continue
            _arms.append(arm)
            self.arm_names[arm.index_name] = True
        
            
        arms = _arms
        num_new_arms = len(arms)
        old_k = self.k
        self.k += num_new_arms
        self.arms.extend(arms)
        
        logging.info(f"Expanding armset with: {num_new_arms}")
        
        top: tuple[int, BanditArm] = self.queue.pop()  # type: ignore
        if top is None:
            top = (0,0) #No preexisting arms. Dummy top for the loop below.
        else:
            self.queue.put(top)  # type: ignore
        for i, arm in enumerate(arms):
            arm.id = old_k+i
            self.in_active_term[arm.index_name] = False
            self.last_term_reward[arm.index_name] = float(sum(arm.arm_value.values())) if weighted else 1.0
            self.last_term_length[arm.index_name] = 1
            self.total_last_term_reward += float(sum(arm.arm_value.values())) if weighted else 1.0
            self.total_last_term_length += 1
            self.priority[arm.index_name] = top[0] - arm.id - 1
            self.queue.put((self.priority[arm.index_name], arm))

    def select_arms(self, m: int, current_round: int) -> list[int]:
        selected: list[int] = []
        popped: list[tuple[int, BanditArm]] = []
        for _ in range(m):
            pop: tuple[int, BanditArm] = self.queue.pop()  # type: ignore
            popped.append(pop)
            selected.append(pop[1].id)

        i: tuple[int, BanditArm]
        for i in popped:
            self.queue.put(i)  # type: ignore

        return selected
    
    def select_arms_budget(self, budget: int,) -> list[int]:
        selected: list[int] = []
        popped: list[tuple[int, BanditArm]] = []
        used_budget = 0
        while used_budget < budget*0.95:
            pop: tuple[int, BanditArm] = self.queue.pop()  # type: ignore
            if pop is None: #No arms left to choose
                logging.warning(f"Checked all arms to fill memory budget. Used {used_budget} of {budget}")
                break
            if used_budget + pop[1].memory < budget:
                used_budget += pop[1].memory
                selected.append(pop[1].id)
            elif pop[1].memory > budget:
                logging.warning(f"Arm {pop[1].id} is too big to ever select. {pop[1].memory}. Removed from queue")
                continue
            popped.append(pop)

        i: tuple[int, BanditArm]
        for i in popped:
            self.queue.put(i)  # type: ignore

        return selected

    def update(
        self, arms_played: list[int], arms_reward: dict[str, tuple[float, float]]
    ) -> None:
        
        logging.info(f"Arm rewards keys: {arms_reward.keys()}")
        logging.info(f"Arm rewards values: {arms_reward.values()}")
    
        for arm_id in arms_played:
            
            arm: BanditArm = self.arms[arm_id]
            idx: str = arm.index_name
            logging.info(f'Arm played: {arm_id}, {idx}')
            if not self.in_active_term[idx]:
                self.total_last_term_reward -= self.last_term_reward[idx]-self.last_term_reward[idx]/self.last_term_length[idx]
                self.last_term_reward[idx] /= self.last_term_length[idx]
                self.total_last_term_length -= self.last_term_length[idx]-1
                self.last_term_length[idx] = 1
                self.in_active_term[idx] = True

             
            self.total_last_term_reward += arms_reward[idx][0] if idx in arms_reward else 0
            self.last_term_reward[idx] += arms_reward[idx][0] if idx in arms_reward else 0
            self.total_last_term_length += 1
            self.last_term_length[idx] += 1

            weighted_global_avg: float = self.total_last_term_reward / float(
                self.total_last_term_length
            )
            local_avg: float = self.last_term_reward[idx] / float(self.last_term_length[idx])

            is_rewarding: bool = (weighted_global_avg < local_avg * random.uniform(0.9, 1.1))
            
            logging.info(f'Global avg: {weighted_global_avg}, Local_avg: {local_avg}')
            if not is_rewarding:
                old_prio = self.priority[idx]
                # NOTE: Implement a .top() for the queue to avoid pop put
                top: tuple[int, BanditArm] = self.queue.pop()  # type: ignore
                self.queue.put(top)  # type: ignore
                new_prio: int = self.last_term_length[idx] - 1
                new_prio = min(new_prio, self.k - 1 - (top[0] - self.priority[idx]))
                self.priority[idx] = (
                    top[0] - 1
                    if new_prio - self.k - 1 == 0
                    else top[0] + (new_prio - self.k - 1)
                )
                logging.info(f'Not rewarding. Prio before: {old_prio}, New prio: {self.priority[idx]}')
                #self.queue.put((self.priority[idx],top[1])) #type: ignore
                self.queue.update_elem(arm, (self.priority[idx], arm))  # type: ignore
                self.in_active_term[idx] = False

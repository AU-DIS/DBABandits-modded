from bandits.bandit import Bandit
from updatable_priority_queue import PriorityQueue
import random
from bandits.bandit_arm import BanditArm
import logging


class QBLBandit(Bandit):
    def __init__(self, arms: list[BanditArm]) -> None:
        self.arms: list[BanditArm] = arms
        random.shuffle(self.arms)
        for i, arm in enumerate(self.arms):
            arm.id = i

        self.k: int = len(self.arms)

        self.in_active_term: dict[str, bool] = {arm.index_name: False for arm in self.arms}
        self.last_term_reward: dict[str, float] = {arm.index_name: 1.0 for arm in self.arms}
        self.last_term_length: dict[str, int] = {arm.index_name: 1 for arm in self.arms}
        self.total_last_term_reward: float = float(len(self.arms))
        self.total_last_term_length: int = len(self.arms)
        
        self.queue: PriorityQueue = PriorityQueue(
            [(i, arm) for i, arm in enumerate(self.arms)]
        )
        self.priority: dict[str, int] = {arm.index_name: i for i, arm in enumerate(self.arms)}

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
                self.total_last_term_reward -= self.last_term_reward[idx]
                self.last_term_reward[idx] = 0
                self.total_last_term_length -= self.last_term_length[idx]
                self.last_term_length[idx] = 0
                self.in_active_term[idx] = True

             
            self.total_last_term_reward += arms_reward[idx][0] if idx in arms_reward else 0
            self.last_term_reward[idx] += arms_reward[idx][0] if idx in arms_reward else 0
            self.total_last_term_length += 1
            self.last_term_length[idx] += 1

            weighted_global_avg: float = self.total_last_term_reward / float(
                self.total_last_term_length
            )
            local_avg: float = self.last_term_reward[idx] / float(self.last_term_length[idx])

            is_rewarding: bool = (
                weighted_global_avg
                < local_avg * 0.85 * random.choices([0, 1], weights=[0.001, 0.999])[0]
            )
            
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

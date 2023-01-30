from updatable_priority_queue import PriorityQueue
import random
from bandit_arm import BanditArm


class QBLBandit:
    def __init__(self, arms: list[BanditArm]):
        self.arms: list[BanditArm] = arms
        self.k: int = len(self.arms)

        self.in_active_term: dict[str, bool] = {
            arm.index_name: False for arm in self.arms
        }
        self.last_term_reward: dict[str, float] = {
            arm.index_name: 1.0 for arm in self.arms
        }
        self.last_term_length: dict[str, int] = {arm.index_name: 1 for arm in self.arms}
        self.total_last_term_reward: float = float(len(self.arms))
        self.total_last_term_length: int = len(self.arms)
        self.queue: PriorityQueue = PriorityQueue(
            [(i, arm) for i, arm in enumerate(self.arms)]
        )
        self.priority: dict[str, int] = {
            arm.index_name: i for i, arm in enumerate(self.arms)
        }

    def select_arms(self, m: int) -> list[BanditArm]:
        selected: list[BanditArm] = []
        popped: list[tuple[int, BanditArm]] = []
        for _ in range(m):
            pop: tuple[int, BanditArm] = self.queue.pop()  # type: ignore
            popped.append(pop)
            selected.append(pop[1])

        i: tuple[int, BanditArm]
        for i in popped:
            self.queue.put(i)  # type: ignore

        return selected

    def update(self, arms_played: list[BanditArm], arms_reward: list[float]) -> None:
        for i, arm in enumerate(arms_played):
            idx: str = arm.index_name
            if not self.in_active_term[idx]:
                self.total_last_term_reward -= self.last_term_reward[idx]
                self.last_term_reward[idx] = 0
                self.total_last_term_length -= self.last_term_length[idx]
                self.last_term_length[idx] = 0
                self.in_active_term[idx] = True

            self.total_last_term_reward += arms_reward[i]
            self.last_term_reward[idx] += arms_reward[i]
            self.total_last_term_length += 1
            self.last_term_length[idx] += 1

            weighted_global_avg: float = self.total_last_term_reward / float(
                self.total_last_term_length
            )
            local_avg: float = self.last_term_reward[idx] / float(
                self.last_term_length[idx]
            )

            is_rewarding: bool = (
                weighted_global_avg
                < local_avg * 0.85 * random.choices([0, 1], weights=[0.001, 0.999])[0]
            )

            if not is_rewarding:
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
                self.queue.update_elem(idx, arm)  # type: ignore
                self.in_active_term[idx] = False

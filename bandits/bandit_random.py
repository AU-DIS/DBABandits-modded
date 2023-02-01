# import logging
# from abc import abstractmethod

# import numpy

# import constants
import random

from bandits.bandit import Bandit, BanditArm


class RandomBandit(Bandit):
    def __init__(self):
        super().__init__()

    def select_arm(
        self, index_arm_list: list[BanditArm], current_round: int
    ) -> list[BanditArm]:
        if current_round == 0:
            return []
        out: list[BanditArm] = []
        for _ in range(8):
            out.append(index_arm_list[random.randrange(len(index_arm_list))])
        return out

    def update(
        self, arms_played: list[BanditArm], arms_reward: dict[str, tuple[float, float]]
    ) -> None:
        pass

    def set_arms(self, arms: list[BanditArm]) -> None:
        self.arms = arms

    def hard_reset(self) -> None:
        pass

    def workload_change_trigger(self, workload_change: int) -> None:
        """
        This is used to mimic the c3ucb
        """
        pass

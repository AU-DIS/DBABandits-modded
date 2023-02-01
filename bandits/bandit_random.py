import logging
from abc import abstractmethod

import numpy

import constants
import random

from bandits.bandit import Bandit


class RandomBandit(Bandit):
    def __init__(self):
        super().__init__()

    def select_arm(self, index_arm_list: list, current_round: int) -> list:
        if current_round == 0:
            return []
        out :list = []
        for _ in range(8):
            out.append(random.randrange(len(index_arm_list)))
        return out

    def update(self, played_arms: list, rewards: list) -> None:
        pass

    def set_arms(self, armsset: list) -> None:
        self.arms = armsset

    def hard_reset(self) -> None:
        pass

    
    def workload_change_trigger(self, workload_change: int) -> None:
        """
        This is used to mimic the c3ucb
        """
        pass

class ContextRandomBandit(Bandit):
    def __init__(self):
        super().__init__()

    def select_arm(self, index_arm_list: list, current_round: int) -> list:
        if current_round == 0:
            return []
        return [random.randrange(len(index_arm_list))]

    def update(self, played_arms: list, rewards: list) -> None:
        pass

    def set_arms(self, armsset: list) -> None:
        self.arms = armsset

    def hard_reset(self) -> None:
        pass

    
    def workload_change_trigger(self, workload_change: int) -> None:
        """
        This is used to mimic the c3ucb
        """
        pass


import logging
from abc import abstractmethod

import numpy

import constants

class Bandit:

    def __init__(self):
        self.arms: list = []

    @abstractmethod
    def select_arm(self, current_round: int) -> list:
        pass
    
    @abstractmethod
    def update(self, played_arms: list, rewards: list) -> None:
        pass

    @abstractmethod
    def set_arms(self, arms: list) -> None:
        pass

    @abstractmethod
    def hard_reset(self) -> None:
        pass

    #@abstractmethod
    #def workload_change_trigger(self, workload_change: int) -> None:
    #    pass
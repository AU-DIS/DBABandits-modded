from bandits.bandit import Bandit
from queue import PriorityQueue as pq

class QBLBandit(Bandit):

    def _init_(self, arms):
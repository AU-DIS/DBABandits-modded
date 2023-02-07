from abc import abstractmethod
from .bandit_arm import BanditArm


class Bandit:
    def __init__(self) -> None:
        self.arms: list[BanditArm] = []

    @abstractmethod
    def select_arms(self, m: int, current_round: int) -> list[int]:
        pass

    @abstractmethod
    def update(
        self, arms_played: list[int], arms_reward: dict[str, tuple[float, float]]
    ) -> None:
        pass

    @abstractmethod
    def set_arms(self, arms: list[BanditArm]) -> None:
        pass

    @abstractmethod
    def hard_reset(self) -> None:
        pass

    # @abstractmethod
    # def workload_change_trigger(self, workload_change: int) -> None:
    #    pass

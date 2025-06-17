# app/jobs/base.py
from abc import ABC, abstractmethod

# ABCs -> Abstract Base Classes

class BaseJob(ABC):
    @abstractmethod
    def run(self):
        """Run the job logic."""
        pass

    @abstractmethod
    def schedule(self):
        """Register this job with scheduler."""
        pass

from abc import ABC, abstractmethod
from typing import Dict, Optional

class RecordingInterface(ABC):
    @abstractmethod
    def start_recording(self, username: str, user_id: int, chat_id: int) -> bool:
        pass

    @abstractmethod
    def stop_recording(self, user_id: int, username: str) -> bool:
        pass

    @abstractmethod
    def get_active_recordings(self, user_id: int) -> Dict[str, Dict]:
        pass
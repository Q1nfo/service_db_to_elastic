import abc
import json

from json import JSONDecodeError
from typing import Optional, Any


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Загрузить состояние локально из постоянного хранилища"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из потсоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        if not file_path:
            file_path = 'data_storage'
        try:
            open(file_path, 'r').close()
            self.file_path = file_path
        except FileNotFoundError:
            open(file_path, 'w').close()
            self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """Cохраняет состояние в файл"""
        with open(self.file_path, 'w') as f:
            json.dump(state, f)

    def retrieve_state(self) -> dict:
        """Получает состояние из файла"""
        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)

                return data
        except FileNotFoundError:
            self.save_state({})
        except JSONDecodeError:
            self.save_state({})


class State:
    """ Кллас для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    """

    def __init__(self, storage: JsonFileStorage):
        self.storage = storage
        self.state = self.retrieve_state

    def retrieve_state(self) -> dict:
        data = self.storage.retrieve_state()
        if not data:
            return {}
        return data

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определенного ключа"""
        state = self.retrieve_state()
        state[key] = value
        self.storage.save_state(state=state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определенному ключу"""
        state = self.retrieve_state()
        return state.get(key)


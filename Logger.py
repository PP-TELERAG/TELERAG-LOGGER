import os
import asyncio
from datetime import datetime
from enum import Enum
import aiofiles


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class LogLevel(Enum):
    INFO = 1
    WARN = 2
    ERROR = 3
    FATAL = 4


class Logger(metaclass=Singleton):

    def __init__(self, log_dir: str, debug: bool = False, flush_interval: float = 2.0):
        self.log_dir = log_dir if log_dir.endswith(os.sep) else log_dir + os.sep
        os.makedirs(self.log_dir, exist_ok=True)

        self.debug = debug
        self.flush_interval = flush_interval
        self._log_queue = asyncio.Queue()
        self._flush_task = asyncio.create_task(self._flush_logs_loop())

        # Определяем путь к файлу логов
        self.log_file_path = os.path.join(
            self.log_dir, "latest-debug.txt" if self.debug else "latest.txt"
        )

    def _format_message(self, message: str, level: LogLevel) -> str:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level_str = level.name
        if self.debug:
            level_str += "/DEBUG"
        return f"[{level_str}]: {message} - {now}"

    async def log(self, message: str, level: LogLevel = LogLevel.INFO):

        formatted_message = self._format_message(message, level)


        color = "\033[32m"
        if level == LogLevel.WARN:
            color = "\033[33m"
        elif level in (LogLevel.ERROR, LogLevel.FATAL):
            color = "\033[31m"

        print(f"{color}{formatted_message}\033[0m")
        await self._log_queue.put(formatted_message)

    async def _flush_logs_loop(self):
        while True:
            try:
                await asyncio.sleep(self.flush_interval)
                await self.flush_logs()
            except Exception as e:
                print(f"Ошибка при сбросе логов: {e}")

    async def flush_logs(self):
        messages = []
        while not self._log_queue.empty():
            try:
                msg = self._log_queue.get_nowait()
                messages.append(msg)
            except asyncio.QueueEmpty:
                break

        if messages:
            async with aiofiles.open(self.log_file_path, mode='a') as f:
                await f.write("\n".join(messages) + "\n")

    async def get_last(self, count: int) -> list[str]:
        if not os.path.exists(self.log_file_path):
            return []
        async with aiofiles.open(self.log_file_path, mode='r') as f:
            content = await f.read()
        lines = content.strip().split("\n")
        return lines[-count:]

    async def close(self):
        await self.flush_logs()
        self._flush_task.cancel()
        try:
            await self._flush_task
        except asyncio.CancelledError:
            pass

from httpx import AsyncClient
from fastapi import FastAPI
from Logger import Logger, LogLevel
from TBA import BrokerApi

import asyncio
logger = Logger(log_dir="logs/", debug=True)
app = FastAPI()
broker_api = BrokerApi()
broker_api.initialize()

async def consume_logs_from_broker():
    async with AsyncClient() as client:
        while True:
            try:
                message = await broker_api.consume_message("LOGGING")
                if message:
                    await logger.log(message, LogLevel.INFO)
                await asyncio.sleep(100)
            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(100)


@app.on_event("startup")
async def startup():
    asyncio.create_task(consume_logs_from_broker())

@app.get("/logs/last/{count}")
async def get_last(count: int):
    logs = await logger.get_last(count)
    return {"logs": logs}

@app.get("/logs/flush")
async def flush_logs():
    await logger.flush_logs()
    return {"message": "Logs flushed"}

@app.get("/echo")
async def echo():
    return {"message": "Service is up"}






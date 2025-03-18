from httpx import AsyncClient

class BrokerApi:

    def __init__(self, broker_url: str):
        self.topics : list[str] = []
        self.broker_url = broker_url.strip("/")

    async def initialize(self):
        async with AsyncClient() as client:
            try:
                response = await client.get(f"{self.broker_url}/echo")
                if response.status_code != 200:
                    raise Exception(f"Echo endpoint returned status code {response.status_code}")
            except Exception as e:
                raise Exception(f"Failed to connect to Broker {self.broker_url} got message: {e}")

            try:
                response = await client.get(f"{self.broker_url}/topics")
                if response.status_code != 200:
                    raise Exception(f"Topics endpoint returned status code {response.status_code}")
                data = response.json()
                self.topics = data["message"]
            except Exception as e:
                raise Exception(f"Failed to gather topics from broker {self.broker_url}: {e}")

    async def produce_message(self, topic_name:str, message: dict, partition: int = None):
        url = f"{self.broker_url}/topics/{topic_name}/produce"
        params = {}
        if partition is not None:
            params["partition"] = partition

        async with AsyncClient() as client:
            try:
                response = await client.post(url, json=message, params=params)
                return response.json()
            except Exception as e:
                raise Exception(f"Failed to produce message: {e}")

    async def consume_message(self, topic_name:str, partition: int = None):
        if topic_name not in self.topics:
            raise Exception(f"Topic {topic_name} not found. Try again later.")
        if partition is not None:
            url = f"{self.broker_url}/topics/{topic_name}/consume/{partition}"
        else:
            url = f"{self.broker_url}/topics/{topic_name}/consume"

        async with AsyncClient() as client:
            try:
                response = await client.get(url)
                if response.status_code != 200:
                    raise Exception(f"Consume failed with status code {response.status_code}")
                return response.json()
            except Exception as e:
                raise Exception(f"Error consuming message: {e}")





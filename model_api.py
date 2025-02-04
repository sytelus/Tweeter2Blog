import requests
import os

class ModelAPI:
    def __init__(self):
        self.api_key = os.getenv("MODEL_API_KEY", "")
        self.api_endpoint = os.getenv("MODEL_API_ENDPOINT", "")
        self.model_name = os.getenv("MODEL_NAME", "")

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        self.available = all([self.api_key, self.api_endpoint, self.model_name])

    def send_message(self, message):
        if not self.available:
            return {"error": "Model API is not available."}

        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "user", "content": message}
            ]
        }

        response = requests.post(self.api_endpoint, headers=self.headers, json=payload)

        if response.status_code == 200:
            return response.json()
        else:
            return {"error": response.text}

# Usage Example:
# api = ModelAPI()
# response = api.send_message("Tell me a joke.")
# print(response)

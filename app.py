from fastapi import FastAPI
import requests
import random
import json
from src.models.uesr import Detail
from src.base64.cred import base64_string


app = FastAPI()


@app.post("/trigger_dag")
async def trigger_dag(info : Detail):

    dag_id = info.dag_name
    dag_run_id = random.randint(000000, 999999)

    url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"

    payload = json.dumps({
    "dag_run_id": f"{dag_run_id}"
    })

    headers = {
    'Authorization': f'Basic {base64_string}',
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if(response.status_code==200):
        return {"message": "DAG triggered"}
    return json.loads(response.text)

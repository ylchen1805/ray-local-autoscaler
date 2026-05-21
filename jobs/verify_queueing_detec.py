# jobs/verify_queueing_detec.py
import requests

response = requests.get("http://localhost:8265/api/cluster_status")
print(response.json())

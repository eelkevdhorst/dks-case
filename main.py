from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import polars as pl

from src.input import validate_record
from src.data_processing import add_processing_timestamp
from src.output import store_in_dynamodb, store_in_dynamodb_fake

app = FastAPI()

class ContainerData(BaseModel):
    container_id: str
    location: str
    timestamp: str
    status: str
    destination_depot: str

@app.post("/update-container")
async def update_container(data: ContainerData):
    """Endpoint to receive container data and update DynamoDB."""

    data = data.dict()

    try:
        # Validate timestamp
        validate_record(data)

        # processing data
        add_processing_timestamp(data)

        # Output/store data in DynamoDB
        store_in_dynamodb_fake(data) #to make it runnable without AWS access

        return {"message": "Container data updated successfully."}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



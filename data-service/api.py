from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

app = FastAPI(title="Pump Data/ML Service", version="1.0.0")

MODEL_VERSION = "v1"


class PumpRequest(BaseModel):
    tag: str = Field(..., min_length=1)
    flow_rate: float = Field(..., gt=0)     # обязательное
    fluid_head: float = Field(..., gt=0)    # обязательное

    # опциональные поля
    rpm: Optional[float] = Field(default=None, gt=0)
    spec_gravity: Optional[float] = Field(default=None, gt=0)
    power_kw: Optional[float] = Field(default=None, gt=0)
    pump_eff: Optional[float] = Field(default=None, gt=0)


class PumpResponse(BaseModel):
    model_version: str
    weight: float


@app.get("/health")
def health():
    return {"status": "ok", "model_version": MODEL_VERSION}


@app.post("/pump/estimate", response_model=PumpResponse)
def pump_estimate(req: PumpRequest):
    default_weight = 123.45
    return PumpResponse(model_version=MODEL_VERSION, weight=default_weight)
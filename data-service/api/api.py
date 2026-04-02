from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Any, Dict
from api.service import get_pump_estimation

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
    debug_features: Dict[str, Any]


@app.get("/health")
def health():
    return {"status": "ok", "model_version": MODEL_VERSION}


@app.post("/pump/estimate", response_model=PumpResponse)
def pump_estimate(req: PumpRequest):
    try:
        # 1. pydantic -> py.dict
        input_data = req.model_dump()

        # 2. вызов пайплайна и модели
        result = get_pump_estimation(input_data)

        # 3. ответ пользователю
        return PumpResponse(
            model_version=MODEL_VERSION,
            weight=result["weight"],
            debug_features=result["features"]
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
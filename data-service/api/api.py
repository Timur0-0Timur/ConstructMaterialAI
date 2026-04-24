from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Any, Dict

from .service import get_pump_estimation, get_vessel_estimation

app = FastAPI(title="Pump Data/ML Service", version="1.0.0")

MODEL_VERSION = "v1"


# ---------- REQUEST MODELS ----------

class PumpRequest(BaseModel):
    tag: str = Field(..., min_length=1)
    flow_rate: float = Field(..., gt=0)      # обязательное
    fluid_head: float = Field(..., gt=0)     # обязательное

    # опциональные
    rpm: Optional[float] = Field(default=None, gt=0)
    spec_gravity: Optional[float] = Field(default=None, gt=0)
    power_kw: Optional[float] = Field(default=None, gt=0)


class DrumRequest(BaseModel):
    tag: str = Field(..., min_length=1)
    vessel_diameter: float = Field(..., gt=0)                     # обязательное
    design_tangent_to_tangent_length: float = Field(..., gt=0)    # обязательное

    # опциональные (как на бэке)
    design_gauge_pressure: Optional[float] = Field(default=None, gt=0)
    design_temperature: Optional[float] = Field(default=None)
    operating_temperature: Optional[float] = Field(default=None)
    corossion_allowance: Optional[float] = Field(default=None, ge=0)  # допускаем 0


class VesselRequest(BaseModel):
    tag: str = Field(..., min_length=1)
    vessel_diameter: float = Field(..., gt=0)                       # обязательное
    vessel_tangent_to_tangent_height: float = Field(..., gt=0)      # обязательное

    # опциональные
    design_gauge_pressure: Optional[float] = Field(default=None, gt=0)
    design_temperature: Optional[float] = Field(default=None)
    operating_temperature: Optional[float] = Field(default=None)
    skirt_height: Optional[float] = Field(default=None, ge=0)        # допускаем 0
    vessel_leg_height: Optional[float] = Field(default=None, ge=0)   # допускаем 0


class ConveyorRequest(BaseModel):
    tag: str = Field(..., min_length=1)
    conveyor_length: float = Field(..., gt=0)     # обязательное
    belt_width: float = Field(..., gt=0)          # обязательное

    # опциональные
    conveyor_flow_rate: Optional[float] = Field(default=None, ge=0)          # допускаем 0
    driver_power_per_second: Optional[float] = Field(default=None, ge=0)     # допускаем 0
    conveyor_speed: Optional[float] = Field(default=None, ge=0)              # допускаем 0
    number_of_walkways: Optional[float] = Field(default=None, ge=0)          # допускаем 0


# ---------- RESPONSE MODEL ----------

class Response(BaseModel):
    model_version: str
    weight: float
    debug_features: Dict[str, Any]


# ---------- ROUTES ----------

@app.get("/health")
def health():
    return {"status": "ok", "model_version": MODEL_VERSION}


@app.post("/pump/estimate", response_model=Response)
def pump_estimate(req: PumpRequest):
    try:
        input_data = req.model_dump()
        result = get_pump_estimation(input_data)

        return Response(
            model_version=MODEL_VERSION,
            weight=result["weight"],
            debug_features=result["features"]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/drum/estimate", response_model=Response)
def drum_estimate(req: DrumRequest):
    try:
        input_data = req.model_dump()
        # TODO: здесь позже будет вызов модели для drum
        return Response(
            model_version=MODEL_VERSION,
            weight=8008.0,               # заглушка
            debug_features=input_data     # чтобы видеть, что пришло
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/vessel/estimate", response_model=Response)
def vessel_estimate(req: VesselRequest):
    try:
        input_data = req.model_dump()
        result = get_vessel_estimation(input_data)
        
        return Response(
            model_version=MODEL_VERSION,
            weight=result["weight"],
            debug_features=result["features"]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/conveyor/estimate", response_model=Response)
def conveyor_estimate(req: ConveyorRequest):
    try:
        input_data = req.model_dump()
        # TODO: здесь позже будет вызов модели для conveyor
        return Response(
            model_version=MODEL_VERSION,
            weight=7007.0,               # заглушка
            debug_features=input_data
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
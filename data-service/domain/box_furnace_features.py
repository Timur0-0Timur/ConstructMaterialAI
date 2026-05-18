import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BoxFurnaceFeatureEngineer:
    """Доменная логика и расчет веса для коробчатых технологических печей (Box type process furnace)"""

    def __init__(self, config: dict = None):
        self.config = config or {}

    def calculate_stub_weight(self, duty: float, gas_flow: float) -> float:
        """Временная заглушка: перемножает важные параметры для оценки веса (кг)
        
        Args:
            duty: Тепловая мощность (нагрузка) печи, MEGAW
            gas_flow: Расход сырья (Standard gas flow rate), L/S
        
        Returns:
            Оценочный вес в килограммах
        """
        logger.info(f"Расчет заглушки веса печи: Duty={duty} MW, Flow={gas_flow} L/S")
        return float(duty * gas_flow)

import logging

logger = logging.getLogger(__name__)


class CentrifugalCompressorFeatureEngineer:
    """Доменная логика и расчет веса для центробежных компрессоров (Centrifugal compressor - horizontal)"""

    def __init__(self, config: dict = None):
        self.config = config or {}

    def calculate_stub_weight(self, gas_flow_inlet: float, pressure_inlet: float, pressure_outlet: float) -> float:
        """Временная заглушка: перемножает важные параметры для оценки веса (кг)
        
        Args:
            gas_flow_inlet: Фактический объемный расход на всасывании
            pressure_inlet: Расчетное давление на всасывании (KPAG)
            pressure_outlet: Расчетное давление нагнетания (KPAG)
        
        Returns:
            Оценочный вес в килограммах
        """
        logger.info(
            f"Расчет заглушки веса компрессора: Flow={gas_flow_inlet}, "
            f"P_in={pressure_inlet}, P_out={pressure_outlet}"
        )
        return float(gas_flow_inlet * pressure_inlet * pressure_outlet)

# определяем начальные названия колонок
RAW_NAMES = {
    "tag": "User Tag number",
    "flow": "Liquid flow rate",
    "head": "Fluid head",
    "speed": "Speed",
    "gravity": "Fluid specific gravity",
    "power": "Driver power",
    "eff": "Pump efficiency",
    "weight_tag": "UserTag",
    "weight_val": "Weight: KG"
}

# внутренние названия
COL_NAMES = {
    "tag": "tag",
    "flow": "flow_rate",
    "head": "fluid_head",
    "rpm": "rpm",
    "gravity": "spec_gravity",
    "power_kw": "power_kw",
    "pump_eff": "pump_eff",
    "weight_kg": "weight_kg"
}

# собираем итоговый конфиг
INFERENCE_CONFIG = {
    "sheets": {
        "features": "TAG_FEATURES",
        "weight": None
    },

    "raw_names": RAW_NAMES,
    "col_names": COL_NAMES,

    "weight_col_to_drop": [
      "Name", "Parent Area", "Type", "# of Piping Lines"
    ],

    "cols_to_convert": [
        RAW_NAMES['flow'],
        RAW_NAMES['head'],
        RAW_NAMES['speed'],
        RAW_NAMES['gravity'],
        RAW_NAMES['power'],
        RAW_NAMES['eff'],
        RAW_NAMES['weight_val']
    ],

    "rename_map": {
        RAW_NAMES["tag"]: COL_NAMES["tag"],
        RAW_NAMES["flow"]: COL_NAMES["flow"],
        RAW_NAMES["head"]: COL_NAMES["head"],
        RAW_NAMES["speed"]: COL_NAMES["rpm"],
        RAW_NAMES["gravity"]: COL_NAMES["gravity"],
        RAW_NAMES["power"]: COL_NAMES["power_kw"],
        RAW_NAMES["eff"]: COL_NAMES["pump_eff"],
        RAW_NAMES["weight_val"]: COL_NAMES["weight_kg"]
    },

    "feat_eng": {
        "diameter_proxy": "diameter_proxy",
        "useful_kw": "useful_kw",
        "weight_log": "weight_log"
    },

    "db_params": {
        "table_prefix": "pump_inference_data"
    }
}
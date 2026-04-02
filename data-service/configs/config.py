# определяем названия колонок
RAW_NAMES = {
        "tag": "User Tag number",
        "flow": "Liquid flow rate",
        "head": "Fluid head",
        "speed": "Speed",
        "gravity": "Fluid specific gravity",
        "power": "Driver power",
        "weight_kg": "Equip Weight",
        "tag_weight": "Tag No"
}

# актуальные названия колонок
COL_NAMES = {
    "tag": "tag",
    "flow": "flow_rate",
    "head": "fluid_head",
    "rpm": "rpm",
    "gravity": "spec_gravity",
    "power_kw": "power_kw",
    "weight_kg": "weight_kg"
}

# основной конфиг
CONFIG = {
    "raw_names": RAW_NAMES,
    "col_names": COL_NAMES,

    "sheets": {
        "features": "TAG_FEATURES",
        "weight": "TAG_WEIGHT"
    },

    "cols_to_convert": [
        RAW_NAMES['flow'],
        RAW_NAMES['head'],
        RAW_NAMES['speed'],
        RAW_NAMES['gravity'],
        RAW_NAMES['power'],
        RAW_NAMES['weight_kg'],
    ],

    "rename_map": {
        RAW_NAMES["tag"]: COL_NAMES["tag"],
        RAW_NAMES["flow"]: COL_NAMES["flow"],
        RAW_NAMES["head"]: COL_NAMES["head"],
        RAW_NAMES["speed"]: COL_NAMES["rpm"],
        RAW_NAMES["gravity"]: COL_NAMES["gravity"],
        RAW_NAMES["power"]: COL_NAMES["power_kw"],
        RAW_NAMES["weight_kg"]: COL_NAMES["weight_kg"]
    },

    "feat_eng": {
        "diameter_proxy": "diameter_proxy",
        "useful_kw": "useful_kw",
        "weight_log": "weight_log"
    },

    "db_params": {
        "table_prefix": "pump_data"
    }
}
RAW_NAMES = {
    "tag": "User Tag number",
    "diameter": "Vessel diameter",
    "ss_dist": "Vessel tangent to tangent height",
    "pressure": "Design gauge pressure",
    "temp": "Design temperature",
    "tag_weight": "Tag",
    "weight_val": "Weight"
}

COL_NAMES = {
    "tag": "tag",
    "diameter": "diameter",
    "ss_dist": "ss_distance",
    "pressure": "pressure",
    "temp": "temp",
    "weight_kg": "weight_kg"
}

VESSEL_CONFIG = {
    "raw_names": RAW_NAMES,
    "col_names": COL_NAMES,
    "sheets": {
        "features": "features",
        "weight": "weight"
    },
    "cols_to_convert": [
        RAW_NAMES["diameter"], RAW_NAMES["ss_dist"],
        RAW_NAMES["pressure"], RAW_NAMES["temp"],
        RAW_NAMES["weight_val"]
    ],
    "rename_map": {
        RAW_NAMES["tag"]: COL_NAMES["tag"],
        RAW_NAMES["diameter"]: COL_NAMES["diameter"],
        RAW_NAMES["ss_dist"]: COL_NAMES["ss_dist"],
        RAW_NAMES["pressure"]: COL_NAMES["pressure"],
        RAW_NAMES["temp"]: COL_NAMES["temp"],
        RAW_NAMES["weight_val"]: COL_NAMES["weight_kg"]
    },
    "feat_eng": {
        "volume_proxy": "volume_proxy",
        "pressure_factor": "pressure_factor",
        "weight_log": "weight_log"
    },
    "db_params": {
        "table_prefix": "vessel_data"
    }
}
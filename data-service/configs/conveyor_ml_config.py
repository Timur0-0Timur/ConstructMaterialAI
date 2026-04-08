RAW_NAMES = {
    "tag": "User Tag number",
    "length": "Conveyor length",
    "width": "Belt width",
    "flow": "Conveyor flow rate",
    "power": "Driver power per section",
    "speed": "Conveyor speed",
    "walkways": "Number of walkways",
    "tag_weight": "Tag",
    "weight_val": "Weight"
}

COL_NAMES = {
    "tag": "tag",
    "length": "length_ft",
    "width": "width_in",
    "flow": "flow_tph",
    "power": "power_hp",
    "speed": "speed_fpm",
    "walkways": "walkways_count",
    "weight_kg": "weight_lb"
}

CONVEYOR_CONFIG = {
    "raw_names": RAW_NAMES,
    "col_names": COL_NAMES,
    "sheets": {
        "features": "features",
        "weight": "weight"
    },
    "cols_to_convert": [
        RAW_NAMES["length"], RAW_NAMES["width"], RAW_NAMES["flow"],
        RAW_NAMES["power"], RAW_NAMES["speed"], RAW_NAMES["walkways"],
        RAW_NAMES["weight_val"]
    ],
    "rename_map": {
        RAW_NAMES["tag"]: COL_NAMES["tag"],
        RAW_NAMES["length"]: COL_NAMES["length"],
        RAW_NAMES["width"]: COL_NAMES["width"],
        RAW_NAMES["flow"]: COL_NAMES["flow"],
        RAW_NAMES["power"]: COL_NAMES["power"],
        RAW_NAMES["speed"]: COL_NAMES["speed"],
        RAW_NAMES["walkways"]: COL_NAMES["walkways"],
        RAW_NAMES["weight_val"]: COL_NAMES["weight_kg"]
    },
    "feat_eng": {
        "belt_area": "belt_surface_area",
        "weight_log": "weight_log"
    },
    "db_params": {
        "table_prefix": "conveyor_data"
    }
}
COL_NAMES = {
    "tag": "tag",
    "flow": "flow_rate",
    "head": "fluid_head",
    "spec_gravity": "spec_gravity",
    "rpm": "rpm",
    "power_kw": "power_kw",
    "pump_eff": "pump_eff",
    "weight_kg": "weight_kg"
}

APP_CONFIG = {
    "critical_cols": [COL_NAMES["flow"], COL_NAMES["head"]],
    "col_names": COL_NAMES,

    "cols_to_clean": [
        COL_NAMES["flow"],
        COL_NAMES["head"],
        COL_NAMES["spec_gravity"],
        COL_NAMES["rpm"],
        COL_NAMES["power_kw"]
    ],

    "search_features": [
        COL_NAMES["flow"],
        COL_NAMES["head"]
    ],
    "target_features": [
        COL_NAMES["rpm"],
        COL_NAMES["power_kw"],
        COL_NAMES["spec_gravity"]
    ],

    "feat_eng": {
        "diameter_proxy": "diameter_proxy",
        "useful_kw": "useful_kw",
        "weight_log": "weight_log"
    }
}
CONFIG = {
    "raw_names": {
        "tag": "User Tag number",
        "flow": "Liquid flow rate",
        "head": "Fluid head",
        "speed": "Speed",
        "gravity": "Fluid specific gravity",
        "power": "Driver power",
        "eff": "Pump efficiency",
        "weight": "Equip Weight",
        "tag_weight": "Tag No"
    },

    "sheets": {
        "features": "TAG_FEATURES",
        "weight": "TAG_WEIGHT"
    },

    "cols_to_convert": [
        'Liquid flow rate', 'Fluid head', 'Speed', 'Driver power', 'Pump efficiency'
    ],

    "rename_map": {
        'User Tag number': 'tag',
        'Liquid flow rate': 'flow_rate',
        'Fluid head': 'fluid_head',
        'Speed': 'rpm',
        'Fluid specific gravity': 'spec_gravity',
        'Driver power': 'power_kw',
        'Pump efficiency': 'pump_eff',
        'Equip Weight': 'weight_kg'
    },

    "col_names": {
        "head": "fluid_head",
        "flow": "flow_rate",
        "weight": "weight_kg",
        "useful_kw": "useful_kw",
        "weight_log": "weight_log"
    },

    "ml_params": {
        "test_size": 0.2,
        "random_state": 42,
    },

    "db_params": {
        "table_prefix": "pump_data"
    },

    "anomaly_check_cols": ['rpm', 'spec_gravity', 'power_kw', 'pump_eff', 'useful_kw', 'weight_log'],
    "features_to_scale": ['rpm', 'spec_gravity', 'power_kw', 'pump_eff', 'useful_kw'],
    "contamination": 0.05
}
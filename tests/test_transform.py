print("✅ test_transform loaded")

import pandas as pd
from pipeline.transform import apply_calibration, tag_anomalies, calculate_z_scores

def test_debug_discovery():
    assert True  # Basic test just to confirm discovery

def test_apply_calibration():
    row = {"value": 25, "reading_type": "temperature"}
    result = apply_calibration(row)
    expected = 25 * 1.02 - 0.5
    assert round(result, 2) == round(expected, 2)

def test_tag_anomalies():
    test_df = pd.DataFrame([
        {"reading_type": "temperature", "value": 50},
        {"reading_type": "temperature", "value": 25},
    ])
    result_df = tag_anomalies(test_df)
    assert result_df.loc[0, "anomalous_reading"] is True
    assert result_df.loc[1, "anomalous_reading"] is False

def test_calculate_z_scores():
    test_df = pd.DataFrame({
        "sensor_id": ["a"] * 5,
        "reading_type": ["temp"] * 5,
        "value_calibrated": [10, 12, 14, 16, 100]
    })
    result_df = calculate_z_scores(test_df)
    assert result_df["is_outlier"].sum() == 1

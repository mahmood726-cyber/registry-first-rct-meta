import pandas as pd

from registry_first_ma.meta import meta_analyze_binary


def test_meta_analyze_binary_rr() -> None:
    df = pd.DataFrame(
        [
            {
                "trial_id": "NCT00000001",
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": "treatment",
                "events": 10,
                "total": 100,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            },
            {
                "trial_id": "NCT00000001",
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": "control",
                "events": 20,
                "total": 100,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            },
            {
                "trial_id": "NCT00000002",
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": "treatment",
                "events": 30,
                "total": 200,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            },
            {
                "trial_id": "NCT00000002",
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": "control",
                "events": 40,
                "total": 200,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            },
        ]
    )

    result = meta_analyze_binary(df, measure="RR")

    assert result["k"] == 2
    assert result["random"]["rr"] < 1.0
    assert "rr_ci_low" in result["random"]

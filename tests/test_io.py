from registry_first_ma.io import load_cochrane_datasets, load_topic_config

from .toy_data import write_toy_cochrane_csv, write_toy_cochrane_json, write_toy_topic_yaml


def test_load_cochrane_datasets(tmp_path) -> None:
    write_toy_cochrane_csv(tmp_path)
    write_toy_cochrane_json(tmp_path)

    reviews = load_cochrane_datasets(tmp_path, max_reviews=10, after_year=2005, rct_filter_toggle=True)
    review_ids = {r.review_id for r in reviews}

    assert "toy_review_1" in review_ids
    assert "toy_review_2" in review_ids


def test_load_topic_config_yaml(tmp_path) -> None:
    cfg_path = write_toy_topic_yaml(tmp_path)
    cfg = load_topic_config(cfg_path)
    assert cfg["review_id"] == "hfpef_sglt2"
    assert "heart failure" in cfg["condition_terms"]


def test_json_preserves_explicit_study_id(tmp_path) -> None:
    path = tmp_path / "one_review.json"
    path.write_text(
        """
{
  "review_id": "CD_TEST",
  "review_title": "CD_TEST",
  "included_studies": [
    {
      "study_id": "COCHRANE-CD_TEST-001",
      "citation_title": "Example Study 2001",
      "doi": "10.1002/14651858.CD_TEST.pub1"
    }
  ]
}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    reviews = load_cochrane_datasets(tmp_path, max_reviews=5, after_year=2000, rct_filter_toggle=False)
    assert len(reviews) == 1
    assert reviews[0].included_studies[0].study_id == "COCHRANE-CD_TEST-001"

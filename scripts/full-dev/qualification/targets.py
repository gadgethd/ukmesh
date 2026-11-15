"""Verify selected stores are the stores used by every actual consumer."""
from pathlib import Path

from common import STATE, read_json
from fixtures import SQLITE, bundle, current_targets


from consumer_refs import database_setting,validate_consumers


def runtime_targets(containers: dict, *, allow_public_variant: bool = False) -> list[dict]:
    targets = current_targets()
    selected_path = STATE / "runtime/selected-fixture.json"
    selected = read_json(selected_path) if selected_path.exists() else {"targets": "original"}
    directory = SQLITE.parent
    if selected.get("targets") != "original":
        fixture = bundle(selected["token"])
        directory = Path(fixture["corescope_data_dir"])
        mode_path = STATE / "runtime/corescope-mode.json"
        mode = read_json(mode_path) if mode_path.exists() else {}
        if mode.get("mode") == "public":
            if not allow_public_variant or mode.get("fixture_token") != fixture["token"]:
                raise ValueError("this workload requires the selected default MQTT data ownership")
            expected = directory.parent / "corescope-public"
            if Path(mode.get("data_dir", "")).resolve() != expected.resolve():
                raise ValueError("CoreScope public variant directory differs from its owned fixture")
            directory = expected
    return validate_consumers(containers, targets, directory)

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from sggg.nav_sheet_parse import list_capital_flow_candidates, pick_capital_flow_adjustment

body = {
    "ClassSeriesFundList": [
        {
            "ClassCode": "I",
            "SectionList": [
                {
                    "SectionName": "Equity",
                    "SectionItem": [
                        {"Name": "Adjusted Opening Equity Contributions", "Value": 9200000.0},
                        {"Name": "Units Contributions", "Value": 597266.8549},
                    ],
                }
            ],
        }
    ]
}
print(list_capital_flow_candidates(body))
print(pick_capital_flow_adjustment(body))

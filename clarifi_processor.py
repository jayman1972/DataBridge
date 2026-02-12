"""
Clarifi and EHP file processing for Data Bridge.
Used by /clarifi/process and /ehp/process endpoints.
"""
import re
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional

try:
    from dateutil import parser as dateutil_parser
    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False


def normalize_date(date_str: str) -> Optional[str]:
    """Normalize date string to YYYY-MM-DD format"""
    if not date_str:
        return None
    month_year_match = re.match(r'^([A-Za-z]{3})-(\d{2})$', str(date_str).strip())
    if month_year_match:
        month_abbr = month_year_match.group(1)
        year_short = int(month_year_match.group(2))
        year = 2000 + year_short if year_short < 50 else 1900 + year_short
        month_map = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
                     "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
        month_num = month_map.get(month_abbr.lower())
        if month_num:
            return f"{year}-{month_num:02d}-01"
    if HAS_DATEUTIL:
        try:
            from datetime import datetime
            dt = dateutil_parser.parse(str(date_str))
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def parse_delimited_file(file_path: Path, delimiter: str = '\t') -> List[Dict[str, Any]]:
    """Parse tab or comma delimited file"""
    rows = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if len(lines) < 2:
                return rows
            headers = [h.strip().lower().replace('"', '') for h in lines[0].strip().split(delimiter)]
            for line in lines[1:]:
                line = line.strip()
                if not line:
                    continue
                values = [v.strip().replace('"', '') for v in line.split(delimiter)]
                if len(values) != len(headers):
                    continue
                row = {}
                for i, header in enumerate(headers):
                    value = values[i]
                    try:
                        row[header] = float(value) if value else None
                    except ValueError:
                        row[header] = value if value else None
                rows.append(row)
    except Exception as e:
        print(f"Error parsing file {file_path}: {e}")
    return rows


def process_clarifi_file(file_path: Path, supabase_client) -> Dict[str, Any]:
    """Process a Clarifi file and upload to Supabase"""
    result = {"file": str(file_path.name), "inserted": 0, "errors": []}
    if not supabase_client:
        result["errors"].append("Supabase client not initialized")
        return result
    file_name_lower = file_path.name.lower()
    column_mappings = {}
    if 'macrodataexport' in file_name_lower:
        column_mappings = {
            "median % change in actual eps q over q 1 yr ago": "estimates_median_pct_change_actual_eps_q_over_q_1yr_ago",
            "median % change in eps estimates q over q 1 yr ago": "estimates_median_pct_change_eps_estimates_q_over_q_1yr_ago",
            "median % change in actual margins q over q 1 yr ago": "estimates_median_pct_change_actual_margins_q_over_q_1yr_ago",
        }
    elif 'oildemand' in file_name_lower:
        column_mappings = {"PX_LAST": "oil_supply_demand_index"}
    elif 'diffusionindexexport' in file_name_lower:
        column_mappings = {
            "fed liquidity index 1 day lag": "fed_liq_raw_value",
            "hf flow - financials sector mean position score (mkt cap weight)": "hf_flow_financials_sector_mean_position_score_mkt_cap",
            "hf flow - communication services sector mean position score (mkt cap weight)": "hf_flow_communication_services_sector_mean_position_score_mkt_c",
            "hf flow - utilities sector mean position score (mkt cap weight)": "hf_flow_utilities_sector_mean_position_score_mkt_cap",
            "hf flow - real estate sector mean position score (mkt cap weight)": "hf_flow_real_estate_sector_mean_position_score_mkt_cap",
            "hf flow - health care sector mean position score (mkt cap weight)": "hf_flow_health_care_sector_mean_position_score_mkt_cap",
            "hf flow - consumer discretionary sector mean position score (mkt cap weight)": "hf_flow_consumer_discretionary_sector_mean_position_score_mkt_c",
            "hf flow - consumer staples sector mean position score (mkt cap weight)": "hf_flow_consumer_staples_sector_mean_position_score_mkt_cap",
            "hf flow - energy sector mean position score (mkt cap weight)": "hf_flow_energy_sector_mean_position_score_mkt_cap",
            "hf flow - industrials sector mean position score (mkt cap weight)": "hf_flow_industrials_sector_mean_position_score_mkt_cap",
            "hf flow - materials sector mean position score (mkt cap weight)": "hf_flow_materials_sector_mean_position_score_mkt_cap",
            "hf flow - information technology sector mean position score (mkt cap weight)": "hf_flow_information_technology_sector_mean_position_score_mkt_c",
            "hf flow - metals & mining industry mean position score (mkt cap weight)": "hf_flow_metals_mining_industry_mean_position_score_mkt_cap",
            "hf flow - semiconductor industry mean position score (mkt cap weight)": "hf_flow_semiconductor_industry_mean_position_score_mkt_cap",
            "hf flow - financials sector mean position score (equal weight)": "hf_flow_financials_sector_mean_position_score_equal",
            "hf flow - communication services sector mean position score (equal weight)": "hf_flow_communication_services_sector_mean_position_score_equal",
            "hf flow - utilities sector mean position score (equal weight)": "hf_flow_utilities_sector_mean_position_score_equal",
            "hf flow - real estate sector mean position score (equal weight)": "hf_flow_real_estate_sector_mean_position_score_equal",
            "hf flow - health care sector mean position score (equal weight)": "hf_flow_health_care_sector_mean_position_score_equal",
            "hf flow - consumer discretionary sector mean position score (equal weight)": "hf_flow_consumer_discretionary_sector_mean_position_score_equal",
            "hf flow - consumer staples sector mean position score (equal weight)": "hf_flow_consumer_staples_sector_mean_position_score_equal",
            "hf flow - energy sector mean position score (equal weight)": "hf_flow_energy_sector_mean_position_score_equal",
            "hf flow - industrials sector mean position score (equal weight)": "hf_flow_industrials_sector_mean_position_score_equal",
            "hf flow - materials sector mean position score (equal weight)": "hf_flow_materials_sector_mean_position_score_equal",
            "hf flow - information technology sector mean position score (equal weight)": "hf_flow_information_technology_sector_mean_position_score_equal",
            "hf flow - metals & mining industry mean position score (equal weight)": "hf_flow_metals_mining_industry_mean_position_score_equal",
            "hf flow - semiconductors & semiconductors equipment industry position score (equal weight)": "hf_flow_semiconductors_equipment_industry_position_score_equal",
            "hf flow - mag7 ownership": "hf_flow_mag7",
        }
    else:
        result["errors"].append(f"Unknown file type: {file_path.name}")
        return result
    try:
        rows = []
        if 'oildemand' in file_name_lower:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if len(lines) < 7:
                result["errors"].append("OilDemand.csv file too short")
                return result
            headers = [h.strip().lower().replace('"', '') for h in lines[5].strip().split(',')]
            for i in range(6, len(lines)):
                line = lines[i].strip()
                if not line:
                    continue
                values = []
                current_value = ""
                in_quotes = False
                for char in line:
                    if char == '"':
                        in_quotes = not in_quotes
                    elif char == ',' and not in_quotes:
                        values.append(current_value.strip().replace('"', ''))
                        current_value = ""
                    else:
                        current_value += char
                values.append(current_value.strip().replace('"', ''))
                if len(values) != len(headers):
                    continue
                row = {}
                for j, header in enumerate(headers):
                    value = values[j]
                    if header in ['date', 'dates', 'month']:
                        row[header] = value if value else None
                    else:
                        try:
                            row[header] = float(value) if value else None
                        except ValueError:
                            row[header] = value if value else None
                rows.append(row)
        else:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                delimiter = '\t' if '\t' in first_line and ',' not in first_line else ','
            rows = parse_delimited_file(file_path, delimiter)
        if not rows:
            result["errors"].append("No data rows found")
            return result
        existing_dates_map = {}
        for db_column in column_mappings.values():
            try:
                response = supabase_client.table("market_data").select("date").not_.is_(db_column, None).execute()
                existing_dates_map[db_column] = {row["date"] for row in response.data if row.get("date")}
            except Exception:
                existing_dates_map[db_column] = set()
        records_map = {}
        for row in rows:
            date_str = row.get("date") or row.get("dates") or row.get("month") or ""
            if not date_str:
                continue
            date = normalize_date(str(date_str))
            if not date:
                continue
            has_existing = any(date in existing_dates_map.get(c, set()) for c in column_mappings.values())
            if has_existing:
                continue
            if date not in records_map:
                records_map[date] = {"date": date}
            record = records_map[date]
            for csv_column, db_column in column_mappings.items():
                value = row.get(csv_column.lower())
                if value is not None and value != "":
                    try:
                        record[db_column] = float(value)
                    except (ValueError, TypeError):
                        pass
        records_to_insert = [r for r in records_map.values() if any(k != "date" for k in r)]
        if records_to_insert:
            for i in range(0, len(records_to_insert), 100):
                batch = records_to_insert[i:i + 100]
                try:
                    supabase_client.table("market_data").upsert(batch, on_conflict="date").execute()
                    result["inserted"] += len(batch)
                except Exception as e:
                    result["errors"].append(str(e))
    except Exception as e:
        result["errors"].append(str(e))
        traceback.print_exc()
    return result

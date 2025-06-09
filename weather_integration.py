import requests
import pandas as pd
import csv  # Added for proper CSV quoting
from datetime import datetime
import time
import sys
import re
import os

def integrate_weather_alerts(input_file, output_file):
    """
    Main integration function for AWS Lambda
    """

    try:
        print(f"Loading data from: {input_file}")
        df_main = pd.read_csv(input_file)
        print(f"Loaded {len(df_main)} rows, {len(df_main.columns)} columns")
        
        # find CWA column
        cwa_column = None
        possible_cwa_columns = ["CWA_Region", "CWA_region", "CWA", "Weather_Office", "NWS_Office"]
        
        for col in possible_cwa_columns:
            if col in df_main.columns:
                cwa_column = col
                break
        
        if not cwa_column:
            print(f"ERROR: CWA column not found. Available: {list(df_main.columns)}")
            return False
        
        print(f"Using CWA column: {cwa_column}")
        
        # unique CWA offices
        cwa_offices = df_main[cwa_column].dropna().unique()
        cwa_offices_set = set(cwa.upper() for cwa in cwa_offices)
        print(f"Found {len(cwa_offices)} unique CWA offices")
        
        # fetch alerts from NWS API
        alerts = fetch_nws_alerts()
        if not alerts:
            print("No alerts retrieved - continuing with empty data")
            matched_alerts = []
        else:
            matched_alerts = match_alerts_to_cwa(alerts, cwa_offices_set)
        
        # create enhanced dataset
        if matched_alerts:
            print("Creating enhanced dataset...")
            df_alerts = pd.DataFrame(matched_alerts)
            
            # join with original data
            df_enhanced = df_main.merge(
                df_alerts,
                left_on=cwa_column,
                right_on='cwa_office',
                how='left'
            )
            
            # fix pandas deprecation warning
            # ensure proper boolean types
            df_enhanced = df_enhanced.infer_objects(copy=False)
            
            # convert string booleans to actual booleans for Tableau
            df_enhanced['alert_active'] = df_enhanced['alert_active'].replace({'True': True, 'False': False}).fillna(False).astype(bool)
            df_enhanced['has_active_alerts'] = df_enhanced['alert_active']
            
            # add summary columns
            df_enhanced['alert_count'] = df_enhanced.groupby(cwa_column)['alert_id'].transform('count').fillna(0)
            df_enhanced['max_severity_score'] = df_enhanced.groupby(cwa_column)['severity_score'].transform('max').fillna(0)
            
            print(f"Enhanced: {len(df_enhanced)} rows, {len(df_enhanced.columns)} columns")
            
        else:
            print("No alerts matched - adding empty columns")
            df_enhanced = df_main.copy()
            
            # add empty alert columns with proper null values
            empty_columns = {
                'cwa_office': None, 'alert_id': None, 'event_type': None, 'severity': None,
                'urgency': None, 'certainty': None, 'status': None, 'headline': None,
                'description': None, 'areas_affected': None, 'effective_time': None,
                'expires_time': None, 'message_type': None, 'alert_active': False,
                'severity_score': 0, 'last_updated': None, 'matching_method': None,
                'has_active_alerts': False, 'alert_count': 0, 'max_severity_score': 0
            }
            
            for col, default_val in empty_columns.items():
                df_enhanced[col] = default_val
        
        # clean text fields to prevent CSV parsing issues for Tableau S3
        text_fields = ['Organization Name', 'Primary Address Street', 'headline', 'description', 'areas_affected']
        
        for field in text_fields:
            if field in df_enhanced.columns:
                df_enhanced[field] = df_enhanced[field].astype(str).apply(
                    lambda x: x.replace('"', '""').replace('\n', ' ').replace('\r', ' ') if pd.notna(x) and x != 'nan' else ''
                )
        
        # save enhanced dataset with Tableau S3-friendly CSV settings
        # this took some trial/error - may require additional tweaks
        print(f"Saving to: {output_file}")
        df_enhanced.to_csv(
            output_file, 
            index=False,
            encoding='utf-8',
            quoting=csv.QUOTE_MINIMAL,
            escapechar=None,
            doublequote=True,
            lineterminator='\n'
        )
        
        print(f"SUCCESS! Final dataset: {len(df_enhanced)} rows, {len(df_enhanced.columns)} columns")
        return True
        
    except Exception as e:
        print(f"Integration error: {str(e)}")
        return False

def fetch_nws_alerts():
    """Fetch all active alerts from NWS API"""
    
    try:
        url = "https://api.weather.gov/alerts/active"
        headers = {'User-Agent': 'AWSLambda-WeatherDashboard/1.0'}
        
        response = requests.get(url, headers=headers, timeout=60)
        print(f"API Response: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            features = data.get('features', [])
            print(f"etrieved {len(features)} alerts")
            return features
        else:
            print(f"API Error: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"Error fetching alerts: {e}")
        return []

def match_alerts_to_cwa(alerts, cwa_offices_set):
    """Match alerts to CWA offices"""
    
    matched_alerts = []
    
    for alert in alerts:
        props = alert['properties']
        alert_office = None
        matching_method = None
        
        # first try alert ID matching
        alert_id = props.get('id', '')
        if alert_id:
            id_parts = alert_id.split('-')
            for part in id_parts:
                if len(part) == 3 and part.upper() in cwa_offices_set:
                    alert_office = part.upper()
                    matching_method = "alert_id"
                    break
        
        # then try zone URL matching
        if not alert_office and 'affectedZones' in props:
            zones = props.get('affectedZones', [])
            for zone_url in zones:
                zone_match = re.search(r'/zones/[^/]+/([A-Z]{2,3})[CZ]?\d+', zone_url, re.IGNORECASE)
                if zone_match:
                    zone_office = zone_match.group(1).upper()
                    if zone_office in cwa_offices_set:
                        alert_office = zone_office
                        matching_method = "affected_zones"
                        break
        
        # roll up to state mapping if CWA not specified
        if not alert_office:
            area_desc = props.get('areaDesc', '').upper()
            state_cwa_mapping = {
                'CALIFORNIA': ['LOX', 'MTR', 'SGX', 'HNX', 'STO', 'EKA', 'MFR'],
                'TEXAS': ['FWD', 'HGX', 'EWX', 'LZK', 'EPZ'],
                'FLORIDA': ['MFL', 'TBW', 'JAX', 'MLB'],
                'NEW YORK': ['OKX', 'ALY', 'BGM', 'BUF'],
                'PENNSYLVANIA': ['PHI', 'PBZ', 'BGM', 'CTP'],
                'ILLINOIS': ['LOT', 'ILX'],
                'VIRGINIA': ['LWX', 'AKQ', 'RNK'],
                'WASHINGTON': ['SEW', 'OTX', 'PQR'],
                'COLORADO': ['BOU', 'GJT', 'PUB'],
                'MONTANA': ['TFX', 'MSO', 'BYZ', 'GGW'],
                'NORTH CAROLINA': ['RAH', 'GSP', 'ILM', 'MHX'],
                'SOUTH CAROLINA': ['CAE', 'CHS', 'GSP'],
                'GEORGIA': ['FFC', 'JAX'],
                'ALABAMA': ['BMX', 'HUN', 'MOB'],
                'TENNESSEE': ['OHX', 'MEG'],
                'KENTUCKY': ['JKL', 'PAH', 'LMK'],
                'OHIO': ['CLE', 'ILN', 'PBZ'],
                'MICHIGAN': ['DTX', 'GRR', 'APX'],
                'WISCONSIN': ['MKX', 'GRB', 'MPX'],
                'MINNESOTA': ['MPX', 'DLH'],
                'IOWA': ['DVN', 'DMX', 'ARX'],
                'MISSOURI': ['SGF', 'LSX', 'EAX'],
                'ARKANSAS': ['LZK', 'SHV', 'TSA'],
                'LOUISIANA': ['LIX', 'SHV', 'LCH'],
                'MISSISSIPPI': ['JAN', 'LIX'],
                'OKLAHOMA': ['OUN', 'TSA'],
                'KANSAS': ['ICT', 'TOP', 'DDC'],
                'NEBRASKA': ['OAX', 'GID', 'LBF'],
                'SOUTH DAKOTA': ['FSD', 'ABR', 'UNR'],
                'NORTH DAKOTA': ['BIS', 'FGF', 'GFK'],
                'WYOMING': ['CYS', 'RIW'],
                'UTAH': ['SLC'],
                'NEVADA': ['REV', 'VEF', 'LKN'],
                'ARIZONA': ['PSR', 'TWC', 'FGZ'],
                'NEW MEXICO': ['ABQ', 'EPZ'],
                'IDAHO': ['BOI', 'PIH', 'MSO'],
                'OREGON': ['PQR', 'MFR', 'PDT'],
                'ALASKA': ['AFC', 'AJK', 'AFG'],
                'HAWAII': ['HFO'],
                'PUERTO RICO': ['SJU']
            }
            
            for state, possible_cwas in state_cwa_mapping.items():
                if state in area_desc:
                    for cwa in possible_cwas:
                        if cwa in cwa_offices_set:
                            alert_office = cwa
                            matching_method = "state_mapping"
                            break
                    if alert_office:
                        break
        
        # text content matching
        if not alert_office:
            text_fields = [
                props.get('headline', ''),
                props.get('description', ''),
                props.get('instruction', '')
            ]
            
            for text in text_fields:
                if text:
                    text_upper = text.upper()
                    for cwa in cwa_offices_set:
                        if f"NWS {cwa}" in text_upper or f" {cwa} " in text_upper:
                            alert_office = cwa
                            matching_method = "text_content"
                            break
                    if alert_office:
                        break

        if alert_office and alert_office in cwa_offices_set:
            # fix areas_affected field to handle both string and list properly
            area_desc = props.get('areaDesc', '')
            if isinstance(area_desc, list):
                areas_affected = '; '.join(area_desc)
            else:
                areas_affected = area_desc if area_desc else None
            
            alert_data = {
                'cwa_office': alert_office,
                'alert_id': props.get('id', '').split('/')[-1] if props.get('id') else None,
                'event_type': props.get('event', '') if props.get('event') else None,
                'severity': props.get('severity', '') if props.get('severity') else None,
                'urgency': props.get('urgency', '') if props.get('urgency') else None,
                'certainty': props.get('certainty', '') if props.get('certainty') else None,
                'status': props.get('status', '') if props.get('status') else None,
                'headline': props.get('headline', '') if props.get('headline') else None,
                'description': (props.get('description') or '')[:300] if props.get('description') else None,
                'areas_affected': areas_affected,
                'effective_time': props.get('effective', '') if props.get('effective') else None,
                'expires_time': props.get('expires', '') if props.get('expires') else None,
                'message_type': props.get('messageType', '') if props.get('messageType') else None,
                'alert_active': is_alert_active(props),
                'severity_score': get_severity_score(props.get('severity', '')),
                'last_updated': datetime.now().isoformat(),
                'matching_method': matching_method
            }
            
            matched_alerts.append(alert_data)
    
    print(f"Matched {len(matched_alerts)} alerts")
    return matched_alerts

def is_alert_active(props):
    """Check if alert is currently active"""
    try:
        effective = props.get('effective')
        expires = props.get('expires')
        status = props.get('status')
        
        if status != 'Actual':
            return False
            
        if effective and expires:
            now = datetime.now()
            effective_dt = pd.to_datetime(effective).to_pydatetime()
            expires_dt = pd.to_datetime(expires).to_pydatetime()
            return effective_dt <= now <= expires_dt
        
        return True
    except:
        return False

def get_severity_score(severity):
    """Convert severity to numeric score"""
    severity_map = {
        'Extreme': 4,
        'Severe': 3,
        'Moderate': 2,
        'Minor': 1,
        'Unknown': 0,
        '': 0
    }
    return severity_map.get(severity, 0)
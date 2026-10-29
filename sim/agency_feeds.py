#!/usr/bin/env python3
"""
External agency feeds for the COP mesh: seven systems, three protocols, one topic space.

  1. DIA Threat Intelligence      REST  -> cop/intel/{ioc,report,actor}/v1/...
  2. Maritime Domain Awareness (MDA, maritime — not missile defense)   MQTT  -> cop/sea/ais/v1/...   cop/intel/{port,voi}/v1/...
  3. CENTCOM Force Readiness      REST  -> cop/intel/{readiness,equipment}/v1/...
  4. NGA Geospatial Intelligence  MQTT  -> cop/intel/{geoint,restriction}/v1/...
  5. Coalition Partner C2         SMF   -> cop/intel/{coalition-air,coalition-naval,coalition}/v1/...
  6. Defense Logistics Agency     REST  -> cop/intel/{supply,shipment}/v1/...
  7. FAA Civil Aviation           MQTT  -> cop/air/adsb/v1/...

The agencies that "only speak HTTP" POST straight to the broker's REST
messaging port; the sensor-style feeds use MQTT; the partner mesh uses the
native API. Nothing is translated by application code: the broker accepts all
three and every subscriber sees one hierarchy.
"""
import json
import random
import time
import threading
import uuid
import hashlib
import math
from datetime import datetime, timedelta, timezone
import os
import logging

import requests
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('agency-feeds')

# All three protocols terminate on the CONUS broker (the enterprise side).
SOLACE_HOST = os.environ.get("SOLACE_HOST", "tcp://localhost:55555")
REST_URL = os.environ.get("SOLACE_REST", "http://localhost:9000")
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
VPN = os.environ.get("SOLACE_VPN", "cop")
USER = os.environ.get("SOLACE_USER", "agency-feeds")
PASSWORD = os.environ.get("APP_PASSWORD", "cop-demo")


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def aor(lat, lon):
    if lat is None or lon is None:
        return "global"
    if lon >= 90:
        return "indopacom"
    if lat < 17 and lon < 44.5:
        return "africom"
    if lon < 33.5 or lat >= 36.5:
        return "eucom"
    return "centcom"


def lvl(s):
    return str(s).strip().lower().replace("/", "-").replace("*", "-").replace(">", "-").replace(" ", "-")


# ============================================================
# 1. DIA THREAT INTELLIGENCE DATA
# ============================================================
# Shared with the main simulator: cyber events draw ~40% of source IPs from this pool,
# and the alerting subscriber learns this list from the DIA feed on the mesh.
KNOWN_BAD_IPS = ['185.220.101.34', '91.108.4.17', '45.155.205.233', '103.75.190.12', '194.26.29.156', '185.156.73.91', '91.219.236.222', '45.9.148.114', '103.253.41.98', '194.165.16.11', '185.234.72.19', '91.240.118.172']

THREAT_ACTORS = [
    {'actor_id': 'TA-APT28', 'name': 'APT28 / Fancy Bear', 'country': 'RU', 'affiliation': 'GRU Unit 26165', 'active_since': '2004',
     'primary_targets': ['Government', 'Military', 'Defense Contractors', 'Media'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['Olympic Destroyer', 'SolarWinds Adjacent', 'Bundestag Hack']},
    {'actor_id': 'TA-APT29', 'name': 'APT29 / Cozy Bear', 'country': 'RU', 'affiliation': 'SVR Foreign Intelligence Service', 'active_since': '2008',
     'primary_targets': ['Government', 'Think Tanks', 'Healthcare', 'Energy'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['SolarWinds SUNBURST', 'WellMess Campaign', 'COVID Vaccine Espionage']},
    {'actor_id': 'TA-SANDWORM', 'name': 'Sandworm Team', 'country': 'RU', 'affiliation': 'GRU Unit 74455', 'active_since': '2009',
     'primary_targets': ['Critical Infrastructure', 'Energy Grid', 'ICS/SCADA', 'Elections'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['NotPetya', 'Ukraine Power Grid 2015/2016', 'Cyclops Blink']},
    {'actor_id': 'TA-APT41', 'name': 'APT41 / Double Dragon', 'country': 'CN', 'affiliation': 'MSS / Chengdu 404 Network', 'active_since': '2012',
     'primary_targets': ['Healthcare', 'Telecom', 'Technology', 'Gaming'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['ShadowPad', 'Winnti Backdoor', 'Supply Chain Attacks']},
    {'actor_id': 'TA-APT40', 'name': 'APT40 / Leviathan', 'country': 'CN', 'affiliation': 'MSS Hainan State Security', 'active_since': '2013',
     'primary_targets': ['Maritime', 'Defense', 'Aviation', 'Chemical'], 'sophistication': 'TIER_1_ADVANCED',
     'known_operations': ['South China Sea Espionage', 'Naval Technology Theft', 'University Targeting']},
    {'actor_id': 'TA-LAZARUS', 'name': 'Lazarus Group', 'country': 'KP', 'affiliation': 'RGB Bureau 121', 'active_since': '2007',
     'primary_targets': ['Financial', 'Cryptocurrency', 'Defense', 'Entertainment'], 'sophistication': 'TIER_2_MODERATE',
     'known_operations': ['Sony Hack', 'WannaCry', 'Bangladesh Bank Heist', 'Axie Infinity']},
    {'actor_id': 'TA-MUDDYWATER', 'name': 'MuddyWater', 'country': 'IR', 'affiliation': 'MOIS Intelligence Ministry', 'active_since': '2017',
     'primary_targets': ['Government', 'Telecom', 'Oil & Gas', 'Middle East Targets'], 'sophistication': 'TIER_2_MODERATE',
     'known_operations': ['Earth Vetala', 'Telecommunication Targeting', 'Exchange Exploitation']},
    {'actor_id': 'TA-CHARMING', 'name': 'Charming Kitten / APT35', 'country': 'IR', 'affiliation': 'IRGC Intelligence', 'active_since': '2014',
     'primary_targets': ['Academic', 'Journalists', 'Dissidents', 'Government'], 'sophistication': 'TIER_2_MODERATE',
     'known_operations': ['HBO Hack', 'Academic Credential Theft', 'Phosphorus Campaign']},
]

MITRE_TECHNIQUES = [
    {'id': 'T1566.001', 'name': 'Spearphishing Attachment', 'tactic': 'Initial Access', 'severity': 'HIGH'},
    {'id': 'T1190', 'name': 'Exploit Public-Facing Application', 'tactic': 'Initial Access', 'severity': 'CRITICAL'},
    {'id': 'T1059.001', 'name': 'PowerShell Execution', 'tactic': 'Execution', 'severity': 'HIGH'},
    {'id': 'T1053.005', 'name': 'Scheduled Task', 'tactic': 'Persistence', 'severity': 'MEDIUM'},
    {'id': 'T1078', 'name': 'Valid Accounts', 'tactic': 'Defense Evasion', 'severity': 'CRITICAL'},
    {'id': 'T1048.002', 'name': 'Exfiltration Over Asymmetric Encrypted Channel', 'tactic': 'Exfiltration', 'severity': 'CRITICAL'},
    {'id': 'T1071.001', 'name': 'Web Protocols C2', 'tactic': 'Command and Control', 'severity': 'HIGH'},
    {'id': 'T1027', 'name': 'Obfuscated Files or Information', 'tactic': 'Defense Evasion', 'severity': 'MEDIUM'},
    {'id': 'T1003.001', 'name': 'LSASS Memory Credential Dump', 'tactic': 'Credential Access', 'severity': 'CRITICAL'},
    {'id': 'T1021.001', 'name': 'Remote Desktop Protocol', 'tactic': 'Lateral Movement', 'severity': 'HIGH'},
    {'id': 'T1486', 'name': 'Data Encrypted for Impact', 'tactic': 'Impact', 'severity': 'CRITICAL'},
    {'id': 'T1505.003', 'name': 'Web Shell', 'tactic': 'Persistence', 'severity': 'HIGH'},
]

IOC_TYPES = ['ipv4', 'domain', 'hash_sha256', 'hash_md5', 'url', 'email', 'file_path', 'registry_key']
MALWARE_FAMILIES = ['SUNBURST', 'Cobalt Strike', 'Mimikatz', 'ShadowPad', 'PlugX', 'Emotet', 'TrickBot', 'Ryuk',
                    'BlackCat/ALPHV', 'Cyclops Blink', 'WellMess', 'NotPetya', 'TEARDROP', 'GoldMax', 'Winnti',
                    'DUSTMAN', 'Shamoon', 'MuddyC3', 'POWERSTATS', 'BladedWrench']
CENTCOM_REGIONS = ['CENTCOM AOR', 'Persian Gulf', 'Red Sea', 'Eastern Mediterranean', 'Horn of Africa', 'Central Asia']
COUNTRY_THREAT_LEVELS = {
    'RU': {'level': 'CRITICAL', 'posture': 'AGGRESSIVE', 'priority': 1},
    'CN': {'level': 'HIGH', 'posture': 'PERSISTENT', 'priority': 2},
    'IR': {'level': 'HIGH', 'posture': 'REGIONAL_AGGRESSIVE', 'priority': 3},
    'KP': {'level': 'ELEVATED', 'posture': 'OPPORTUNISTIC', 'priority': 4},
}

# ============================================================
# 2. MARITIME DOMAIN AWARENESS DATA
# ============================================================
COMMERCIAL_VESSEL_TYPES = [
    'VLCC', 'Suezmax Tanker', 'Aframax Tanker', 'Product Tanker', 'Chemical Tanker', 'LNG Carrier', 'LPG Carrier',
    'Container Ship', 'Bulk Carrier', 'General Cargo', 'Ro-Ro', 'Car Carrier',
    'Fishing Vessel', 'Research Vessel', 'Passenger Ferry', 'Cruise Ship', 'Yacht', 'Tug', 'Supply Vessel', 'Cable Layer', 'Dredger']
VESSEL_FLAGS = [
    {'country': 'PA', 'name': 'Panama'}, {'country': 'LR', 'name': 'Liberia'}, {'country': 'MH', 'name': 'Marshall Islands'},
    {'country': 'HK', 'name': 'Hong Kong'}, {'country': 'SG', 'name': 'Singapore'}, {'country': 'MT', 'name': 'Malta'},
    {'country': 'CN', 'name': 'China'}, {'country': 'JP', 'name': 'Japan'}, {'country': 'IR', 'name': 'Iran'},
    {'country': 'RU', 'name': 'Russia'}, {'country': 'AE', 'name': 'UAE'}, {'country': 'SA', 'name': 'Saudi Arabia'},
    {'country': 'GB', 'name': 'United Kingdom'}, {'country': 'US', 'name': 'United States'}, {'country': 'IN', 'name': 'India'},
    {'country': 'GR', 'name': 'Greece'}, {'country': 'NO', 'name': 'Norway'}, {'country': 'TR', 'name': 'Turkey'},
    {'country': 'KR', 'name': 'South Korea'}, {'country': 'BS', 'name': 'Bahamas'},
]
SHIPPING_LANES = [
    {'name': 'Strait of Hormuz', 'lat_range': (26.2, 26.8), 'lon_range': (56.0, 56.8)},
    {'name': 'Bab el-Mandeb', 'lat_range': (12.4, 13.0), 'lon_range': (43.0, 43.6)},
    {'name': 'Suez Canal Approach', 'lat_range': (29.8, 31.0), 'lon_range': (32.0, 33.0)},
    {'name': 'Persian Gulf Central', 'lat_range': (25.0, 28.0), 'lon_range': (50.0, 54.0)},
    {'name': 'Gulf of Oman', 'lat_range': (23.5, 25.5), 'lon_range': (57.0, 60.0)},
    {'name': 'Red Sea North', 'lat_range': (22.0, 28.0), 'lon_range': (34.0, 38.0)},
    {'name': 'Red Sea South', 'lat_range': (13.0, 20.0), 'lon_range': (38.0, 43.0)},
    {'name': 'Arabian Sea', 'lat_range': (15.0, 22.0), 'lon_range': (55.0, 65.0)},
    {'name': 'Eastern Mediterranean', 'lat_range': (31.0, 36.0), 'lon_range': (28.0, 36.0)},
    {'name': 'Gulf of Aden', 'lat_range': (11.5, 14.0), 'lon_range': (43.0, 51.0)},
]
# Open-water routes: waypoints that stay in the channel. Ships steam along one and
# turn around at the ends, so nothing sails over Riyadh. (The lane boxes above are
# only used to pick a region; positions come from these.)
SEA_ROUTES = {
    'Red Sea':        [(27.6, 34.3), (26.2, 35.4), (24.2, 36.9), (21.8, 38.3), (19.5, 39.3), (17.0, 40.6), (15.0, 41.8), (13.2, 43.0)],
    'Persian Gulf':   [(29.2, 48.9), (28.4, 49.6), (27.3, 50.6), (26.5, 51.9), (25.6, 53.4), (26.0, 54.6), (26.4, 55.6), (26.5, 56.4)],
    'Gulf of Oman':   [(26.3, 56.9), (25.3, 57.6), (24.4, 58.7), (23.6, 59.8), (22.3, 60.6), (20.5, 60.6)],
    'Arabian Sea':    [(20.5, 60.6), (18.5, 59.2), (16.5, 57.5), (15.0, 55.0), (13.8, 52.0)],
    'Gulf of Aden':   [(13.8, 52.0), (13.2, 49.5), (12.8, 47.0), (12.5, 44.6), (12.7, 43.4)],
    'Eastern Med':    [(32.0, 31.8), (32.8, 32.8), (33.6, 33.8), (34.6, 33.2), (35.3, 34.8), (34.0, 35.0)],
}
def route_seed(route):
    pts = SEA_ROUTES[route]; i = random.randint(0, len(pts) - 2); f = random.random()
    lat = pts[i][0] + f * (pts[i+1][0] - pts[i][0]); lon = pts[i][1] + f * (pts[i+1][1] - pts[i][1])
    return {'route': route, 'leg': i, 'dir': random.choice([1, -1]), 'lat': round(lat, 6), 'lon': round(lon, 6)}

def follow_route(t, nm):
    """Move t `nm` nautical miles along its route toward the next waypoint; bounce at the ends."""
    pts = SEA_ROUTES[t['route']]
    for _ in range(20):
        if nm <= 0.01:
            break
        nxt = max(0, min(len(pts) - 1, t['leg'] + (1 if t['dir'] > 0 else 0)))
        tlat, tlon = pts[nxt]
        dlat, dlon = tlat - t['lat'], (tlon - t['lon']) * math.cos(math.radians(t['lat']))
        dist = math.hypot(dlat, dlon) * 60.0
        t['heading_deg'] = round(math.degrees(math.atan2(dlon, dlat)) % 360, 1)
        if dist <= nm:
            t['lat'], t['lon'] = tlat, tlon; nm -= dist
            if t['dir'] > 0:
                if nxt >= len(pts) - 1: t['dir'] = -1
                else: t['leg'] = nxt
            else:
                if nxt <= 0: t['dir'] = 1
                else: t['leg'] = nxt - 1
        else:
            f = nm / dist
            t['lat'] = round(t['lat'] + dlat * f, 6); t['lon'] = round(t['lon'] + (tlon - t['lon']) * f, 6); nm = 0
    return t

PORTS = [
    {'name': 'Jebel Ali', 'country': 'AE', 'lat': 25.02, 'lon': 55.06}, {'name': 'Fujairah', 'country': 'AE', 'lat': 25.12, 'lon': 56.35},
    {'name': 'Ras Tanura', 'country': 'SA', 'lat': 26.63, 'lon': 50.16}, {'name': 'Bandar Abbas', 'country': 'IR', 'lat': 27.18, 'lon': 56.28},
    {'name': 'Jeddah', 'country': 'SA', 'lat': 21.49, 'lon': 39.17}, {'name': 'Djibouti', 'country': 'DJ', 'lat': 11.59, 'lon': 43.14},
    {'name': 'Port Said', 'country': 'EG', 'lat': 31.26, 'lon': 32.31}, {'name': 'Karachi', 'country': 'PK', 'lat': 24.85, 'lon': 66.99},
    {'name': 'Muscat', 'country': 'OM', 'lat': 23.62, 'lon': 58.57}, {'name': 'Aden', 'country': 'YE', 'lat': 12.80, 'lon': 45.03},
]
# Vessel-of-interest reasons, by what the flag makes plausible. A US-flagged hull
# is never a VOI; an allied or neutral flag gets behaviour-based reasons; an
# adversary flag gets state- and sanctions-based ones. Tankers get cargo reasons.
VOI_REASONS = {
    'adversary': ['Sanctions evasion - flagged vessel', 'State-linked ownership chain', 'Previously flagged for arms transport',
                  'AIS transponder disabled in sensitive area', 'Unusual loitering near military installation', 'Operating in restricted military zone'],
    'tanker':    ['Ship-to-ship transfer suspected', 'AIS gap near sanctioned terminal', 'Cargo origin inconsistent with declaration',
                  'Deviating from declared route', 'Reflagged within 90 days (flag of convenience)'],
    'other':     ['AIS transponder disabled in sensitive area', 'Deviating from declared route', 'Unusual loitering near military installation',
                  'Vessel associated with known smuggling network', 'Crew manifest discrepancy at last port'],
}
ADVERSARY_FLAGS = {'IR', 'RU', 'CN', 'KP'}
ALLIED_FLAGS = {'US'}

def voi_reason_for(v):
    if v['flag_country'] in ADVERSARY_FLAGS:
        pool = VOI_REASONS['adversary'] + (VOI_REASONS['tanker'] if 'Tanker' in v['vessel_type'] or v['vessel_type'] == 'VLCC' else [])
    elif 'Tanker' in v['vessel_type'] or v['vessel_type'] in ('VLCC', 'LNG Carrier', 'LPG Carrier'):
        pool = VOI_REASONS['tanker']
    else:
        pool = VOI_REASONS['other']
    return random.choice(pool)

# ============================================================
# 3. CENTCOM FORCE READINESS DATA
# ============================================================
CENTCOM_UNITS = [
    {'unit_id': 'UNIT-5ID', 'name': '5th Infantry Division', 'type': 'INFANTRY', 'base': 'Camp Arifjan', 'lat': 28.933, 'lon': 48.100, 'auth': 18000},
    {'unit_id': 'UNIT-1AD', 'name': '1st Armored Division', 'type': 'ARMOR', 'base': 'Camp Buehring', 'lat': 29.28, 'lon': 47.68, 'auth': 16000},
    {'unit_id': 'UNIT-82ABN', 'name': '82nd Airborne Division', 'type': 'AIRBORNE', 'base': 'Prince Sultan AB', 'lat': 24.062, 'lon': 47.580, 'auth': 14000},
    {'unit_id': 'UNIT-101ABN', 'name': '101st Airborne (Air Assault)', 'type': 'AIR_ASSAULT', 'base': 'Al Udeid AB', 'lat': 25.117, 'lon': 51.315, 'auth': 15000},
    {'unit_id': 'UNIT-3MEF', 'name': '3rd Marine Expeditionary Force', 'type': 'MARINES', 'base': 'NSA Bahrain', 'lat': 26.237, 'lon': 50.652, 'auth': 12000},
    {'unit_id': 'UNIT-CSG5', 'name': 'Carrier Strike Group 5', 'type': 'NAVAL', 'base': 'NSA Bahrain', 'lat': 26.237, 'lon': 50.652, 'auth': 7500},
    {'unit_id': 'UNIT-380AEW', 'name': '380th Air Expeditionary Wing', 'type': 'AIR_FORCE', 'base': 'Al Dhafra AB', 'lat': 24.248, 'lon': 54.547, 'auth': 5000},
    {'unit_id': 'UNIT-332AEW', 'name': '332nd Air Expeditionary Wing', 'type': 'AIR_FORCE', 'base': 'Al Udeid AB', 'lat': 25.117, 'lon': 51.315, 'auth': 4500},
    {'unit_id': 'UNIT-NAVCENT', 'name': 'US Naval Forces Central Command', 'type': 'NAVAL_HQ', 'base': 'NSA Bahrain', 'lat': 26.237, 'lon': 50.652, 'auth': 3000},
    {'unit_id': 'UNIT-SOCCENT', 'name': 'Special Operations Command Central', 'type': 'SOF', 'base': 'MacDill Fwd', 'lat': 25.30, 'lon': 51.50, 'auth': 2000},
]
EQUIPMENT_TYPES = [
    {'type': 'M1A2 Abrams', 'category': 'ARMOR'}, {'type': 'M2A3 Bradley', 'category': 'IFV'},
    {'type': 'HMMWV', 'category': 'VEHICLE'}, {'type': 'M777 Howitzer', 'category': 'ARTILLERY'},
    {'type': 'AH-64E Apache', 'category': 'ROTARY_WING'}, {'type': 'UH-60M Black Hawk', 'category': 'ROTARY_WING'},
    {'type': 'F-15E Strike Eagle', 'category': 'FIXED_WING'}, {'type': 'F-35A Lightning II', 'category': 'FIXED_WING'},
    {'type': 'MQ-9 Reaper', 'category': 'UAS'}, {'type': 'Patriot PAC-3', 'category': 'AIR_DEFENSE'},
    {'type': 'THAAD', 'category': 'MISSILE_DEFENSE'}, {'type': 'CH-47F Chinook', 'category': 'ROTARY_WING'},
]

# ============================================================
# 4. NGA GEOSPATIAL INTELLIGENCE DATA
# ============================================================
NGA_AREAS = [
    {'name': 'Strait of Hormuz Chokepoint', 'type': 'MARITIME_CHOKEPOINT', 'lat': 26.5, 'lon': 56.4, 'radius_km': 50},
    {'name': 'Bandar Abbas Naval Complex', 'type': 'MILITARY_INSTALLATION', 'lat': 27.15, 'lon': 56.25, 'radius_km': 15},
    {'name': 'Bushehr Nuclear Facility', 'type': 'NUCLEAR_SITE', 'lat': 28.83, 'lon': 50.88, 'radius_km': 10},
    {'name': 'Natanz Enrichment Plant', 'type': 'NUCLEAR_SITE', 'lat': 33.72, 'lon': 51.73, 'radius_km': 8},
    {'name': 'Tartus Naval Base (RU)', 'type': 'FOREIGN_MILITARY', 'lat': 34.89, 'lon': 35.87, 'radius_km': 10},
    {'name': 'Hmeimim Air Base (RU)', 'type': 'FOREIGN_MILITARY', 'lat': 35.41, 'lon': 35.95, 'radius_km': 12},
    {'name': 'Bab el-Mandeb Chokepoint', 'type': 'MARITIME_CHOKEPOINT', 'lat': 12.6, 'lon': 43.3, 'radius_km': 40},
    {'name': 'Suez Canal Zone', 'type': 'CRITICAL_INFRASTRUCTURE', 'lat': 30.45, 'lon': 32.35, 'radius_km': 30},
    {'name': 'Camp Lemonnier Perimeter', 'type': 'US_INSTALLATION', 'lat': 11.55, 'lon': 43.15, 'radius_km': 5},
    {'name': 'Chabahar Port Complex', 'type': 'DUAL_USE_PORT', 'lat': 25.30, 'lon': 60.62, 'radius_km': 10},
    {'name': 'Socotra Island', 'type': 'STRATEGIC_TERRAIN', 'lat': 12.50, 'lon': 54.00, 'radius_km': 25},
    {'name': 'Qeshm Island Military Zone', 'type': 'MILITARY_INSTALLATION', 'lat': 26.85, 'lon': 55.90, 'radius_km': 15},
]
NGA_CHANGE_TYPES = [
    'New construction detected', 'Vehicle movement observed', 'Increased activity at site',
    'Vessel concentration anomaly', 'SAM battery repositioned', 'Troop staging area detected',
    'Underground facility entrance activity', 'Runway extension in progress', 'Port loading activity surge',
    'Camouflage netting deployed', 'Radar installation identified', 'Road/infrastructure development',
]

# ============================================================
# 5. COALITION PARTNER C2 DATA
# ============================================================
COALITION_NATIONS = [
    {'code': 'GBR', 'name': 'United Kingdom', 'alliance': 'FIVE_EYES'},
    {'code': 'AUS', 'name': 'Australia', 'alliance': 'FIVE_EYES'},
    {'code': 'FRA', 'name': 'France', 'alliance': 'NATO'},
    {'code': 'DEU', 'name': 'Germany', 'alliance': 'NATO'},
    {'code': 'JPN', 'name': 'Japan', 'alliance': 'BILATERAL'},
]
COALITION_AIRCRAFT = {
    'GBR': ['Typhoon FGR4', 'F-35B Lightning', 'Voyager KC3', 'RC-135W Rivet Joint'],
    'AUS': ['F/A-18F Super Hornet', 'EA-18G Growler', 'P-8A Poseidon', 'E-7A Wedgetail'],
    'FRA': ['Rafale C', 'Rafale M', 'E-3F Sentry', 'Atlantique 2'],
    'DEU': ['Eurofighter Typhoon', 'A400M Atlas', 'Tornado ECR'],
    'JPN': ['F-35A Lightning II', 'P-1 Maritime Patrol', 'E-767 AWACS'],
}
COALITION_NAVAL = {
    'GBR': ['HMS Queen Elizabeth (CVN)', 'HMS Daring (DDG)', 'HMS Astute (SSN)', 'RFA Tidespring (AO)'],
    'AUS': ['HMAS Hobart (DDG)', 'HMAS Collins (SSK)', 'HMAS Canberra (LHD)'],
    'FRA': ['FS Charles de Gaulle (CVN)', 'FS Aquitaine (FFG)', 'FS Suffren (SSN)'],
    'DEU': ['FGS Sachsen (FFG)', 'FGS U-36 (SSK)'],
    'JPN': ['JS Izumo (DDH)', 'JS Atago (DDG)', 'JS Soryu (SSK)'],
}

# ============================================================
# 6. DLA DEFENSE LOGISTICS DATA
# ============================================================
DLA_SUPPLY_CLASSES = [
    {'cls': 'I', 'name': 'Subsistence', 'unit': 'short_tons', 'critical_days': 14},
    {'cls': 'III', 'name': 'POL (Fuel)', 'unit': 'gallons', 'critical_days': 7},
    {'cls': 'V', 'name': 'Ammunition', 'unit': 'short_tons', 'critical_days': 10},
    {'cls': 'VIII', 'name': 'Medical Material', 'unit': 'pallets', 'critical_days': 7},
    {'cls': 'IX', 'name': 'Repair Parts', 'unit': 'pallets', 'critical_days': 14},
]
DLA_DEPOTS = [
    {'depot_id': 'DEP-AJ', 'name': 'Camp Arifjan LSA', 'lat': 28.933, 'lon': 48.100, 'cap': 1.0},
    {'depot_id': 'DEP-BA', 'name': 'NSA Bahrain Logistics', 'lat': 26.237, 'lon': 50.652, 'cap': 0.6},
    {'depot_id': 'DEP-DJ', 'name': 'Camp Lemonnier Supply', 'lat': 11.55, 'lon': 43.15, 'cap': 0.4},
    {'depot_id': 'DEP-PS', 'name': 'Prince Sultan AB Depot', 'lat': 24.062, 'lon': 47.580, 'cap': 0.7},
    {'depot_id': 'DEP-AD', 'name': 'Al Dhafra AB Supply', 'lat': 24.248, 'lon': 54.547, 'cap': 0.5},
    {'depot_id': 'DEP-AU', 'name': 'Al Udeid AB Logistics', 'lat': 25.117, 'lon': 51.315, 'cap': 0.8},
]

# ============================================================
# 7. FAA CIVIL AVIATION DATA
# ============================================================
CIVIL_AIRCRAFT_TYPES = ['B737-800', 'B737 MAX 8', 'B777-300ER', 'B787-9', 'B747-8F', 'A320neo', 'A321neo', 'A330-300', 'A350-900', 'A380-800']
CIVIL_AIRLINES = [
    {'code': 'UAE', 'name': 'Emirates'}, {'code': 'ETH', 'name': 'Ethiopian Airlines'}, {'code': 'QTR', 'name': 'Qatar Airways'},
    {'code': 'BAW', 'name': 'British Airways'}, {'code': 'AFR', 'name': 'Air France'}, {'code': 'DLH', 'name': 'Lufthansa'},
    {'code': 'THY', 'name': 'Turkish Airlines'}, {'code': 'SVA', 'name': 'Saudia'}, {'code': 'AIC', 'name': 'Air India'},
    {'code': 'IRA', 'name': 'Iran Air'}, {'code': 'FDB', 'name': 'flydubai'}, {'code': 'MEA', 'name': 'Middle East Airlines'},
]
CIVIL_AIRPORTS = [
    {'icao': 'OMDB', 'name': 'Dubai Intl', 'lat': 25.25, 'lon': 55.36}, {'icao': 'OTHH', 'name': 'Hamad Intl', 'lat': 25.27, 'lon': 51.61},
    {'icao': 'OEJN', 'name': 'Jeddah Intl', 'lat': 21.68, 'lon': 39.16}, {'icao': 'OBBI', 'name': 'Bahrain Intl', 'lat': 26.27, 'lon': 50.63},
    {'icao': 'OOMS', 'name': 'Muscat Intl', 'lat': 23.59, 'lon': 58.28}, {'icao': 'OIIE', 'name': 'Tehran IKA', 'lat': 35.42, 'lon': 51.15},
    {'icao': 'LTFM', 'name': 'Istanbul Intl', 'lat': 41.27, 'lon': 28.74}, {'icao': 'OPKC', 'name': 'Karachi Intl', 'lat': 24.91, 'lon': 67.16},
    {'icao': 'HECA', 'name': 'Cairo Intl', 'lat': 30.12, 'lon': 31.41}, {'icao': 'HDAM', 'name': 'Djibouti Ambouli', 'lat': 11.55, 'lon': 43.16},
]


# ============================================================
# GENERATOR CLASS
# ============================================================


# ============================================================
# TRANSPORTS: one per kind of agency
# ============================================================
class RestFeed:
    """Agencies that only speak HTTP: POST to the broker's REST messaging port.
    The URL path is the topic; a header picks guaranteed vs direct delivery."""
    def __init__(self):
        self.s = requests.Session()
        self.s.auth = (USER, PASSWORD)
        self.s.headers["content-type"] = "application/json"
        self.sent = 0

    def publish(self, topic, body, guaranteed=True):
        r = self.s.post(f"{REST_URL}/{topic}", data=json.dumps(body),
                        headers={"Solace-Delivery-Mode": "persistent" if guaranteed else "direct"}, timeout=5)
        if r.status_code != 200:
            raise RuntimeError(f"REST {r.status_code} {r.text[:120]}")
        self.sent += 1


class MqttFeed:
    """Sensor-style feeds: MQTT, same topic strings, QoS 1 for things that must arrive."""
    def __init__(self):
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="feed-mda-nga-faa-mqtt", protocol=mqtt.MQTTv311)
        self.c.username_pw_set(USER, PASSWORD)
        self.c.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.c.loop_start()
        self.sent = 0

    def publish(self, topic, body, guaranteed=True):
        self.c.publish(topic, json.dumps(body), qos=1 if guaranteed else 0)
        self.sent += 1


class SmfFeed:
    """The partner mesh: native Solace API."""
    def __init__(self):
        from solace.messaging.messaging_service import MessagingService, RetryStrategy
        from solace.messaging.resources.topic import Topic
        from solace.messaging.config.solace_properties import transport_layer_properties as T
        from solace.messaging.config.solace_properties import service_properties as S
        from solace.messaging.config.solace_properties import authentication_properties as A
        from solace.messaging.config.solace_properties import client_properties as CP
        self.Topic = Topic
        props = {T.HOST: SOLACE_HOST, S.VPN_NAME: VPN, CP.NAME: "feed-coalition-c2-smf",
                 A.SCHEME_BASIC_USER_NAME: USER, A.SCHEME_BASIC_PASSWORD: PASSWORD}
        self.svc = (MessagingService.builder().from_properties(props)
                    .with_reconnection_retry_strategy(RetryStrategy.forever_retry(3000)).build())
        self.svc.connect()
        self.direct = self.svc.create_direct_message_publisher_builder().build(); self.direct.start()
        self.persistent = self.svc.create_persistent_message_publisher_builder().build(); self.persistent.start()
        self.sent = 0

    def publish(self, topic, body, guaranteed=True):
        (self.persistent if guaranteed else self.direct).publish(json.dumps(body), self.Topic.of(topic))
        self.sent += 1


# ============================================================
# THE SEVEN SYSTEMS
# ============================================================
class AgencyFeeds:
    def __init__(self):
        self.rest = self.mq = self.smf = None
        self.running = False
        self.mda_vessels, self.faa_aircraft = [], []
        self.centcom_equipment, self.dla_levels = {}, {}

    def connect(self):
        self.rest = RestFeed()
        self.mq = MqttFeed()
        self.smf = SmfFeed()
        logger.info(f"✓ REST {REST_URL} · MQTT {MQTT_HOST}:{MQTT_PORT} · SMF {SOLACE_HOST}  (user {USER})")
        return True

    # ---- 1. DIA (REST) -------------------------------------------------------
    def _init_dia(self):
        for actor in THREAT_ACTORS:
            rec = {**actor, 'status': 'ACTIVE', 'last_activity': now_iso(), 'updated_by': 'DIA_ANALYST', 'updated_at': now_iso(), 'agency': 'DIA', 'feed': 'actor'}
            self.rest.publish(f"cop/intel/actor/v1/global/dia/{lvl(actor['actor_id'])}/active", rec)
        names = {'RU': 'Russia', 'CN': 'China', 'IR': 'Iran', 'KP': 'North Korea'}
        for country, a in COUNTRY_THREAT_LEVELS.items():
            rec = {'country_code': country, 'country_name': names.get(country), 'threat_level': a['level'], 'cyber_posture': a['posture'],
                   'priority': a['priority'], 'active_campaigns': random.randint(2, 8), 'assessment_date': now_iso(), 'source': 'DIA_THREAT_INTEL_DB', 'agency': 'DIA', 'feed': 'assessment'}
            self.rest.publish(f"cop/intel/assessment/v1/global/dia/{lvl(country)}/{lvl(a['level'])}", rec)
        for ip in KNOWN_BAD_IPS:
            actor = random.choice(THREAT_ACTORS)
            rec = {'ioc_id': f"IOC-{hashlib.md5(ip.encode()).hexdigest()[:10].upper()}", 'ioc_type': 'ipv4', 'ioc_value': ip,
                   'threat_actor': actor['name'], 'actor_id': actor['actor_id'], 'actor_country': actor['country'],
                   'malware_family': random.choice(MALWARE_FAMILIES), 'mitre_technique': 'T1071', 'mitre_name': 'Application Layer Protocol',
                   'severity': 'HIGH', 'confidence': 'HIGH', 'created_at': now_iso(), 'source': 'DIA_THREAT_INTEL_DB', 'agency': 'DIA', 'feed': 'ioc'}
            self.rest.publish(f"cop/intel/ioc/v1/global/dia/{lvl(rec['ioc_id'])}/high", rec)
        logger.info(f"  Published {len(THREAT_ACTORS)} threat actors, {len(COUNTRY_THREAT_LEVELS)} assessments, {len(KNOWN_BAD_IPS)} known-bad IOCs over REST")

    def _dia_ioc_loop(self):
        while self.running:
            time.sleep(random.uniform(15, 45))
            try:
                actor = random.choice(THREAT_ACTORS); tech = random.choice(MITRE_TECHNIQUES); ioc_type = random.choice(IOC_TYPES)
                if ioc_type == 'ipv4':
                    val = random.choice(KNOWN_BAD_IPS) if random.random() < 0.5 else f"{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"
                elif ioc_type == 'domain':
                    val = f"{random.choice(['secure','update','cdn','api','vpn'])}-{random.randint(1,99)}{random.choice(['.xyz','.top','.club','.cc'])}"
                elif ioc_type.startswith('hash'):
                    val = hashlib.sha256(str(random.random()).encode()).hexdigest()[:64 if 'sha' in ioc_type else 32]
                else:
                    val = f"{''.join(random.choices('abcdef0123456789', k=12))}"
                ioc = {'ioc_id': f"IOC-{uuid.uuid4().hex[:10].upper()}", 'ioc_type': ioc_type, 'ioc_value': val,
                       'threat_actor': actor['name'], 'actor_id': actor['actor_id'], 'actor_country': actor['country'],
                       'malware_family': random.choice(MALWARE_FAMILIES), 'mitre_technique': tech['id'], 'mitre_name': tech['name'],
                       'severity': tech['severity'], 'confidence': random.choice(['HIGH', 'HIGH', 'MODERATE']),
                       'created_at': now_iso(), 'source': 'DIA_THREAT_INTEL_DB', 'agency': 'DIA', 'feed': 'ioc', 'summary': f"{ioc_type} {val} → {actor['name']}"}
                self.rest.publish(f"cop/intel/ioc/v1/global/dia/{lvl(ioc['ioc_id'])}/{lvl(ioc['severity'])}", ioc)
                logger.info(f"[DIA/REST] IOC: {ioc_type} → {actor['name']}")
            except Exception as e:
                logger.error(f"DIA IOC error: {e}")

    def _dia_report_loop(self):
        while self.running:
            time.sleep(random.uniform(60, 180))
            try:
                actor = random.choice(THREAT_ACTORS); region = random.choice(CENTCOM_REGIONS)
                report = {'report_id': f"DIA-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{random.randint(1000,9999)}",
                          'title': f"{actor['name']} activity targeting {region}", 'threat_actor': actor['name'], 'actor_country': actor['country'],
                          'severity': random.choice(['CRITICAL', 'HIGH', 'HIGH', 'MEDIUM']),
                          'techniques': [{'id': t['id'], 'name': t['name']} for t in random.sample(MITRE_TECHNIQUES, k=random.randint(2, 4))],
                          'published_at': now_iso(), 'source': 'DIA_THREAT_INTEL_DB', 'agency': 'DIA', 'feed': 'report'}
                report['summary'] = report['title']
                self.rest.publish(f"cop/intel/report/v1/centcom/dia/{lvl(report['report_id'])}/{lvl(report['severity'])}", report)
                logger.info(f"[DIA/REST] Report: {report['title'][:50]}")
            except Exception as e:
                logger.error(f"DIA report error: {e}")

    def _dia_assessment_loop(self):
        """Country assessments are re-issued periodically, as a real feed would."""
        names = {'RU': 'Russia', 'CN': 'China', 'IR': 'Iran', 'KP': 'North Korea'}
        while self.running:
            time.sleep(random.uniform(60, 120))
            try:
                country = random.choice(list(COUNTRY_THREAT_LEVELS)); a = COUNTRY_THREAT_LEVELS[country]
                rec = {'country_code': country, 'country_name': names.get(country), 'threat_level': a['level'], 'cyber_posture': a['posture'],
                       'priority': a['priority'], 'active_campaigns': random.randint(2, 8), 'assessment_date': now_iso(), 'source': 'DIA_THREAT_INTEL_DB', 'agency': 'DIA', 'feed': 'assessment'}
                self.rest.publish(f"cop/intel/assessment/v1/global/dia/{lvl(country)}/{lvl(a['level'])}", rec)
                logger.info(f"[DIA/REST] Assessment refreshed: {rec['country_name']} {a['level']}")
            except Exception as e:
                logger.error(f"DIA assessment error: {e}")

    def _dia_actor_update_loop(self):
        while self.running:
            time.sleep(random.uniform(120, 300))
            try:
                actor = random.choice(THREAT_ACTORS)
                rec = {**actor, 'last_activity': now_iso(), 'threat_score': random.randint(60, 99), 'updated_at': now_iso(),
                       'updated_by': f'DIA-ANALYST-{random.randint(100,999)}', 'agency': 'DIA', 'feed': 'actor', 'summary': f"{actor['name']} profile updated"}
                self.rest.publish(f"cop/intel/actor/v1/global/dia/{lvl(actor['actor_id'])}/active", rec)
                logger.info(f"[DIA/REST] Updated: {actor['name']}")
            except Exception as e:
                logger.error(f"DIA actor error: {e}")

    # ---- 2. MDA (MQTT) -------------------------------------------------------
    def _init_mda(self):
        self.mda_vessels = []
        for i in range(60):
            seed = route_seed(random.choice(list(SEA_ROUTES))); vtype = random.choice(COMMERCIAL_VESSEL_TYPES); flag = random.choice(VESSEL_FLAGS)
            speed = random.uniform(5, 18) if vtype not in ['Fishing Vessel', 'Tug'] else random.uniform(2, 8)
            prefix = random.choice(['OCEAN', 'SEA', 'STAR', 'GOLDEN', 'BLUE', 'PACIFIC', 'GULF', 'EVER', 'MAERSK'])
            suffix = random.choice(['FORTUNE', 'SPIRIT', 'GRACE', 'PRIDE', 'GLORY', 'HARMONY', 'PIONEER', 'NAVIGATOR', 'TRADER', 'VOYAGER'])
            v = {'mmsi': f"{random.randint(200,799)}{random.randint(100000,999999)}", 'vessel_name': f"{prefix} {suffix}",
                 'vessel_type': vtype, 'flag_country': flag['country'], 'flag_name': flag['name'],
                 **seed, 'speed_knots': round(speed, 1), 'heading_deg': round(random.uniform(0, 360), 1), 'destination': random.choice(PORTS)['name'],
                 'last_update': now_iso(), 'source': 'ONI_MDA_AIS_FEED', 'agency': 'MDA', 'feed': 'ais'}
            self.mda_vessels.append(v)
            self._ais(v)
        # The watch list. Vessels of interest are rare and specific: an Iranian-flagged hull working the Gulf / Hormuz,
        # and a Houthi-linked dhow on the Yemeni Red Sea coast. These two are the only hulls the VOI feed ever names.
        self.watchlist = [
            {'mmsi': '422031770', 'vessel_name': 'TOUSKA', 'vessel_type': 'General Cargo', 'flag_country': 'IR', 'flag_name': 'Iran',
             **route_seed('Persian Gulf'), 'speed_knots': 9.0, 'heading_deg': 110.0, 'destination': 'Bandar Abbas',
             'voi_reason': 'IRGC-N affiliated support vessel · AIS dark periods off Bandar Abbas · loitering near the Hormuz TSS', 'voi_severity': 'CRITICAL',
             'last_update': now_iso(), 'source': 'ONI_MDA_AIS_FEED', 'agency': 'MDA', 'feed': 'ais'},
            {'mmsi': '473111902', 'vessel_name': 'AL KHAIR', 'vessel_type': 'Dhow', 'flag_country': 'YE', 'flag_name': 'Yemen',
             'route': 'Red Sea', 'leg': 6, 'dir': 1, 'lat': 14.6, 'lon': 42.6, 'speed_knots': 6.0, 'heading_deg': 20.0, 'destination': 'Hodeidah',
             'voi_reason': 'Houthi-linked · matches ONI advisory for weapons transshipment · dark transit from Hodeidah', 'voi_severity': 'HIGH',
             'last_update': now_iso(), 'source': 'ONI_MDA_AIS_FEED', 'agency': 'MDA', 'feed': 'ais'},
        ]
        for v in self.watchlist:
            self.mda_vessels.append(v); self._ais(v)
        logger.info(f"  Seeded {len(self.mda_vessels)} commercial vessels over MQTT ({len(self.watchlist)} on the ONI watch list)")

    def _ais(self, v):
        self.mq.publish(f"cop/sea/ais/v1/{aor(v['lat'], v['lon'])}/mda/{v['mmsi']}/{lvl(v['flag_country'])}", v, guaranteed=False)

    def _mda_vessel_loop(self):
        while self.running:
            time.sleep(random.uniform(3, 8))
            try:
                v = random.choice(self.mda_vessels)
                follow_route(v, v['speed_knots'] / 3600.0 * random.uniform(120, 300))   # 2-5 min of steaming per update
                v['speed_knots'] = round(max(0.5, v['speed_knots'] + random.uniform(-0.5, 0.5)), 1)
                v['last_update'] = now_iso()
                self._ais(v)
            except Exception as e:
                logger.error(f"MDA vessel error: {e}")

    def _mda_port_loop(self):
        while self.running:
            time.sleep(random.uniform(20, 60))
            try:
                port = random.choice(PORTS); v = random.choice(self.mda_vessels)
                a = {'event_id': f"PORT-{uuid.uuid4().hex[:8].upper()}", 'port_name': port['name'], 'port_lat': port['lat'], 'port_lon': port['lon'],
                     'vessel_name': v['vessel_name'], 'vessel_type': v['vessel_type'], 'vessel_flag': v['flag_country'],
                     'event_type': random.choice(['ARRIVAL', 'DEPARTURE', 'ANCHORED', 'PILOT_BOARDED']), 'timestamp': now_iso(),
                     'source': 'ONI_MDA_AIS_FEED', 'agency': 'MDA', 'feed': 'port'}
                a['summary'] = f"{a['event_type']} {v['vessel_name']} at {port['name']}"
                self.mq.publish(f"cop/intel/port/v1/{aor(port['lat'], port['lon'])}/mda/{lvl(a['event_id'])}/{lvl(a['event_type'])}", a)
                logger.info(f"[MDA maritime/MQTT] Port: {a['summary']}")
            except Exception as e:
                logger.error(f"MDA port error: {e}")

    def _mda_voi_loop(self):
        """One or two vessels of interest, and only the watch-list hulls (coastal Iran, Houthi): the VOI advisory is
        re-issued every few minutes so a late-joining fusion service picks it up, never a new random ship."""
        first = True
        while self.running:
            time.sleep(5 if first else random.uniform(240, 420)); first = False
            try:
                v = random.choice(self.watchlist)
                a = {'alert_id': f"VOI-{v['mmsi'][-4:]}", 'vessel_name': v['vessel_name'], 'vessel_flag': v['flag_country'],
                     'vessel_type': v['vessel_type'], 'mmsi': v['mmsi'],
                     'lat': v['lat'], 'lon': v['lon'], 'reason': v['voi_reason'], 'severity': v['voi_severity'],
                     'timestamp': now_iso(), 'source': 'ONI_MDA_AIS_FEED', 'agency': 'MDA', 'feed': 'voi'}
                a['summary'] = f"{v['vessel_name']} ({v['flag_country']}): {a['reason']}"
                self.mq.publish(f"cop/intel/voi/v1/{aor(v['lat'], v['lon'])}/mda/{lvl(a['alert_id'])}/{lvl(a['severity'])}", a)
                logger.info(f"[MDA maritime/MQTT] VOI: {a['severity']} - {a['summary']}")
            except Exception as e:
                logger.error(f"MDA VOI error: {e}")

    # ---- 3. CENTCOM (REST) ---------------------------------------------------
    def _readiness_rec(self, unit):
        p = random.uniform(0.85, 0.98)
        rec = {**unit, 'readiness': random.choice(['C1', 'C1', 'C2', 'C2', 'C3']), 'personnel_assigned': int(unit['auth'] * p),
               'personnel_available': int(unit['auth'] * p * random.uniform(0.90, 0.98)),
               'training_status': random.choice(['GREEN', 'GREEN', 'AMBER', 'AMBER', 'RED']), 'equipment_or_rate': round(random.uniform(0.80, 0.97), 2),
               'last_assessment': now_iso(), 'source': 'CENTCOM_FORCE_READINESS', 'agency': 'CENTCOM', 'feed': 'readiness'}
        rec['summary'] = f"{unit['name']} {rec['readiness']} / training {rec['training_status']} / OR {rec['equipment_or_rate']:.0%}"
        return rec

    def _init_centcom(self):
        for unit in CENTCOM_UNITS:
            rec = self._readiness_rec(unit)
            self.rest.publish(f"cop/intel/readiness/v1/centcom/centcom/{lvl(unit['unit_id'])}/{lvl(rec['readiness'])}", rec)
        eq_id = 0
        for unit in CENTCOM_UNITS:
            for equip in random.sample(EQUIPMENT_TYPES, k=random.randint(3, 6)):
                eq_id += 1; total = random.randint(10, 80); fmc = int(total * random.uniform(0.70, 0.95))
                rec = {'equipment_id': f"EQ-{eq_id:04d}", 'unit_id': unit['unit_id'], 'unit_name': unit['name'], 'equipment_type': equip['type'],
                       'category': equip['category'], 'total': total, 'fmc': fmc, 'nmc': total - fmc, 'or_rate': round(fmc / total, 2),
                       'last_update': now_iso(), 'source': 'CENTCOM_FORCE_READINESS', 'agency': 'CENTCOM', 'feed': 'equipment'}
                self.centcom_equipment[rec['equipment_id']] = rec
                self._equipment(rec)
        logger.info(f"  Published {len(CENTCOM_UNITS)} units, {eq_id} equipment records over REST")

    def _equipment(self, rec):
        status = 'green' if rec['or_rate'] >= 0.9 else ('amber' if rec['or_rate'] >= 0.75 else 'red')
        rec['summary'] = f"{rec['unit_name']} {rec['equipment_type']} {rec['fmc']}/{rec['total']} FMC ({status.upper()})"
        self.rest.publish(f"cop/intel/equipment/v1/centcom/centcom/{lvl(rec['equipment_id'])}/{status}", rec)

    def _centcom_readiness_loop(self):
        while self.running:
            time.sleep(random.uniform(30, 90))
            try:
                unit = random.choice(CENTCOM_UNITS); rec = self._readiness_rec(unit)
                self.rest.publish(f"cop/intel/readiness/v1/centcom/centcom/{lvl(unit['unit_id'])}/{lvl(rec['readiness'])}", rec)
                logger.info(f"[CENTCOM/REST] Readiness: {rec['summary']}")
            except Exception as e:
                logger.error(f"CENTCOM readiness error: {e}")

    def _centcom_equipment_loop(self):
        while self.running:
            time.sleep(random.uniform(20, 60))
            try:
                rec = random.choice(list(self.centcom_equipment.values()))
                rec['fmc'] = max(1, min(rec['total'], rec['fmc'] + random.randint(-2, 2)))
                rec['nmc'] = rec['total'] - rec['fmc']; rec['or_rate'] = round(rec['fmc'] / rec['total'], 2); rec['last_update'] = now_iso()
                self._equipment(rec)
            except Exception as e:
                logger.error(f"CENTCOM equipment error: {e}")

    # ---- 4. NGA (MQTT) -------------------------------------------------------
    def _nga_change_detection_loop(self):
        while self.running:
            time.sleep(random.uniform(20, 60))
            try:
                area = random.choice(NGA_AREAS); change = random.choice(NGA_CHANGE_TYPES)
                r = {'report_id': f"NGA-CD-{uuid.uuid4().hex[:8].upper()}", 'area_name': area['name'], 'area_type': area['type'],
                     'lat': round(area['lat'] + random.uniform(-0.05, 0.05), 6), 'lon': round(area['lon'] + random.uniform(-0.05, 0.05), 6),
                     'change_type': change, 'confidence': random.choice(['HIGH', 'HIGH', 'MODERATE']),
                     'imagery_source': random.choice(['ELECTRO-OPTICAL', 'SAR', 'INFRARED', 'MULTISPECTRAL']), 'resolution_m': round(random.uniform(0.3, 5.0), 1),
                     'classification': random.choice(['SECRET', 'SECRET//NOFORN', 'TOP SECRET//SI']), 'timestamp': now_iso(),
                     'analyst': f'NGA-ANALYST-{random.randint(100,999)}', 'source': 'NGA_GEOINT_SYSTEM', 'agency': 'NGA', 'feed': 'geoint'}
                r['summary'] = f"{change} at {area['name']}"
                self.mq.publish(f"cop/intel/geoint/v1/{aor(r['lat'], r['lon'])}/nga/{lvl(r['report_id'])}/{lvl(r['confidence'])}", r)
                logger.info(f"[NGA/MQTT] Change: {change[:30]} at {area['name'][:25]}")
            except Exception as e:
                logger.error(f"NGA change detection error: {e}")

    def _nga_restriction_loop(self):
        while self.running:
            time.sleep(random.uniform(60, 180))
            try:
                area = random.choice(NGA_AREAS)
                r = {'restriction_id': f"NGA-R-{uuid.uuid4().hex[:8].upper()}", 'area_name': area['name'],
                     'restriction_type': random.choice(['NO_FLY_ZONE', 'RESTRICTED_AREA', 'DANGER_AREA', 'WARNING_AREA']),
                     'lat': area['lat'], 'lon': area['lon'], 'radius_km': area['radius_km'], 'floor_ft': random.choice([0, 0, 5000, 10000]),
                     'ceiling_ft': random.choice([25000, 40000, 60000, 99999]), 'effective_from': now_iso(),
                     'effective_until': (datetime.now(timezone.utc) + timedelta(hours=random.randint(6, 72))).isoformat(timespec='seconds'),
                     'authority': random.choice(['CENTCOM', 'CAOC', 'Host Nation']), 'timestamp': now_iso(), 'source': 'NGA_GEOINT_SYSTEM', 'agency': 'NGA', 'feed': 'restriction'}
                r['summary'] = f"{r['restriction_type']} {area['name']} ({r['authority']})"
                self.mq.publish(f"cop/intel/restriction/v1/{aor(r['lat'], r['lon'])}/nga/{lvl(r['restriction_id'])}/{lvl(r['restriction_type'])}", r)
                logger.info(f"[NGA/MQTT] Restriction: {r['summary']}")
            except Exception as e:
                logger.error(f"NGA restriction error: {e}")

    # ---- 5. Coalition C2 (SMF) -----------------------------------------------
    def _init_coalition(self):
        """A persistent coalition fleet, so partner tracks move on the map like everything else."""
        self.coalition_air, self.coalition_naval = [], []
        for i in range(8):
            nation = random.choice(COALITION_NATIONS); ac_type = random.choice(COALITION_AIRCRAFT.get(nation['code'], ['Unknown']))
            self.coalition_air.append({'track_id': f"{nation['code']}-AIR-{i+1:02d}", 'nation': nation['code'], 'nation_name': nation['name'], 'alliance': nation['alliance'],
                 'aircraft_type': ac_type, 'callsign': f"{nation['code'][:2]}{random.randint(100,999)}",
                 'lat': round(random.uniform(14.0, 34.0), 6), 'lon': round(random.uniform(32.0, 62.0), 6),
                 'altitude_ft': random.choice([15000, 20000, 25000, 30000, 35000, 40000]), 'heading_deg': round(random.uniform(0, 360), 1),
                 'speed_knots': random.randint(250, 550), 'mission_type': random.choice(['CAP', 'ISR', 'TANKER', 'TRANSPORT', 'STRIKE', 'SEAD', 'AEW']),
                 'iff_mode': random.choice(['MODE_4', 'MODE_5']), 'source': f'COALITION_{nation["code"]}_C2', 'agency': nation['code'], 'feed': 'coalition-air'})
        # Ships steam along open-water routes (SEA_ROUTES), the same ones the MDA AIS feed uses.
        routes = list(SEA_ROUTES)
        for i in range(5):
            nation = random.choice(COALITION_NATIONS); vessel = random.choice(COALITION_NAVAL.get(nation['code'], ['Unknown']))
            self.coalition_naval.append({'track_id': f"{nation['code']}-NAV-{i+1:02d}", 'nation': nation['code'], 'nation_name': nation['name'],
                 'vessel_name': vessel, 'hull_type': vessel.split('(')[-1].rstrip(')') if '(' in vessel else 'FFG',
                 **route_seed(routes[i % len(routes)]), 'heading_deg': round(random.uniform(0, 360), 1),
                 'speed_knots': round(random.uniform(8, 25), 1), 'mission': random.choice(['PATROL', 'ESCORT', 'ASW', 'STRIKE_GROUP', 'MINE_WARFARE', 'AMPHIBIOUS']),
                 'source': f'COALITION_{nation["code"]}_C2', 'agency': nation['code'], 'feed': 'coalition-naval'})
        for t in self.coalition_air: self._coalition_air_pub(t)
        for t in self.coalition_naval: self._coalition_naval_pub(t)
        logger.info(f"  Seeded {len(self.coalition_air)} coalition aircraft and {len(self.coalition_naval)} ships over SMF")

    @staticmethod
    def _advance(t, seconds, lat_lo, lat_hi, lon_lo, lon_hi):
        dist = t['speed_knots'] / 3600.0 * seconds; hr = math.radians(t['heading_deg'])
        t['lat'] = round(max(lat_lo, min(lat_hi, t['lat'] + (dist / 60.0) * math.cos(hr))), 6)
        t['lon'] = round(max(lon_lo, min(lon_hi, t['lon'] + (dist / 60.0) * math.sin(hr) / math.cos(math.radians(t['lat'])))), 6)
        t['heading_deg'] = round((t['heading_deg'] + random.uniform(-4, 4)) % 360, 1)
        t['timestamp'] = now_iso()

    def _coalition_air_pub(self, t):
        self.smf.publish(f"cop/intel/coalition-air/v1/{aor(t['lat'], t['lon'])}/{lvl(t['nation'])}/{lvl(t['track_id'])}/{lvl(t['mission_type'])}", t, guaranteed=False)

    def _coalition_naval_pub(self, t):
        self.smf.publish(f"cop/intel/coalition-naval/v1/{aor(t['lat'], t['lon'])}/{lvl(t['nation'])}/{lvl(t['track_id'])}/{lvl(t['mission'])}", t, guaranteed=False)

    def _coalition_air_loop(self):
        while self.running:
            time.sleep(3)
            try:
                for t in self.coalition_air:
                    self._advance(t, 30, 12.0, 38.0, 28.0, 65.0)
                    self._coalition_air_pub(t)
            except Exception as e:
                logger.error(f"Coalition air error: {e}")

    def _coalition_naval_loop(self):
        while self.running:
            time.sleep(6)
            try:
                for t in self.coalition_naval:
                    follow_route(t, t['speed_knots'] / 3600.0 * 120)   # 2 min of steaming per update
                    t['timestamp'] = now_iso()
                    self._coalition_naval_pub(t)
            except Exception as e:
                logger.error(f"Coalition naval error: {e}")

    def _coalition_intel_loop(self):
        while self.running:
            time.sleep(random.uniform(45, 120))
            try:
                nation = random.choice(COALITION_NATIONS)
                r = {'report_id': f"{nation['code']}-INTEL-{uuid.uuid4().hex[:8].upper()}", 'nation': nation['code'], 'nation_name': nation['name'],
                     'report_type': random.choice(['INTSUM', 'SIGINT_REPORT', 'HUMINT_REPORT', 'IMINT_REPORT', 'SPOT_REPORT']),
                     'priority': random.choice(['FLASH', 'IMMEDIATE', 'PRIORITY', 'ROUTINE']), 'region': random.choice(CENTCOM_REGIONS),
                     'subject': random.choice(['Hostile naval movement detected', 'New SAM site identified', 'Militia activity increase', 'Suspected weapons transfer',
                                               'Communications intercept summary', 'Force buildup assessment', 'Airfield activity change', 'Maritime interdiction result']),
                     'timestamp': now_iso(), 'source': f'COALITION_{nation["code"]}_C2', 'agency': nation['code'], 'feed': 'coalition'}
                r['summary'] = f"{r['report_type']} {r['priority']}: {r['subject']}"
                self.smf.publish(f"cop/intel/coalition/v1/centcom/{lvl(nation['code'])}/{lvl(r['report_id'])}/{lvl(r['priority'])}", r)
                logger.info(f"[COALITION/SMF] {nation['code']} intel: {r['subject'][:40]}")
            except Exception as e:
                logger.error(f"Coalition intel error: {e}")

    # ---- 6. DLA (REST) -------------------------------------------------------
    def _supply(self, rec):
        fill = rec['on_hand'] / rec['max_capacity']
        rec['fill_rate'] = round(fill, 2); rec['days_of_supply'] = round(rec['on_hand'] / max(1, rec['max_capacity'] / 30), 1)
        rec['status'] = 'GREEN' if fill > 0.6 else ('AMBER' if fill > 0.3 else 'RED'); rec['last_update'] = now_iso()
        rec['summary'] = f"{rec['depot_name']} class {rec['supply_class']} ({rec['supply_name']}) {fill:.0%} {rec['status']}"
        self.rest.publish(f"cop/intel/supply/v1/{aor(rec['lat'], rec['lon'])}/dla/{lvl(rec['depot_id'])}-{lvl(rec['supply_class'])}/{lvl(rec['status'])}", rec)

    def _init_dla(self):
        for depot in DLA_DEPOTS:
            for supply in DLA_SUPPLY_CLASSES:
                max_qty = int({'I': 500, 'III': 2000000, 'V': 300, 'VIII': 200, 'IX': 150}.get(supply['cls'], 100) * depot['cap'])
                rec = {'depot_id': depot['depot_id'], 'depot_name': depot['name'], 'lat': depot['lat'], 'lon': depot['lon'],
                       'supply_class': supply['cls'], 'supply_name': supply['name'], 'unit': supply['unit'],
                       'on_hand': int(max_qty * random.uniform(0.40, 0.95)), 'max_capacity': max_qty, 'source': 'DLA_LOGISTICS_SYSTEM', 'agency': 'DLA', 'feed': 'supply'}
                self.dla_levels[(depot['depot_id'], supply['cls'])] = rec
                self._supply(rec)
        logger.info(f"  Published {len(DLA_DEPOTS)} depots x {len(DLA_SUPPLY_CLASSES)} supply classes over REST")

    def _dla_supply_loop(self):
        while self.running:
            time.sleep(random.uniform(15, 45))
            try:
                rec = random.choice(list(self.dla_levels.values()))
                rec['on_hand'] = max(0, min(rec['max_capacity'], rec['on_hand'] + random.randint(-50, 30)))
                self._supply(rec)
            except Exception as e:
                logger.error(f"DLA supply error: {e}")

    def _dla_shipment_loop(self):
        while self.running:
            time.sleep(random.uniform(30, 90))
            try:
                depot = random.choice(DLA_DEPOTS); supply = random.choice(DLA_SUPPLY_CLASSES)
                s = {'shipment_id': f"DLA-{uuid.uuid4().hex[:8].upper()}", 'depot_id': depot['depot_id'], 'depot_name': depot['name'],
                     'supply_class': supply['cls'], 'supply_name': supply['name'], 'quantity': random.randint(10, 500), 'unit': supply['unit'],
                     'origin': random.choice(['CONUS', 'Rota Spain', 'Diego Garcia', 'Sigonella Italy', 'Guam']),
                     'status': random.choice(['IN_TRANSIT', 'IN_TRANSIT', 'DELIVERED', 'CUSTOMS_HOLD', 'LOADING']),
                     'eta': (datetime.now(timezone.utc) + timedelta(days=random.randint(1, 14))).isoformat(timespec='seconds'),
                     'priority': random.choice(['ROUTINE', 'PRIORITY', 'IMMEDIATE']), 'timestamp': now_iso(), 'source': 'DLA_LOGISTICS_SYSTEM', 'agency': 'DLA', 'feed': 'shipment'}
                s['summary'] = f"{s['quantity']} {s['unit']} {supply['name']} → {depot['name']} ({s['status']})"
                self.rest.publish(f"cop/intel/shipment/v1/{aor(depot['lat'], depot['lon'])}/dla/{lvl(s['shipment_id'])}/{lvl(s['status'])}", s)
                logger.info(f"[DLA/REST] Shipment: {s['summary']}")
            except Exception as e:
                logger.error(f"DLA shipment error: {e}")

    # ---- 7. FAA (MQTT) -------------------------------------------------------
    def _adsb(self, ac):
        self.mq.publish(f"cop/air/adsb/v1/{aor(ac['lat'], ac['lon'])}/faa/{lvl(ac['hex_code'])}/{lvl(ac['airline_code'])}", ac, guaranteed=False)

    def _init_faa(self):
        self.faa_aircraft = []
        for i in range(40):
            airline = random.choice(CIVIL_AIRLINES); ac_type = random.choice(CIVIL_AIRCRAFT_TYPES); dep = random.choice(CIVIL_AIRPORTS)
            arr = random.choice([a for a in CIVIL_AIRPORTS if a['icao'] != dep['icao']]); t = random.uniform(0.1, 0.9)
            ac = {'hex_code': f"{random.randint(0xA00000, 0xAFFFFF):06X}", 'flight': f"{airline['code']}{random.randint(100, 999)}",
                  'airline': airline['name'], 'airline_code': airline['code'], 'aircraft_type': ac_type,
                  'lat': round(dep['lat'] + t * (arr['lat'] - dep['lat']) + random.uniform(-1, 1), 6), 'lon': round(dep['lon'] + t * (arr['lon'] - dep['lon']) + random.uniform(-1, 1), 6),
                  'altitude_ft': random.choice([31000, 33000, 35000, 37000, 39000, 41000]), 'heading_deg': round(random.uniform(0, 360), 1),
                  'speed_knots': random.randint(400, 520), 'squawk': f"{random.randint(1000, 7777):04d}",
                  'departure': dep['icao'], 'departure_name': dep['name'], 'arrival': arr['icao'], 'arrival_name': arr['name'],
                  'source': 'FAA_ADSB_FEED', 'agency': 'FAA', 'feed': 'adsb'}
            self.faa_aircraft.append(ac)
            self._adsb(ac)
        logger.info(f"  Seeded {len(self.faa_aircraft)} civil aircraft over MQTT")

    def _faa_adsb_loop(self):
        while self.running:
            time.sleep(random.uniform(2, 6))
            try:
                ac = random.choice(self.faa_aircraft)
                dist = ac['speed_knots'] / 3600.0 * random.uniform(10, 30); hr = math.radians(ac['heading_deg'])
                ac['lat'] = round(max(10.0, min(42.0, ac['lat'] + (dist / 60.0) * math.cos(hr))), 6)
                ac['lon'] = round(max(25.0, min(70.0, ac['lon'] + (dist / 60.0) * math.sin(hr) / math.cos(math.radians(ac['lat'])))), 6)
                ac['heading_deg'] = round((ac['heading_deg'] + random.uniform(-2, 2)) % 360, 1)
                ac['altitude_ft'] = max(5000, min(45000, ac['altitude_ft'] + random.choice([-1000, 0, 0, 0, 1000])))
                ac['last_update'] = now_iso()
                self._adsb(ac)
            except Exception as e:
                logger.error(f"FAA ADS-B error: {e}")

    # ---- startup -------------------------------------------------------------
    def start(self):
        if not self.connect():
            return False
        logger.info("--- 1. DIA Threat Intelligence (REST) ---"); self._init_dia()
        logger.info("--- 2. Maritime Domain Awareness (MQTT) ---"); self._init_mda()
        logger.info("--- 3. CENTCOM Force Readiness (REST) ---"); self._init_centcom()
        logger.info("--- 4. NGA Geospatial Intelligence (MQTT) ---")
        logger.info("--- 5. Coalition Partner C2 (SMF) ---"); self._init_coalition()
        logger.info("--- 6. Defense Logistics Agency (REST) ---"); self._init_dla()
        logger.info("--- 7. FAA Civil Aviation (MQTT) ---"); self._init_faa()
        self.running = True
        threads = [
            ('DIA IOC Feed', self._dia_ioc_loop), ('DIA Threat Reports', self._dia_report_loop), ('DIA Actor Updates', self._dia_actor_update_loop), ('DIA Assessments', self._dia_assessment_loop),
            ('MDA (maritime) Vessel Tracks', self._mda_vessel_loop), ('MDA (maritime) Port Activity', self._mda_port_loop), ('MDA (maritime) VOI Alerts', self._mda_voi_loop),
            ('CENTCOM Readiness', self._centcom_readiness_loop), ('CENTCOM Equipment', self._centcom_equipment_loop),
            ('NGA Change Detection', self._nga_change_detection_loop), ('NGA Restrictions', self._nga_restriction_loop),
            ('Coalition Air Tracks', self._coalition_air_loop), ('Coalition Naval Tracks', self._coalition_naval_loop), ('Coalition Intel', self._coalition_intel_loop),
            ('DLA Supply Levels', self._dla_supply_loop), ('DLA Shipments', self._dla_shipment_loop),
            ('FAA ADS-B Feed', self._faa_adsb_loop),
        ]
        for name, fn in threads:
            threading.Thread(target=fn, daemon=True, name=name).start()
        logger.info(f"✓ {len(threads)} feeds running")
        return True

    def stop(self):
        self.running = False


def main():
    print("\n" + "=" * 60)
    print("COP external agency feeds -> Solace event mesh")
    print("=" * 60)
    print("  1. DIA Threat Intelligence    REST  (HTTP POST to the broker)")
    print("  2. Maritime Domain Awareness  MQTT")
    print("  3. CENTCOM Force Readiness    REST")
    print("  4. NGA Geospatial Intel       MQTT")
    print("  5. Coalition Partner C2       SMF   (native API)")
    print("  6. Defense Logistics Agency   REST")
    print("  7. FAA Civil Aviation         MQTT")
    print("=" * 60 + "\n")
    gen = AgencyFeeds()
    if not gen.start():
        return 1
    try:
        while True:
            time.sleep(30)
            logger.info(f"[STATS] sent rest={gen.rest.sent} mqtt={gen.mq.sent} smf={gen.smf.sent} | MDA {len(gen.mda_vessels)} vessels | FAA {len(gen.faa_aircraft)} aircraft")
    except KeyboardInterrupt:
        gen.stop()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

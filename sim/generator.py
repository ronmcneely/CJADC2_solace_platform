#!/usr/bin/env python3
"""
COP multi-domain simulator: seven source systems publishing to a Solace event mesh.

Air, sea, land, cyber, SIGINT, space and EW records are published to
hierarchical topics (see sim/solace_transport.py and docs/topics.md). Tracks go
out as direct messages; incidents, intercepts, imagery and jamming reports go
out as guaranteed messages. Ground-force tracks also ride MQTT into the broker.
"""

import json
import random
import time
import threading
import uuid
import math
from datetime import datetime
import os

from solace_transport import (SolaceTransport, AIRCRAFT, NAVAL, GPS, CYBER, SIGINT,
                              SATELLITE, SPECTRUM, EMBM)

# Stream keys; the transport maps each to a hierarchical topic + QoS.
AIRCRAFT_TOPIC = AIRCRAFT
NAVAL_TOPIC = NAVAL
GPS_TOPIC = GPS
CYBER_TOPIC = CYBER
SIGINT_TOPIC = SIGINT
SATELLITE_TOPIC = SATELLITE
SPECTRUM_TOPIC = SPECTRUM
EMBM_TOPIC = EMBM

# US SATCOM assets and frequencies
US_SATCOM_SYSTEMS = [
    {'name': 'MUOS-1', 'type': 'UHF_SATCOM', 'freq_ghz': 0.36, 'orbit': 'GEO', 'lon_slot': 75.0, 'users': ['CSG', 'MEU', 'SOCOM']},
    {'name': 'MUOS-4', 'type': 'UHF_SATCOM', 'freq_ghz': 0.38, 'orbit': 'GEO', 'lon_slot': 100.0, 'users': ['PACFLT', 'MEF']},
    {'name': 'WGS-6', 'type': 'SHF_SATCOM', 'freq_ghz': 7.9, 'orbit': 'GEO', 'lon_slot': 65.0, 'users': ['CSG', 'AWACS', 'THAAD']},
    {'name': 'WGS-8', 'type': 'SHF_SATCOM', 'freq_ghz': 8.4, 'orbit': 'GEO', 'lon_slot': 105.0, 'users': ['CENTCOM', 'AFRICOM']},
    {'name': 'AEHF-4', 'type': 'EHF_SATCOM', 'freq_ghz': 44.0, 'orbit': 'GEO', 'lon_slot': 85.0, 'users': ['STRATCOM', 'NC3']},
    {'name': 'AEHF-5', 'type': 'EHF_SATCOM', 'freq_ghz': 44.5, 'orbit': 'GEO', 'lon_slot': 145.0, 'users': ['PACFLT', 'SPACECOM']},
]

# Enemy jammer types
HOSTILE_JAMMERS = [
    {'type': 'TRUCK_JAMMER', 'country': 'RU', 'system': 'Tirada-2S', 'power_kw': 5, 'range_km': 500, 'bands': ['SHF', 'EHF']},
    {'type': 'SHIP_JAMMER', 'country': 'RU', 'system': 'TK-25', 'power_kw': 10, 'range_km': 800, 'bands': ['UHF', 'SHF']},
    {'type': 'FIXED_SITE', 'country': 'CN', 'system': 'BM/KG-8601', 'power_kw': 50, 'range_km': 1500, 'bands': ['UHF', 'SHF', 'EHF']},
    {'type': 'MOBILE_JAMMER', 'country': 'CN', 'system': 'JN-1101', 'power_kw': 8, 'range_km': 600, 'bands': ['SHF']},
    {'type': 'TRUCK_JAMMER', 'country': 'IR', 'system': 'Mohajer-J', 'power_kw': 2, 'range_km': 200, 'bands': ['UHF', 'SHF']},
    {'type': 'NAVAL_EW', 'country': 'KP', 'system': 'Musudan-EW', 'power_kw': 3, 'range_km': 300, 'bands': ['UHF']},
]

# Hostile jammer locations (potential sites)
JAMMER_SITES = [
    {'id': 'JAM-001', 'name': 'PLA Support Base Djibouti EW', 'country': 'CN', 'lat': 11.59, 'lon': 43.06, 'terrain': 'COASTAL'},
    {'id': 'JAM-002', 'name': 'Gwadar Port EW Site', 'country': 'CN', 'lat': 25.12, 'lon': 62.33, 'terrain': 'COASTAL'},
    {'id': 'JAM-003', 'name': 'Tartus Naval EW', 'country': 'RU', 'lat': 34.89, 'lon': 35.87, 'terrain': 'COASTAL'},
    {'id': 'JAM-004', 'name': 'Latakia Mobile Site', 'country': 'RU', 'lat': 35.52, 'lon': 35.78, 'terrain': 'RIDGE'},
    {'id': 'JAM-005', 'name': 'Bandar Abbas EW', 'country': 'IR', 'lat': 27.18, 'lon': 56.28, 'terrain': 'COASTAL'},
    {'id': 'JAM-006', 'name': 'Qeshm Island', 'country': 'IR', 'lat': 26.95, 'lon': 56.27, 'terrain': 'ISLAND'},
    {'id': 'JAM-007', 'name': 'Hodeidah Coastal EW (KP-supplied)', 'country': 'KP', 'lat': 14.80, 'lon': 42.95, 'terrain': 'COASTAL'},
]

# Affected US units for jamming scenarios
AFFECTED_UNITS = [
    {'callsign': 'CSG-5', 'type': 'CARRIER_STRIKE_GROUP', 'region': 'CENTCOM', 'satcom_deps': ['MUOS-4', 'WGS-8']},
    {'callsign': 'MEU-26', 'type': 'MARINE_EXPEDITIONARY_UNIT', 'region': 'CENTCOM', 'satcom_deps': ['MUOS-4']},
    {'callsign': 'CSG-2', 'type': 'CARRIER_STRIKE_GROUP', 'region': 'CENTCOM', 'satcom_deps': ['WGS-6', 'AEHF-4']},
    {'callsign': 'THAAD-1', 'type': 'MISSILE_DEFENSE', 'region': 'CENTCOM', 'satcom_deps': ['WGS-6', 'AEHF-4']},
    {'callsign': 'SOF-TF', 'type': 'SPECIAL_OPS_TASK_FORCE', 'region': 'AFRICOM', 'satcom_deps': ['MUOS-1', 'AEHF-4']},
]

# US Military Facilities - All cyber targets
# US installations, or permanent US presence on a host-nation base. Non-US bases
# (Izmir AS, RAF Akrotiri, Cairo West AB) were removed: cyber targets must be US assets.
US_FACILITIES = [
    {'id': 'FAC-001', 'name': 'Al Udeid AB', 'country': 'Qatar', 'lat': 25.117, 'lon': 51.315, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['CENTCOM HQ Network', 'NIPR Gateway', 'Air Ops Center']},
    {'id': 'FAC-002', 'name': 'Al Dhafra AB', 'country': 'UAE', 'lat': 24.248, 'lon': 54.547, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['Air Defense Network', 'ISR Control', 'Logistics Hub']},
    {'id': 'FAC-003', 'name': 'Camp Arifjan', 'country': 'Kuwait', 'lat': 28.933, 'lon': 48.100, 'type': 'ARMY_BASE', 'region': 'CENTCOM', 'networks': ['SIPR Enclave', 'Army Battle Command', 'Supply Chain DB']},
    {'id': 'FAC-004', 'name': 'NSA Bahrain', 'country': 'Bahrain', 'lat': 26.237, 'lon': 50.652, 'type': 'NAVAL_BASE', 'region': 'CENTCOM', 'networks': ['Fleet Networks', 'Naval Intel Center', 'Maritime Domain Awareness']},
    {'id': 'FAC-005', 'name': 'Prince Sultan AB', 'country': 'Saudi Arabia', 'lat': 24.062, 'lon': 47.580, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['Combined Air Ops', 'Patriot Battery Net', 'CAOC Link']},
    {'id': 'FAC-006', 'name': 'Al Asad AB', 'country': 'Iraq', 'lat': 33.786, 'lon': 42.441, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['Tactical Data Link', 'UAV Control Station', 'Force Protection']},
    {'id': 'FAC-007', 'name': 'Incirlik AB', 'country': 'Turkey', 'lat': 37.002, 'lon': 35.426, 'type': 'AIR_BASE', 'region': 'EUCOM', 'networks': ['NATO Secure Net', 'Nuclear Storage Sys', 'Satellite Uplink']},
    {'id': 'FAC-009', 'name': 'Muwaffaq Salti AB', 'country': 'Jordan', 'lat': 32.161, 'lon': 36.259, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['P-8 Mission Systems', 'Med Surveillance', 'Logistics Database']},
    {'id': 'FAC-010', 'name': 'Ali Al Salem AB', 'country': 'Kuwait', 'lat': 29.347, 'lon': 47.521, 'type': 'AIR_BASE', 'region': 'CENTCOM', 'networks': ['Sub Support Net', 'Ammo Storage Sys', 'Port Control']},
    {'id': 'FAC-012', 'name': 'Camp Lemonnier', 'country': 'Djibouti', 'lat': 11.547, 'lon': 43.145, 'type': 'EXPEDITIONARY', 'region': 'AFRICOM', 'networks': ['AFRICOM Fusion Center', 'SOF Ops Network', 'Counter-Terror DB']},
    {'id': 'FAC-013', 'name': 'Chabelley Airfield', 'country': 'Djibouti', 'lat': 11.517, 'lon': 42.847, 'type': 'UAV_BASE', 'region': 'AFRICOM', 'networks': ['Drone Control System', 'ISR Downlink', 'Strike Coordination']},
]

# Aircraft definitions
US_AIRCRAFT = [
    {'type': 'F-22A Raptor', 'role': 'AIR_SUPERIORITY', 'max_speed': 1500, 'weapons': 'AIM-120D x6, AIM-9X x2'},
    {'type': 'F-35A Lightning II', 'role': 'STEALTH', 'max_speed': 1200, 'weapons': 'AIM-120D x4, GBU-31 x2'},
    {'type': 'F-15E Strike Eagle', 'role': 'STRIKE', 'max_speed': 1600, 'weapons': 'AIM-120C x4, GBU-39 x8'},
    {'type': 'F-16C Fighting Falcon', 'role': 'MULTIROLE', 'max_speed': 1300, 'weapons': 'AIM-120 x4, AIM-9X x2'},
    {'type': 'MQ-9 Reaper', 'role': 'ISR_UAV', 'max_speed': 300, 'weapons': 'AGM-114 Hellfire x4, GBU-12 x2'},
    {'type': 'E-3 Sentry AWACS', 'role': 'AWACS', 'max_speed': 500, 'weapons': 'None - Radar/C2'},
    {'type': 'KC-135 Stratotanker', 'role': 'TANKER', 'max_speed': 500, 'weapons': 'None - Refueling'},
]

RUSSIAN_AIRCRAFT = [
    {'type': 'Su-35S Flanker-E', 'role': 'AIR_SUPERIORITY', 'max_speed': 1500, 'weapons': 'R-77-1 x6, R-73M x4'},
    {'type': 'Su-34 Fullback', 'role': 'STRIKE', 'max_speed': 1200, 'weapons': 'Kh-59MK x4, KAB-1500 x3'},
    {'type': 'Su-57 Felon', 'role': 'STEALTH', 'max_speed': 1600, 'weapons': 'R-77M x4, Kh-59MK2 x4'},
    {'type': 'MiG-31BM Foxhound', 'role': 'INTERCEPTOR', 'max_speed': 1900, 'weapons': 'R-37M x4, R-77 x2'},
    {'type': 'Tu-22M3 Backfire', 'role': 'BOMBER', 'max_speed': 1400, 'weapons': 'Kh-22 x3'},
]

CHINESE_AIRCRAFT = [
    {'type': 'J-20 Mighty Dragon', 'role': 'STEALTH', 'max_speed': 1300, 'weapons': 'PL-15 x4, PL-10 x2'},
    {'type': 'J-16', 'role': 'MULTIROLE', 'max_speed': 1500, 'weapons': 'PL-15 x6, YJ-83 x4'},
    {'type': 'H-6K', 'role': 'BOMBER', 'max_speed': 650, 'weapons': 'CJ-10A x6, YJ-12 x4'},
]

IRANIAN_AIRCRAFT = [
    {'type': 'F-14A Tomcat', 'role': 'AIR_SUPERIORITY', 'max_speed': 1500, 'weapons': 'AIM-54 Phoenix x4, AIM-9 x2'},
    {'type': 'MiG-29A Fulcrum', 'role': 'MULTIROLE', 'max_speed': 1500, 'weapons': 'R-27 x2, R-73 x4'},
    {'type': 'Su-24MK Fencer', 'role': 'STRIKE', 'max_speed': 1100, 'weapons': 'Kh-29 x4, FAB-500 x6'},
    {'type': 'Shahed-136', 'role': 'ATTACK_UAV', 'max_speed': 115, 'weapons': 'Warhead 40kg'},
    {'type': 'F-4E Phantom II', 'role': 'STRIKE', 'max_speed': 1400, 'weapons': 'AGM-65 x4, Mk-82 x6'},
]

# Naval vessels
US_NAVAL = [
    {'type': 'Nimitz-class CVN', 'class': 'CARRIER', 'displacement': 100000, 'weapons': 'RIM-7, RIM-116, Phalanx x3', 'aircraft': 90},
    {'type': 'Gerald R. Ford CVN', 'class': 'CARRIER', 'displacement': 100000, 'weapons': 'RIM-162, RIM-116, Phalanx x3', 'aircraft': 75},
    {'type': 'Arleigh Burke DDG', 'class': 'DESTROYER', 'displacement': 9700, 'weapons': 'Mk-41 VLS x96, Harpoon x8'},
    {'type': 'Ticonderoga CG', 'class': 'CRUISER', 'displacement': 9800, 'weapons': 'Mk-41 VLS x122, Harpoon x8'},
    {'type': 'Virginia SSN', 'class': 'SUBMARINE', 'displacement': 7900, 'weapons': 'Mk-48 Torpedo x26, TLAM x12'},
]

RUSSIAN_NAVAL = [
    {'type': 'Admiral Kuznetsov CV', 'class': 'CARRIER', 'displacement': 58500, 'weapons': 'P-700 Granit x12', 'aircraft': 40},
    {'type': 'Slava-class CG', 'class': 'CRUISER', 'displacement': 12000, 'weapons': 'P-500 Bazalt x16, S-300F'},
    {'type': 'Kilo-class SSK', 'class': 'SUBMARINE', 'displacement': 3100, 'weapons': 'Torpedoes x18, Kalibr x4'},
]

CHINESE_NAVAL = [
    {'type': 'Shandong CV', 'class': 'CARRIER', 'displacement': 70000, 'weapons': 'HQ-10, Type 1130 CIWS', 'aircraft': 44},
    {'type': 'Type 055 DDG', 'class': 'DESTROYER', 'displacement': 13000, 'weapons': 'VLS x112'},
    {'type': 'Type 039A SSK', 'class': 'SUBMARINE', 'displacement': 3600, 'weapons': 'Torpedoes x18, YJ-82 x6'},
]

IRANIAN_NAVAL = [
    {'type': 'Alvand-class FFG', 'class': 'FRIGATE', 'displacement': 1350, 'weapons': 'C-802 x4, 76mm gun'},
    {'type': 'Moudge-class FFG', 'class': 'FRIGATE', 'displacement': 1500, 'weapons': 'Noor AShM x4'},
    {'type': 'Ghadir-class SSK', 'class': 'SUBMARINE', 'displacement': 120, 'weapons': 'Torpedoes x2'},
    {'type': 'Sina-class FAC', 'class': 'MISSILE_BOAT', 'displacement': 310, 'weapons': 'C-802 x4'},
]

# Cyber threat actors
CYBER_ACTORS = [
    {'name': 'APT28 (Fancy Bear)', 'country': 'RU', 'sophistication': 'ADVANCED', 'ttps': ['T1566', 'T1190', 'T1078']},
    {'name': 'APT29 (Cozy Bear)', 'country': 'RU', 'sophistication': 'ADVANCED', 'ttps': ['T1195', 'T1027', 'T1071']},
    {'name': 'Sandworm Team', 'country': 'RU', 'sophistication': 'ADVANCED', 'ttps': ['T1059', 'T1486', 'T1561']},
    {'name': 'APT41 (Double Dragon)', 'country': 'CN', 'sophistication': 'ADVANCED', 'ttps': ['T1190', 'T1055', 'T1021']},
    {'name': 'APT40 (Leviathan)', 'country': 'CN', 'sophistication': 'ADVANCED', 'ttps': ['T1566', 'T1203', 'T1048']},
    {'name': 'Lazarus Group', 'country': 'KP', 'sophistication': 'ADVANCED', 'ttps': ['T1566', 'T1059', 'T1486']},
    {'name': 'MuddyWater', 'country': 'IR', 'sophistication': 'MODERATE', 'ttps': ['T1566', 'T1059', 'T1105']},
    {'name': 'OilRig (APT34)', 'country': 'IR', 'sophistication': 'MODERATE', 'ttps': ['T1566', 'T1078', 'T1048']},
    {'name': 'Charming Kitten', 'country': 'IR', 'sophistication': 'ADVANCED', 'ttps': ['T1566', 'T1528', 'T1114']},
]

CYBER_ATTACKS = [
    {'type': 'NETWORK_INTRUSION', 'severity': 'CRITICAL', 'desc': 'Unauthorized network access detected'},
    {'type': 'MALWARE_DETECTED', 'severity': 'CRITICAL', 'desc': 'APT malware identified in system'},
    {'type': 'DATA_EXFILTRATION', 'severity': 'CRITICAL', 'desc': 'Sensitive data transfer to external IP'},
    {'type': 'C2_COMMUNICATION', 'severity': 'CRITICAL', 'desc': 'Command and control beacon detected'},
    {'type': 'RANSOMWARE_ATTEMPT', 'severity': 'CRITICAL', 'desc': 'Ransomware deployment blocked'},
    {'type': 'ZERO_DAY_EXPLOIT', 'severity': 'CRITICAL', 'desc': 'Unknown vulnerability exploited'},
    {'type': 'PHISHING_CAMPAIGN', 'severity': 'HIGH', 'desc': 'Targeted spearphishing detected'},
    {'type': 'CREDENTIAL_THEFT', 'severity': 'HIGH', 'desc': 'Credential harvesting attempt'},
    {'type': 'LATERAL_MOVEMENT', 'severity': 'HIGH', 'desc': 'Attacker moving laterally in network'},
    {'type': 'DNS_TUNNELING', 'severity': 'HIGH', 'desc': 'Data exfiltration via DNS queries'},
]

# Ground units with home bases
# IPs that also appear in DIA's IOC feed (external-systems generator keeps the same list),
# so the alerting subscriber's IOC matching has something real to hit.
KNOWN_BAD_IPS = ['185.220.101.34', '91.108.4.17', '45.155.205.233', '103.75.190.12', '194.26.29.156', '185.156.73.91', '91.219.236.222', '45.9.148.114', '103.253.41.98', '194.165.16.11', '185.234.72.19', '91.240.118.172']

# Coalition partner assets a hostile launch site can aim at. Attacks on these run the coalition
# workflow: the partner's own MOD / joint operations centre is notified first, a US operator decides
# whether US forces engage on the partner's behalf, and the partner is told when they do.
COALITION_FACILITIES = [
    {'id': 'COAL-GBR-AKROTIRI', 'name': 'RAF Akrotiri', 'country': 'GBR', 'host': 'CYP', 'lat': 34.590, 'lon': 32.988, 'type': 'AIR_BASE', 'defended_by': 'Sky Sabre (RAF Regiment)'},
    {'id': 'COAL-GBR-JUFAIR', 'name': 'UK Naval Support Facility Bahrain (HMS Jufair)', 'country': 'GBR', 'host': 'BHR', 'lat': 26.214, 'lon': 50.610, 'type': 'NAVAL_BASE', 'defended_by': 'Sea Ceptor (Type 23 alongside)'},
    {'id': 'COAL-FRA-ABUDHABI', 'name': 'French Forces UAE (Camp de la Paix)', 'country': 'FRA', 'host': 'ARE', 'lat': 24.433, 'lon': 54.457, 'type': 'NAVAL_BASE', 'defended_by': 'Crotale NG / SAMP-T'},
    {'id': 'COAL-AUS-MINHAD', 'name': 'Al Minhad AB (Australian detachment)', 'country': 'AUS', 'host': 'ARE', 'lat': 25.027, 'lon': 55.366, 'type': 'AIR_BASE', 'defended_by': 'UAE Patriot / THAAD'},
    {'id': 'COAL-JPN-DJIBOUTI', 'name': 'JSDF Base Djibouti', 'country': 'JPN', 'host': 'DJI', 'lat': 11.553, 'lon': 43.160, 'type': 'EXPEDITIONARY', 'defended_by': 'JGSDF point defence; US Lemonnier C-UAS'},
    {'id': 'COAL-SAU-JIZAN', 'name': 'King Abdullah AB Jizan', 'country': 'SAU', 'host': 'SAU', 'lat': 16.901, 'lon': 42.586, 'type': 'AIR_BASE', 'defended_by': 'RSADF Patriot PAC-3'},
    {'id': 'COAL-SAU-KKMC', 'name': 'King Khalid Military City', 'country': 'SAU', 'host': 'SAU', 'lat': 27.901, 'lon': 45.528, 'type': 'ARMY_BASE', 'defended_by': 'RSADF Patriot / THAAD'},
    {'id': 'COAL-ARE-FUJAIRAH', 'name': 'Fujairah Naval Base', 'country': 'ARE', 'host': 'ARE', 'lat': 25.153, 'lon': 56.360, 'type': 'NAVAL_BASE', 'defended_by': 'UAE Patriot / Pantsir'},
]

GROUND_UNITS = [
    {'id': 'UNIT-1AD', 'name': '1st Armored Division', 'type': 'ARMOR', 'base': 'FAC-003', 'personnel': 4500, 'equipment': ['M1A2 Abrams x58', 'M2 Bradley x120']},
    {'id': 'UNIT-5ID', 'name': '5th Infantry Division', 'type': 'INFANTRY', 'base': 'FAC-006', 'personnel': 3200, 'equipment': ['MRAP x80', 'Stryker x45']},
    {'id': 'UNIT-82ABN', 'name': '82nd Airborne Division', 'type': 'AVIATION', 'base': 'FAC-001', 'personnel': 2800, 'equipment': ['AH-64E x24', 'UH-60M x36']},
    {'id': 'UNIT-3MEF', 'name': '3rd Marine Expeditionary Force', 'type': 'MARINES', 'base': 'FAC-012', 'personnel': 2200, 'equipment': ['LAV-25 x28', 'AAV-7 x12']},
    {'id': 'UNIT-SOCCENT', 'name': 'Special Operations Command Central', 'type': 'SOF', 'base': 'FAC-012', 'personnel': 650, 'equipment': ['GMV x24', 'MH-6 x8']},
    {'id': 'UNIT-101ABN', 'name': '101st Airborne (Air Assault)', 'type': 'AIR_ASSAULT', 'base': 'FAC-002', 'personnel': 3800, 'equipment': ['UH-60 x48', 'CH-47 x16']},
]


class EntityState:
    """Base class for tracking entity state"""
    def __init__(self, entity_id, entity_type, lat, lon, country='US'):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.lat = lat
        self.lon = lon
        self.country = country
        self.heading = random.uniform(0, 360)
        self.speed = 0
        self.altitude = 0

    def get_gps_record(self):
        """Generate GPS record for unified tracking"""
        return {
            'unit_id': self.entity_id,
            'unit_name': getattr(self, 'name', self.entity_id),
            'unit_type': self.entity_type,
            'timestamp': int(time.time() * 1000),
            'lat': round(self.lat, 6),
            'lon': round(self.lon, 6),
            'heading_deg': round(self.heading, 1),
            'speed_kph': round(self.speed * 1.852, 1),  # knots to kph
            'altitude_m': round(self.altitude * 0.3048, 1),  # ft to m
            'fuel_level_pct': getattr(self, 'fuel', 80.0),
            'ammo_level_pct': getattr(self, 'ammo', 85.0),
            'personnel_count': getattr(self, 'personnel', 0),
            'equipment_status': 'FULLY_OPERATIONAL',
            'mission_status': getattr(self, 'status', 'STANDBY'),
            'fuel_consumption_rate': 2.5,
            'comms_status': 'NOMINAL',
        }


class AircraftState(EntityState):
    def __init__(self, config, country, tail, base_lat, base_lon):
        super().__init__(tail, f"AIRCRAFT_{config['role']}", base_lat, base_lon, country)
        self.name = f"{config['type']} ({tail})"
        self.type = config['type']
        self.role = config['role']
        self.max_speed = config['max_speed']
        self.weapons = config.get('weapons', 'Unknown')
        self.tail = tail
        self.lat = base_lat + random.uniform(-2, 2)
        self.lon = base_lon + random.uniform(-1.2, 1.2)
        self.speed = random.uniform(250, self.max_speed * 0.5)
        self.altitude = random.uniform(8000, 42000) if 'UAV' not in self.role else random.uniform(3000, 15000)
        self.status = random.choice(['PATROL', 'PATROL', 'CAP', 'TRANSIT'])
        self.fuel = random.uniform(50, 95)
        self.iff = 'FRIENDLY' if country == 'US' else 'HOSTILE'
        # Patrol box (degrees) around the spawn point. Lets aircraft fly at a
        # visible speed without leaving the theater: when they cross the edge
        # they turn back toward home, so they orbit their operating area.
        self.home_lat, self.home_lon = self.lat, self.lon
        self.patrol_radius = 1.6 if country == 'US' else 0.9     # hostile aircraft stay in their own operating areas; a probe toward a US asset is an event

    def update(self, dt):
        self.heading = (self.heading + random.uniform(-3, 3)) % 360
        d_lat, d_lon = self.home_lat - self.lat, self.home_lon - self.lon
        if math.hypot(d_lat, d_lon) > self.patrol_radius:
            self.heading = (math.degrees(math.atan2(d_lon, d_lat)) + random.uniform(-25, 25)) % 360
        # Movement factor: 1.0 = true ground speed. Was 0.08 (a 12x slowdown),
        # which made aircraft look frozen at map scale. 1.5 keeps motion clearly
        # visible when zoomed to an area of interest.
        speed_deg = (self.speed / 3600) / 60 * 7.5   # ~7.5x true speed so motion reads at map scale
        self.lat += speed_deg * math.cos(math.radians(self.heading)) * dt
        self.lon += speed_deg * math.sin(math.radians(self.heading)) * dt
        # Fuel burns on station; below 20% the aircraft goes RTB, tanks, and
        # comes back. Low fuel is therefore an event, not a permanent state.
        if self.status == 'RTB':
            self.fuel = min(95, self.fuel + 0.6 * dt)
            if self.fuel >= 90:
                self.status = random.choice(['PATROL', 'CAP', 'TRANSIT'])
        else:
            self.fuel = max(12, self.fuel - 0.008 * dt)
            if self.fuel < 20:
                self.status = 'RTB'
            elif random.random() < (0.003 if self.country == 'US' else 0.0008):
                # a hostile only goes to INTERCEPT / STRIKE when the exercise vectors it (probe); on its own it patrols
                self.status = random.choice(['PATROL', 'PATROL', 'CAP', 'INTERCEPT', 'STRIKE', 'RTB'] if self.country == 'US' else ['PATROL', 'PATROL', 'CAP', 'TRANSIT', 'RTB'])
        return {
            'aircraft_id': self.tail, 'tail_number': self.tail, 'aircraft_type': self.type,
            'role': self.role, 'country': self.country, 'weapons': self.weapons,
            'timestamp': int(time.time() * 1000),
            'lat': round(self.lat, 6), 'lon': round(self.lon, 6),
            'altitude_ft': round(self.altitude, 0), 'heading_deg': round(self.heading, 1),
            'speed_kts': round(self.speed * 0.54, 1), 'status': self.status,
            'fuel_pct': round(self.fuel, 1), 'iff_mode': self.iff,
        }


class NavalState(EntityState):
    def __init__(self, config, country, hull, lat, lon, patrol_radius=0.30):
        super().__init__(hull, f"NAVAL_{config['class']}", lat, lon, country)
        self.name = f"{config['type']} ({hull})"
        self.type = config['type']
        self.ship_class = config['class']
        self.displacement = config['displacement']
        self.weapons = config.get('weapons', 'Unknown')
        self.aircraft_cap = config.get('aircraft', 0)
        self.hull = hull
        self.speed = random.uniform(10, 22) if 'PATROL' not in self.ship_class else random.uniform(25, 40)
        self.status = random.choice(['PATROL', 'TRANSIT', 'STATION'])
        self.iff = 'FRIENDLY' if country == 'US' else 'HOSTILE'
        self.personnel = config['displacement'] // 10
        # Patrol box: ships stay within this radius (degrees) of their spawn point.
        # Spawn points are in open water, so this keeps them off the coastline
        # instead of random-walking ashore over a long-running demo.
        self.home_lat, self.home_lon = lat, lon
        self.patrol_radius = patrol_radius   # degrees; sized per spawn to the water it sits in

    def update(self, dt):
        self.heading = (self.heading + random.uniform(-1.5, 1.5)) % 360
        # If drifting past the patrol box, turn back toward home
        d_lat, d_lon = self.home_lat - self.lat, self.home_lon - self.lon
        if math.hypot(d_lat, d_lon) > self.patrol_radius:
            self.heading = (math.degrees(math.atan2(d_lon, d_lat)) + random.uniform(-20, 20)) % 360
        speed_deg = (self.speed * 1.852 / 3600) / 60 * 2.0   # 2x true speed (was 0.05, a 20x slowdown)
        self.lat += speed_deg * math.cos(math.radians(self.heading)) * dt
        self.lon += speed_deg * math.sin(math.radians(self.heading)) * dt
        return {
            'vessel_id': self.hull, 'hull_number': self.hull, 'vessel_type': self.type,
            'vessel_class': self.ship_class, 'displacement_tons': self.displacement,
            'country': self.country, 'weapons': self.weapons, 'aircraft_capacity': self.aircraft_cap,
            'timestamp': int(time.time() * 1000),
            'lat': round(self.lat, 6), 'lon': round(self.lon, 6),
            'heading_deg': round(self.heading, 1), 'speed_kts': round(self.speed, 1),
            'status': self.status, 'iff_mode': self.iff,
        }


class GroundUnitState(EntityState):
    def __init__(self, config, facility):
        super().__init__(config['id'], config['type'], facility['lat'], facility['lon'], 'US')
        self.name = config['name']
        self.unit_type = config['type']
        self.personnel = config['personnel']
        self.equipment = config['equipment']
        self.base_facility = facility
        self.lat = facility['lat'] + random.uniform(-0.1, 0.1)
        self.lon = facility['lon'] + random.uniform(-0.1, 0.1)
        self.fuel = random.uniform(60, 95)
        self.ammo = random.uniform(65, 95)
        self.status = random.choice(['STANDBY', 'PATROL', 'TRAINING', 'ALERT'])

    def update(self, dt):
        # Small movements around base
        self.lat += random.uniform(-0.005, 0.005)
        self.lon += random.uniform(-0.005, 0.005)
        self.fuel = max(15, min(100, self.fuel + random.uniform(-0.12, 0.08)))
        self.ammo = max(25, min(100, self.ammo + random.uniform(-0.05, 0.03)))
        # Resupply convoy: a unit that has run low gets topped up now and then.
        if (self.fuel < 30 or self.ammo < 40) and random.random() < 0.03:
            self.fuel, self.ammo = random.uniform(85, 98), random.uniform(85, 98)
        if random.random() < 0.01:
            self.status = random.choice(['STANDBY', 'PATROL', 'TRAINING', 'ALERT', 'DEPLOYED'])
        return self.get_gps_record()


class Generator:
    def __init__(self):
        self.producer = None
        self.aircraft = []
        self.naval = []
        self.ground_units = []
        self.running = False
        self._init_forces()

    def _init_forces(self):
        # US Aircraft from various bases
        us_air_bases = [f for f in US_FACILITIES if 'AIR' in f['type'] or f['type'] == 'UAV_BASE']
        for base in us_air_bases:
            for _ in range(random.randint(1, 3)):
                ac = random.choice(US_AIRCRAFT)
                self.aircraft.append(AircraftState(ac, 'US', f"{random.randint(70,99)}-{random.randint(1000,9999)}", base['lat'], base['lon']))
        
        # Russian Aircraft - Syria (a detachment, not an air army)
        ru_syria = [{'lat': 35.401, 'lon': 35.949}, {'lat': 34.889, 'lon': 35.887}]
        for _ in range(4):
            ac = random.choice(RUSSIAN_AIRCRAFT)
            base = random.choice(ru_syria)
            self.aircraft.append(AircraftState(ac, 'RU', f"RF-{random.randint(81000,95999)}", base['lat'], base['lon']))
        
        # Russian Aircraft - wider Syria deployment
        ru_syria_fwd = [
            {'lat': 34.52, 'lon': 37.63},   # Tiyas (T4)
            {'lat': 35.28, 'lon': 40.16},   # Deir ez-Zor
            {'lat': 37.02, 'lon': 41.19},   # Qamishli
        ]
        for base in ru_syria_fwd:
            for _ in range(1):
                ac = random.choice(RUSSIAN_AIRCRAFT)
                self.aircraft.append(AircraftState(ac, 'RU', f"RF-{random.randint(81000,95999)}", base['lat'], base['lon']))
        
        # Chinese Aircraft - Indian Ocean patrol
        cn_patrol = [(15.0, 55.0), (12.0, 52.0)]
        for pos in cn_patrol:
            for _ in range(1):
                ac = random.choice(CHINESE_AIRCRAFT)
                self.aircraft.append(AircraftState(ac, 'CN', f"{random.randint(10000,99999)}", pos[0], pos[1]))
        
        # Iranian Aircraft
        ir_bases = [{'lat': 35.689, 'lon': 51.311}, {'lat': 32.751, 'lon': 51.861}, {'lat': 29.540, 'lon': 52.589}, {'lat': 27.218, 'lon': 56.378}]
        for base in ir_bases:
            for _ in range(random.randint(1, 2)):
                ac = random.choice(IRANIAN_AIRCRAFT)
                self.aircraft.append(AircraftState(ac, 'IR', f"IR-{random.randint(1000,9999)}", base['lat'], base['lon']))
        
        # US Naval - Persian Gulf CSG
        self.naval.append(NavalState(US_NAVAL[0], 'US', 'CVN-72', 25.8, 54.5))
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-89', 25.5, 54.8, 0.25))  # moved: hugging Jebel Ali -> further offshore
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-91', 26.1, 54.0, 0.22))
        self.naval.append(NavalState(US_NAVAL[3], 'US', 'CG-62', 25.6, 54.9))  # moved: was on the Sharjah coast -> open Gulf
        self.naval.append(NavalState(US_NAVAL[4], 'US', 'SSN-784', 24.6, 58.6))  # moved: was inland Oman -> Gulf of Oman off Muscat
        # Red Sea CSG
        self.naval.append(NavalState(US_NAVAL[1], 'US', 'CVN-78', 14.5, 42.5))
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-105', 14.0, 42.6, 0.18))
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-108', 15.0, 42.2))
        # Med
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-112', 34.2, 33.0))   # south of Cyprus
        self.naval.append(NavalState(US_NAVAL[3], 'US', 'CG-70', 33.8, 34.2, 0.18))
        # Northern Red Sea
        self.naval.append(NavalState(US_NAVAL[2], 'US', 'DDG-119', 22.5, 38.0, 0.15))  # moved: was downtown Istanbul -> Black Sea off the Bosphorus
        
        # Russian Naval - Eastern Med
        self.naval.append(NavalState(RUSSIAN_NAVAL[0], 'RU', '063', 35.2, 34.9, 0.20))   # off Tartus
        self.naval.append(NavalState(RUSSIAN_NAVAL[2], 'RU', 'B-261', 34.9, 34.6, 0.20))
        # Russian Naval - Port Sudan
        self.naval.append(NavalState(RUSSIAN_NAVAL[1], 'RU', '810', 19.3, 37.9, 0.15))  # Port Sudan
        
        # Chinese Naval - Indian Ocean
        self.naval.append(NavalState(CHINESE_NAVAL[1], 'CN', '101', 11.9, 51.9, 0.18))   # Gulf of Aden escort task group
        self.naval.append(NavalState(CHINESE_NAVAL[2], 'CN', '332', 10.5, 53.0))
        
        # Iranian Naval - Strait of Hormuz
        self.naval.append(NavalState(IRANIAN_NAVAL[0], 'IR', 'F71', 26.58, 56.40, 0.10))  # moved: was on Larak Island -> strait water south of it
        self.naval.append(NavalState(IRANIAN_NAVAL[1], 'IR', 'F77', 26.40, 56.10, 0.10))  # moved: was Qeshm Island shoreline -> open Gulf south of Qeshm
        self.naval.append(NavalState(IRANIAN_NAVAL[2], 'IR', 'IS901', 26.50, 56.55, 0.15))
        self.naval.append(NavalState(IRANIAN_NAVAL[3], 'IR', 'P221', 26.45, 56.70, 0.12))  # moved: was Bandar Abbas city -> open strait
        
        # Ground units
        for unit_cfg in GROUND_UNITS:
            facility = next((f for f in US_FACILITIES if f['id'] == unit_cfg['base']), US_FACILITIES[0])
            self.ground_units.append(GroundUnitState(unit_cfg, facility))

    def connect(self):
        self.producer = SolaceTransport()
        return self.producer.connect()

    def _produce(self, stream, key, value):
        # The transport derives the hierarchical topic from the record itself and
        # picks direct vs guaranteed delivery per stream. Ground tracks also go
        # out over MQTT inside the transport.
        self.producer.publish(stream, value)

    def _air_loop(self):
        """Aircraft updates"""
        last = time.time()
        while self.running:
            dt = time.time() - last
            last = time.time()
            for ac in self.aircraft:
                ac_data = ac.update(dt)
                self._produce(AIRCRAFT_TOPIC, ac.tail, ac_data)
                # No duplicate "unified" feed needed: cop/*/track/v1/> covers every domain.
            time.sleep(2)

    def _naval_loop(self):
        """Naval updates"""
        last = time.time()
        while self.running:
            dt = time.time() - last
            last = time.time()
            for ship in self.naval:
                ship_data = ship.update(dt)
                self._produce(NAVAL_TOPIC, ship.hull, ship_data)
            time.sleep(3)

    def _ground_loop(self):
        """Ground unit updates - primary GPS emitter"""
        last = time.time()
        while self.running:
            dt = time.time() - last
            last = time.time()
            for unit in self.ground_units:
                gps_data = unit.update(dt)
                self._produce(GPS_TOPIC, unit.entity_id, gps_data)
            time.sleep(4)

    def _cyber_loop(self):
        """Cyber threats - tied to US facilities"""
        while self.running:
            time.sleep(random.uniform(150, 360))      # a cyber incident every few minutes, not every 20 s
            
            # Target is ALWAYS a US asset: a facility, or (~35% of the time) a live
            # US ship or aircraft at its current position. Both use the same target_*
            # fields so every consumer sees one incident shape.
            us_assets = [a for a in self.aircraft if a.country == 'US'] + [s for s in self.naval if s.country == 'US']
            if us_assets and random.random() < 0.35:
                asset = random.choice(us_assets)
                if isinstance(asset, AircraftState):
                    t_id, t_name, t_type = asset.tail, f"{asset.type} ({asset.tail})", 'AIRCRAFT'
                    network = random.choice(['Mission Computer', 'Link 16 Terminal', 'Nav / GPS Receiver', 'Datalink Gateway', 'EW Suite'])
                else:
                    t_id, t_name, t_type = asset.hull, f"{asset.type} ({asset.hull})", 'VESSEL'
                    network = random.choice(['Combat System Network', 'SATCOM Terminal', 'Nav / Bridge Systems', 'Engineering Control', 'Fleet Data Link'])
                target = {'id': t_id, 'name': t_name, 'type': t_type, 'region': 'MOBILE_ASSET',
                          'country': 'US', 'lat': round(asset.lat, 6), 'lon': round(asset.lon, 6)}
            else:
                facility = random.choice(US_FACILITIES)
                network = random.choice(facility['networks'])
                target = {'id': facility['id'], 'name': facility['name'], 'type': facility['type'],
                          'region': facility['region'], 'country': facility['country'],
                          'lat': facility['lat'], 'lon': facility['lon']}
            
            actor = random.choice(CYBER_ACTORS)
            attack = random.choice(CYBER_ATTACKS)
            
            threat = {
                'incident_id': f"CYBER-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(time.time() * 1000),
                'threat_actor': actor['name'],
                'actor_country': actor['country'],
                'sophistication': actor['sophistication'],
                'attack_type': attack['type'],
                'severity': attack['severity'],
                'description': attack['desc'],
                'target_facility_id': target['id'],
                'target_facility': target['name'],
                'target_system': network,
                'target_type': target['type'],
                'target_region': target['region'],
                'target_country': target['country'],
                'target_lat': target['lat'],
                'target_lon': target['lon'],
                # Attack details
                'source_ip': random.choice(KNOWN_BAD_IPS) if random.random() < 0.4 else f"{random.choice([185,91,45,103,194])}.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}",
                'destination_ip': f"10.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}",
                'protocol': random.choice(['TCP', 'HTTPS', 'DNS', 'SSH', 'RDP']),
                'port': random.choice([22, 443, 53, 3389, 445, 8080]),
                'mitre_attack': random.choice(actor['ttps']),
                'status': random.choice(['DETECTED', 'BLOCKED', 'INVESTIGATING', 'CONTAINED']),
            }
            self._produce(CYBER_TOPIC, threat['incident_id'], threat)
            print(f"[CYBER] {threat['severity']}: {threat['attack_type']} → {target['name']} [{target['type']}] ({network})")

    def _sigint_loop(self):
        """Traditional SIGINT emissions from hostile forces"""
        while self.running:
            time.sleep(random.uniform(300, 600))  # Every 5-10 minutes
            
            # Pick a hostile aircraft or ship
            hostiles = [a for a in self.aircraft if a.country != 'US'] + [s for s in self.naval if s.country != 'US']
            if not hostiles:
                continue
            
            emitter = random.choice(hostiles)
            signal_type = random.choice(['RADAR', 'COMMS', 'DATALINK', 'IFF', 'JAMMER'])
            threat_level = random.choices(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'], weights=[50, 35, 12, 3])[0]
            
            actions_by_type = {
                'RADAR': {
                    'CRITICAL': ['Immediate evasive maneuvers', 'Deploy EA-18G for SEAD', 'Request ARM strike package'],
                    'HIGH': ['Reroute aircraft around threat ring', 'Alert airborne assets', 'Update threat library'],
                    'MEDIUM': ['Mark emitter location', 'Monitor for mode changes', 'Brief strike planners'],
                    'LOW': ['Continue monitoring', 'Log in threat database', 'Update EW picture']
                },
                'JAMMER': {
                    'CRITICAL': ['Switch to backup frequency', 'Request JSIR geolocation', 'Coordinate kinetic strike'],
                    'HIGH': ['Activate anti-jam protocols', 'Request EMBM-J support', 'Notify SATCOM operators'],
                    'MEDIUM': ['Document interference pattern', 'Test alternate channels', 'Report to JEMSOC'],
                    'LOW': ['Monitor jamming extent', 'Log spectrum activity', 'Continue mission']
                },
                'COMMS': {
                    'CRITICAL': ['Priority SIGINT exploitation', 'Forward to NSA/CSS', 'Request ELINT tasking'],
                    'HIGH': ['Analyze communication protocol', 'Cross-reference with tracks', 'Share with intel fusion'],
                    'MEDIUM': ['Continue monitoring', 'Forward to SIGINT cell', 'Update order of battle'],
                    'LOW': ['Log intercept', 'Catalog in database', 'Routine reporting']
                },
                'DATALINK': {
                    'CRITICAL': ['Priority signal exploitation', 'Attempt protocol analysis', 'Share with cyber ops'],
                    'HIGH': ['Analyze datalink protocol', 'Cross-reference with platforms', 'NSA coordination'],
                    'MEDIUM': ['Monitor link activity', 'Document message patterns', 'Intel fusion update'],
                    'LOW': ['Catalog datalink signature', 'Update EW library', 'Routine monitoring']
                },
                'IFF': {
                    'CRITICAL': ['Verify all friendly IFF codes', 'Check for spoofing', 'Alert air defense'],
                    'HIGH': ['Cross-check with known tracks', 'Update IFF database', 'Notify AWACS'],
                    'MEDIUM': ['Log IFF signature', 'Compare to known modes', 'Update threat picture'],
                    'LOW': ['Catalog IFF mode', 'Routine logging', 'Continue monitoring']
                }
            }
            
            corrective_actions = actions_by_type.get(signal_type, {}).get(threat_level, ['Monitor and report'])
            
            sigint = {
                'intercept_id': f"SIG-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(time.time() * 1000),
                'signal_type': signal_type,
                'category': 'TRADITIONAL_SIGINT',
                'frequency_mhz': round(random.uniform(100, 18000), 2),
                'bandwidth_khz': round(random.uniform(10, 500), 1),
                'modulation': random.choice(['PULSE', 'CW', 'FM', 'SPREAD_SPECTRUM', 'FHSS']),
                'bearing_deg': round(random.uniform(0, 360), 1),
                'signal_strength_dbm': round(random.uniform(-90, -30), 1),
                'lat': round(emitter.lat, 6),
                'lon': round(emitter.lon, 6),
                'elevation_deg': round(random.uniform(-5, 45), 1),
                'confidence_pct': round(random.uniform(70, 99), 1),
                'emitter_classification': f"{emitter.country}-{getattr(emitter, 'type', 'UNKNOWN')}",
                'threat_level': threat_level,
                'emission_pattern': random.choice(['SEARCH', 'TRACK', 'ACQUISITION', 'FIRE_CONTROL']),
                'collector_id': random.choice(['RC-135V', 'EP-3E', 'GROUND-STATION']),
                'corrective_actions': corrective_actions,
            }
            self._produce(SIGINT_TOPIC, sigint['intercept_id'], sigint)

    def _cyber_ew_convergence_loop(self):
        """Cyber-EW Convergence threats - RF-enabled cyber and side-channel attacks"""
        while self.running:
            time.sleep(random.uniform(480, 900))  # Every 8-15 minutes
            
            # Select a US facility as target
            facility = random.choice(US_FACILITIES)
            
            # Cyber-EW convergence event types
            event_types = [
                {
                    'signal_type': 'RF_CYBER_INJECTION',
                    'category': 'CYBER_EW_CONVERGENCE',
                    'description': 'RF-enabled malicious code injection via wireless network',
                    'attack_vector': 'Radio frequency signal carrying malicious payload',
                    'target_system': random.choice(['Tactical WiFi', 'SATCOM Terminal', 'Radio Gateway', 'Mesh Network Node']),
                    'bypass_method': 'Physical firewall bypass via RF injection',
                    'indicators': ['Anomalous RF patterns', 'Unexpected protocol headers', 'Malformed packets on air interface'],
                    'corrective_actions': [
                        'Isolate affected RF segment immediately',
                        'Enable RF shielding protocols',
                        'Scan for rogue transmitters',
                        'Coordinate with Cyber Protection Team',
                        'Report to JFHQ-DODIN'
                    ]
                },
                {
                    'signal_type': 'SIDE_CHANNEL_ATTACK',
                    'category': 'CYBER_EW_CONVERGENCE',
                    'description': 'Electromagnetic emanations exploitation (TEMPEST threat)',
                    'attack_vector': 'Passive collection of EM leakage from hardware',
                    'target_system': random.choice(['Crypto Device', 'KG-175 TACLANE', 'Server Rack', 'KIV-7M', 'HAIPE Device']),
                    'bypass_method': 'Non-invasive key extraction via EM analysis',
                    'indicators': ['Unusual TSCM readings', 'Unshielded emanations detected', 'Proximity sensor alerts'],
                    'corrective_actions': [
                        'Conduct immediate TSCM sweep',
                        'Verify TEMPEST shielding integrity',
                        'Rotate cryptographic keys',
                        'Increase physical security perimeter',
                        'Report to NSA/CSS TEMPEST office'
                    ]
                }
            ]
            
            event = random.choice(event_types)
            threat_level = random.choices(['MEDIUM', 'HIGH', 'CRITICAL'], weights=[30, 50, 20])[0]
            
            sigint = {
                'intercept_id': f"CEW-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(time.time() * 1000),
                'signal_type': event['signal_type'],
                'category': event['category'],
                'description': event['description'],
                'attack_vector': event['attack_vector'],
                'target_system': event['target_system'],
                'target_facility': facility['name'],
                'target_facility_id': facility['id'],
                'bypass_method': event['bypass_method'],
                'indicators': event['indicators'],
                'frequency_mhz': round(random.uniform(2400, 5800), 2),  # WiFi/tactical bands
                'signal_strength_dbm': round(random.uniform(-70, -40), 1),
                'lat': facility['lat'],
                'lon': facility['lon'],
                'threat_level': threat_level,
                'attribution': random.choice(['RU-GRU', 'CN-PLA-SSF', 'IR-IRGC-EC', 'Unknown APT']),
                'collector_id': random.choice(['TSCM-TEAM', 'CYBER-SENTINEL', 'RF-HUNTER']),
                'confidence_pct': round(random.uniform(65, 95), 1),
                'corrective_actions': event['corrective_actions'],
            }
            self._produce(SIGINT_TOPIC, sigint['intercept_id'], sigint)
            print(f"[CYBER-EW] {threat_level}: {event['signal_type']} targeting {facility['name']}")

    def _spectrum_management_loop(self):
        """Spectrum Management & Interference - DSO/DISA spectrum issues"""
        while self.running:
            time.sleep(random.uniform(600, 1200))  # Every 10-20 minutes
            
            # Spectrum management event types
            event_types = [
                {
                    'signal_type': 'SPECTRUM_FRATRICIDE',
                    'category': 'SPECTRUM_MANAGEMENT',
                    'description': 'Friendly force electromagnetic interference',
                    'source_system': random.choice(['AN/APG-81 Radar', 'JTRS Radio', 'Link-16 Terminal', 'AESA Array', 'SATCOM Uplink']),
                    'affected_system': random.choice(['HF Comms Net', 'UHF SATCOM', 'Tactical Data Link', 'IFF Interrogator', 'GPS Receiver']),
                    'interference_type': 'Unintentional - Frequency Overlap',
                    'band_affected': random.choice(['L-Band', 'S-Band', 'C-Band', 'X-Band', 'Ku-Band']),
                    'corrective_actions': [
                        'Coordinate frequency deconfliction via JCEOI',
                        'Adjust source system operating frequency',
                        'Notify DSO Spectrum Manager',
                        'Update Joint Spectrum Interference Resolution (JSIR)',
                        'Implement time-sharing protocol'
                    ]
                },
                {
                    'signal_type': 'COMMERCIAL_ENCROACHMENT',
                    'category': 'SPECTRUM_MANAGEMENT',
                    'description': 'Commercial 5G/LTE interference with military bands',
                    'source_system': random.choice(['5G NR Tower', 'LTE Macro Cell', 'C-Band 5G', 'CBRS System']),
                    'affected_system': random.choice(['Radar Altimeter', 'Precision Approach Radar', 'Weather Radar', 'TACAN', 'ILS Glideslope']),
                    'interference_type': 'Commercial signal bleed into protected band',
                    'band_affected': random.choice(['3.7-3.98 GHz (C-Band)', '4.2-4.4 GHz (Altimeter)', 'AWS-3 Band']),
                    'corrective_actions': [
                        'Document interference for FCC coordination',
                        'Implement geographic exclusion zone',
                        'Activate interference mitigation filters',
                        'Coordinate with NTIA on spectrum sharing',
                        'Report to DSO Spectrum Operations'
                    ]
                },
                {
                    'signal_type': 'DYNAMIC_SPECTRUM_EXPLOIT',
                    'category': 'SPECTRUM_MANAGEMENT',
                    'description': 'Adversary frequency-hopping/agile radio detected',
                    'source_system': random.choice(['Near-peer agile radio', 'Cognitive EW system', 'Adaptive jammer', 'FHSS tactical net']),
                    'affected_system': 'Multiple - Unpredictable interference pattern',
                    'interference_type': 'Intentional - Adaptive frequency exploitation',
                    'band_affected': random.choice(['VHF Tactical', 'UHF Tactical', 'L-Band', 'S-Band']),
                    'hop_rate': f"{random.randint(100, 1000)} hops/sec",
                    'corrective_actions': [
                        'Activate Cognitive EW countermeasures',
                        'Enable adaptive frequency management',
                        'Deploy spectrum sensing assets',
                        'Coordinate with EC-130H Compass Call',
                        'Request JEMSOC support for pattern analysis'
                    ]
                }
            ]
            
            event = random.choice(event_types)
            # Fratricide is usually MEDIUM, others can be higher
            if event['signal_type'] == 'SPECTRUM_FRATRICIDE':
                threat_level = random.choices(['LOW', 'MEDIUM', 'HIGH'], weights=[30, 50, 20])[0]
            else:
                threat_level = random.choices(['MEDIUM', 'HIGH', 'CRITICAL'], weights=[40, 40, 20])[0]
            
            # Pick a location (facility for fratricide, random for others)
            if event['signal_type'] == 'SPECTRUM_FRATRICIDE':
                facility = random.choice(US_FACILITIES)
                lat, lon = facility['lat'], facility['lon']
                location_name = facility['name']
            else:
                # Random location in theater
                lat = round(random.uniform(20, 40), 6)
                lon = round(random.uniform(30, 60), 6)
                location_name = f"Grid {int(lat)}{int(lon)}"
            
            sigint = {
                'intercept_id': f"SPM-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(time.time() * 1000),
                'signal_type': event['signal_type'],
                'category': event['category'],
                'description': event['description'],
                'source_system': event['source_system'],
                'affected_system': event['affected_system'],
                'interference_type': event['interference_type'],
                'band_affected': event['band_affected'],
                'hop_rate': event.get('hop_rate'),
                'location_name': location_name,
                'frequency_mhz': round(random.uniform(225, 5800), 2),
                'signal_strength_dbm': round(random.uniform(-60, -30), 1),
                'lat': lat,
                'lon': lon,
                'threat_level': threat_level,
                'collector_id': random.choice(['DSO-MONITOR', 'JSIR-SYSTEM', 'SPECTRUM-ANALYZER']),
                'confidence_pct': round(random.uniform(75, 98), 1),
                'corrective_actions': event['corrective_actions'],
            }
            self._produce(SIGINT_TOPIC, sigint['intercept_id'], sigint)
            print(f"[SPECTRUM] {threat_level}: {event['signal_type']} - {event['affected_system']}")

    def _navwar_pnt_loop(self):
        """Navigation Warfare (NAVWAR) / PNT Threats - GPS jamming and spoofing"""
        while self.running:
            time.sleep(random.uniform(720, 1200))  # Every 12-20 minutes
            
            # Known GPS threat zones
            threat_zones = [
                {'name': 'Eastern Mediterranean', 'lat': 35.0, 'lon': 34.0, 'source': 'RU-Syria'},
                {'name': 'Persian Gulf', 'lat': 27.0, 'lon': 52.0, 'source': 'IR-IRGC'},
                {'name': 'Strait of Hormuz', 'lat': 26.5, 'lon': 56.5, 'source': 'IR-IRGCN'},
                {'name': 'Southern Red Sea', 'lat': 15.5, 'lon': 41.5, 'source': 'Houthi-EW'},
                {'name': 'Gulf of Aden', 'lat': 12.5, 'lon': 46.0, 'source': 'Unattributed'},
            ]
            
            zone = random.choice(threat_zones)
            
            # NAVWAR event types
            event_types = [
                {
                    'signal_type': 'GNSS_JAMMING',
                    'category': 'NAVWAR_PNT',
                    'description': 'Broad-spectrum GNSS jamming detected',
                    'attack_method': 'High-power noise jamming across L1/L2 bands',
                    'gnss_affected': random.choice(['GPS L1/L2', 'GPS + GLONASS', 'GPS + Galileo', 'All GNSS']),
                    'jamming_radius_km': random.randint(20, 150),
                    'jammer_type': random.choice(['R-330Zh Zhitel', 'Pole-21', 'RP-377', 'Unknown Ground-Based']),
                    'affected_operations': random.choice(['Drone ISR', 'Precision Strike', 'Logistics Convoy', 'Aircraft Navigation', 'Ship Navigation']),
                    'corrective_actions': [
                        'Switch to inertial navigation backup',
                        'Activate M-Code GPS receivers',
                        'Enable anti-jam antenna systems',
                        'Coordinate alternate PNT source',
                        'Report to NAVWAR Fusion Center'
                    ]
                },
                {
                    'signal_type': 'GNSS_SPOOFING',
                    'category': 'NAVWAR_PNT',
                    'description': 'GPS spoofing attack - false position/timing',
                    'attack_method': 'Counterfeit GPS signal broadcast',
                    'gnss_affected': 'GPS L1 C/A (civil signal)',
                    'spoofing_type': random.choice(['Position Offset', 'Time Manipulation', 'Trajectory Deviation']),
                    'position_error_m': random.randint(100, 5000),
                    'time_error_ns': random.randint(100, 10000),
                    'spoofer_type': random.choice(['Vehicle-mounted', 'Fixed Site', 'Drone-deployed', 'Ship-based']),
                    'affected_operations': random.choice(['Crypto Timing Sync', 'Network Time Protocol', 'Precision Munitions', 'UAV Navigation', 'Tactical Data Links']),
                    'crypto_impact': random.choice(['Key rollover failure risk', 'HAIPE timing drift', 'Link encryption desync', 'None detected']),
                    'corrective_actions': [
                        'Enable GPS spoofing detection algorithms',
                        'Cross-check with alternative PNT sources',
                        'Verify timing against WWVB/Cesium backup',
                        'Alert crypto custodians of timing anomaly',
                        'Implement chipscale atomic clock backup',
                        'Report to USCYBERCOM for attribution'
                    ]
                }
            ]
            
            event = random.choice(event_types)
            # Spoofing is generally more dangerous than jamming
            if event['signal_type'] == 'GNSS_SPOOFING':
                threat_level = random.choices(['HIGH', 'CRITICAL'], weights=[40, 60])[0]
            else:
                threat_level = random.choices(['MEDIUM', 'HIGH', 'CRITICAL'], weights=[30, 50, 20])[0]
            
            # Add some position variance around the threat zone
            lat = round(zone['lat'] + random.uniform(-2, 2), 6)
            lon = round(zone['lon'] + random.uniform(-2, 2), 6)
            
            sigint = {
                'intercept_id': f"NAV-{uuid.uuid4().hex[:8].upper()}",
                'timestamp': int(time.time() * 1000),
                'signal_type': event['signal_type'],
                'category': event['category'],
                'description': event['description'],
                'attack_method': event['attack_method'],
                'gnss_affected': event['gnss_affected'],
                'threat_zone': zone['name'],
                'attribution': zone['source'],
                'affected_operations': event['affected_operations'],
                'frequency_mhz': round(random.choice([1575.42, 1227.60, 1176.45]), 2),  # GPS frequencies
                'signal_strength_dbm': round(random.uniform(-50, -20), 1),
                'lat': lat,
                'lon': lon,
                'threat_level': threat_level,
                'collector_id': random.choice(['GPS-MONITOR', 'PNT-SENTINEL', 'NAVWAR-SENSOR']),
                'confidence_pct': round(random.uniform(80, 99), 1),
                'corrective_actions': event['corrective_actions'],
            }
            
            # Add event-specific fields
            if event['signal_type'] == 'GNSS_JAMMING':
                sigint['jamming_radius_km'] = event['jamming_radius_km']
                sigint['jammer_type'] = event['jammer_type']
            else:
                sigint['spoofing_type'] = event['spoofing_type']
                sigint['position_error_m'] = event['position_error_m']
                sigint['time_error_ns'] = event['time_error_ns']
                sigint['crypto_impact'] = event['crypto_impact']
                sigint['spoofer_type'] = event['spoofer_type']
            
            self._produce(SIGINT_TOPIC, sigint['intercept_id'], sigint)
            print(f"[NAVWAR] {threat_level}: {event['signal_type']} in {zone['name']} - {event['affected_operations']}")

    def _satellite_loop(self):
        """Satellite imagery metadata"""
        while self.running:
            time.sleep(random.uniform(60, 180))       # an image every few minutes
            
            sat = {
                'image_id': f"SAT-{uuid.uuid4().hex[:8].upper()}",
                'source_id': random.choice(['WV-3', 'GE-1', 'PL-1', 'ICEYE-X1']),
                'timestamp': int(time.time() * 1000),
                'acquisition_time': datetime.utcnow().isoformat(),
                'sensor_type': random.choice(['EO', 'IR', 'SAR', 'MSI']),
                'resolution_m': round(random.uniform(0.3, 5.0), 2),
                'cloud_cover_pct': round(random.uniform(0, 30), 1),
                'sun_elevation_deg': round(random.uniform(20, 70), 1),
                'off_nadir_angle_deg': round(random.uniform(0, 30), 1),
                'center_lat': round(random.uniform(10, 40), 6),
                'center_lon': round(random.uniform(25, 65), 6),
                'classification': random.choice(['UNCLASSIFIED', 'SECRET', 'TOP_SECRET']),
                'quality_score': round(random.uniform(70, 99), 1),
                'processing_status': 'COMPLETE',
            }
            self._produce(SATELLITE_TOPIC, sat['image_id'], sat)

    def _jsir_loop(self):
        """JSIR - Joint Spectrum Interference Resolution events
        Simulates SATCOM jamming incidents and DISA JSIR workflow
        """
        while self.running:
            time.sleep(random.uniform(300, 600))  # a jamming incident every 5-10 minutes
            
            # Select jammer, target satellite, and affected units
            jammer_site = random.choice(JAMMER_SITES)
            jammer_system = random.choice([j for j in HOSTILE_JAMMERS if j['country'] == jammer_site['country']])
            target_sat = random.choice(US_SATCOM_SYSTEMS)
            affected = [u for u in AFFECTED_UNITS if target_sat['name'] in u['satcom_deps']]
            
            if not affected:
                affected = [random.choice(AFFECTED_UNITS)]
            
            # Generate TDOA/FDOA geolocation data
            # Uncertainty ellipse based on jammer power and satellite geometry
            cep_km = round(random.uniform(0.5, 5.0) * (10 / jammer_system['power_kw']), 2)
            ellipse_major_km = round(cep_km * random.uniform(1.5, 3.0), 2)
            ellipse_minor_km = round(cep_km * random.uniform(0.8, 1.2), 2)
            ellipse_orientation = round(random.uniform(0, 180), 1)
            
            # Adjacent satellite used for TDOA
            adjacent_sat = random.choice([s for s in US_SATCOM_SYSTEMS if s['name'] != target_sat['name']])
            tdoa_ms = round(random.uniform(0.1, 5.0), 3)
            fdoa_hz = round(random.uniform(-50, 50), 2)
            
            incident_id = f"JSIR-{uuid.uuid4().hex[:8].upper()}"
            
            jsir_event = {
                'incident_id': incident_id,
                'timestamp': int(time.time() * 1000),
                'event_type': 'SATCOM_INTERFERENCE',
                'workflow_stage': random.choice(['DETECTION', 'CHARACTERIZATION', 'GEOLOCATION', 'MITIGATION']),
                
                # Jammer information
                'jammer_id': jammer_site['id'],
                'jammer_location': jammer_site['name'],
                'jammer_country': jammer_site['country'],
                'jammer_lat': jammer_site['lat'] + random.uniform(-0.01, 0.01),  # Slight position variation
                'jammer_lon': jammer_site['lon'] + random.uniform(-0.01, 0.01),
                'jammer_terrain': jammer_site['terrain'],
                'jammer_system': jammer_system['system'],
                'jammer_type': jammer_system['type'],
                'jammer_power_kw': jammer_system['power_kw'],
                'jammer_bands': jammer_system['bands'],
                
                # Target satellite
                'target_satellite': target_sat['name'],
                'target_sat_type': target_sat['type'],
                'target_freq_ghz': target_sat['freq_ghz'],
                'target_lon_slot': target_sat['lon_slot'],
                
                # Interference characteristics
                'interference_type': random.choice(['BROADBAND_NOISE', 'SWEPT_CW', 'PULSED', 'SPOT_JAMMING']),
                'noise_floor_increase_db': round(random.uniform(10, 40), 1),
                'signal_to_noise_degradation_db': round(random.uniform(5, 25), 1),
                'affected_bandwidth_mhz': round(random.uniform(5, 50), 1),
                'spectral_signature': random.choice(['HAYSTACK', 'SWEPT', 'MULTI_TONE', 'BARRAGE']),
                
                # TDOA/FDOA geolocation
                'geolocation_method': 'TDOA_FDOA',
                'primary_satellite': target_sat['name'],
                'adjacent_satellite': adjacent_sat['name'],
                'tdoa_milliseconds': tdoa_ms,
                'fdoa_hertz': fdoa_hz,
                'cep_km': cep_km,
                'uncertainty_ellipse_major_km': ellipse_major_km,
                'uncertainty_ellipse_minor_km': ellipse_minor_km,
                'uncertainty_ellipse_orientation_deg': ellipse_orientation,
                'geolocation_confidence_pct': round(random.uniform(75, 98), 1),
                
                # Affected units
                'affected_units': [u['callsign'] for u in affected],
                'affected_unit_types': [u['type'] for u in affected],
                'affected_region': affected[0]['region'],
                'mission_impact': random.choice(['DEGRADED_COMMS', 'TOTAL_BLACKOUT', 'INTERMITTENT', 'BACKUP_ACTIVATED']),
                'mission_impact_severity': random.choice(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']),
                
                # JSIR workflow status
                'jsir_ticket_status': random.choice(['SUBMITTED', 'ANALYZING', 'GEOLOCATING', 'MITIGATING', 'RESOLVED']),
                'friendly_fire_ruled_out': True,
                'recommended_action': random.choice([
                    'FREQUENCY_HOP',
                    'BEAM_REALLOCATION', 
                    'ALTERNATE_SATELLITE',
                    'INCREASE_EIRP',
                    'KINETIC_STRIKE_COORDS',
                    'ELECTRONIC_ATTACK'
                ]),
                
                # Response options
                'mitigation_option_a': 'BEAM_REALLOCATION',
                'mitigation_option_b': 'ALTERNATE_SATELLITE',
                'strike_coords_available': True,
                'strike_coords_lat': jammer_site['lat'],
                'strike_coords_lon': jammer_site['lon'],
            }
            
            self._produce(SPECTRUM_TOPIC, incident_id, jsir_event)
            print(f"[JSIR] {jsir_event['mission_impact_severity']}: {jammer_system['system']} jamming {target_sat['name']} - Affects {', '.join([u['callsign'] for u in affected])}")
            
            # Also generate EMBM track for the jammer
            self._generate_embm_track(jsir_event)
    
    def _generate_embm_track(self, jsir_event):
        """Generate EMBM-J track from JSIR geolocation"""
        embm_track = {
            'track_id': f"EMBM-{jsir_event['jammer_id']}-{uuid.uuid4().hex[:4].upper()}",
            'timestamp': int(time.time() * 1000),
            'source_incident': jsir_event['incident_id'],
            
            # Track classification
            'track_type': 'HOSTILE_EMITTER',
            'track_category': 'EW_JAMMER',
            'emitter_function': 'SATCOM_JAMMER',
            
            # Position from geolocation
            'lat': jsir_event['jammer_lat'],
            'lon': jsir_event['jammer_lon'],
            'position_accuracy_m': jsir_event['cep_km'] * 1000,
            'terrain': jsir_event['jammer_terrain'],
            
            # Emitter characteristics
            'emitter_system': jsir_event['jammer_system'],
            'emitter_country': jsir_event['jammer_country'],
            'emitter_type': jsir_event['jammer_type'],
            'estimated_power_kw': jsir_event['jammer_power_kw'],
            'operating_bands': jsir_event['jammer_bands'],
            'spectral_signature': jsir_event['spectral_signature'],
            
            # Threat assessment
            'threat_level': jsir_event['mission_impact_severity'],
            'confidence': jsir_event['geolocation_confidence_pct'],
            'first_detected': datetime.utcnow().isoformat(),
            'last_updated': datetime.utcnow().isoformat(),
            
            # Targeting data
            'targetable': True,
            'target_coord_lat': jsir_event['strike_coords_lat'],
            'target_coord_lon': jsir_event['strike_coords_lon'],
            'target_coord_accuracy_m': jsir_event['cep_km'] * 1000,
            
            # Correlation with other INT
            'correlated_sigint': True,
            'correlated_imagery': random.choice([True, False]),
            'nearby_assets': random.choice([
                'MOBILE_RADAR',
                'SAM_BATTERY', 
                'COMMAND_POST',
                'LOGISTICS_HUB',
                'ISOLATED'
            ]),
            
            # Display properties for EMBM-J
            'display_icon': 'JAMMER',
            'display_color': 'RED',
            'display_priority': 'HIGH' if jsir_event['mission_impact_severity'] in ['HIGH', 'CRITICAL'] else 'MEDIUM',
        }
        
        self._produce(EMBM_TOPIC, embm_track['track_id'], embm_track)

    def _facility_gps_loop(self):
        """Emit GPS records for all US facilities"""
        while self.running:
            for fac in US_FACILITIES:
                gps_record = {
                    'unit_id': fac['id'],
                    'unit_name': fac['name'],
                    'unit_type': fac['type'],
                    'timestamp': int(time.time() * 1000),
                    'lat': fac['lat'],
                    'lon': fac['lon'],
                    'heading_deg': 0,
                    'speed_kph': 0,
                    'altitude_m': 0,
                    'fuel_level_pct': 100.0,
                    'ammo_level_pct': 100.0,
                    'personnel_count': random.randint(1000, 10000),
                    'equipment_status': 'FULLY_OPERATIONAL',
                    'mission_status': 'OPERATIONAL',
                    'fuel_consumption_rate': 0,
                    'comms_status': 'NOMINAL',
                    'country': fac['country'],          # host nation: the agents' FLASH notification goes to its air-defence watch
                    'region': fac.get('region', 'CENTCOM'),
                }
                self._produce(GPS_TOPIC, fac['id'], gps_record)
            time.sleep(10)

    def _stats_loop(self):
        while self.running:
            time.sleep(30)
            print(f"[STATS] Aircraft: {len(self.aircraft)} | Ships: {len(self.naval)} | Ground: {len(self.ground_units)} | Facilities: {len(US_FACILITIES)} | {self.threats.stats()} | sent {self.producer.stats()}")

    def _on_action(self, topic, action):
        """Operator actions close the loop (engage -> interceptor); exercise injects stage a scenario on cue."""
        if str(action.get('domain', '')).upper() == 'EXERCISE' or action.get('action') == 'inject':
            if action.get('scenario') == 'probe':       # a hostile aircraft turns toward a US ship: proximity + ship-threat
                self._probe(action.get('target_id'))
            else:
                self.threats.inject(action)
            return
        self.threats.on_action(topic, action)

    def _probe(self, hull=None):
        ships = [s for s in self.naval if s.country == 'US' and s.ship_class != 'SUBMARINE']
        if hull:
            ships = [s for s in ships if s.hull.lower() == hull.lower()] or ships
        if not ships:
            return
        ship = random.choice(ships)
        hostile = [a for a in self.aircraft if a.country != 'US']
        ac = min(hostile, key=lambda a: (a.lat - ship.lat) ** 2 + (a.lon - ship.lon) ** 2)
        home = (ac.home_lat, ac.home_lon)
        ac.home_lat, ac.home_lon = ship.lat, ship.lon            # its patrol box is now centered on the ship
        ac.lat, ac.lon = ship.lat + 0.9, ship.lon + 0.6           # start ~60 nm out, closing
        ac.status = 'INTERCEPT'; ac.patrol_radius = 0.3
        print(f"[EXERCISE] probe: {ac.country} {ac.type} {ac.tail} vectored at {ship.name}")
        def release():
            ac.home_lat, ac.home_lon = home
            ac.patrol_radius = 0.9; ac.status = 'RTB'
        threading.Timer(600.0, release).start()

    def _threat_targets(self):
        """Friendly ships and facilities a launch site could aim at: US assets (IAMD / ship-defense workflows)
        and coalition partner assets (the coalition liaison workflow)."""
        out = [{'id': f['id'], 'name': f['name'], 'type': f['type'], 'lat': f['lat'], 'lon': f['lon'], 'country': 'US', 'host': f['country'], 'coalition': False,
                'defended_by': 'THAAD / Patriot' if 'AIR' in f['type'] else 'Patriot / NASAMS'} for f in US_FACILITIES]
        for s in self.naval:
            if s.country == 'US' and s.ship_class != 'SUBMARINE':       # a submerged boat is not a missile or drone target
                out.append({'id': s.hull, 'name': s.name, 'type': 'CARRIER' if s.ship_class == 'CARRIER' else 'VESSEL',
                            'lat': s.lat, 'lon': s.lon, 'country': 'US', 'coalition': False, 'defended_by': 'Aegis (SM-6 / SM-2 / ESSM)'})
        for f in COALITION_FACILITIES:
            out.append({'id': f['id'], 'name': f['name'], 'type': f['type'], 'lat': f['lat'], 'lon': f['lon'], 'country': f['country'], 'host': f['host'],
                        'coalition': True, 'defended_by': f['defended_by']})
        with self._coalition_lock:
            ships = list(self._coalition_ships.values())
        for c in ships:                                                 # partner ships, as their own C2 reports them
            out.append({'id': c['track_id'], 'name': f"{c.get('vessel_name', 'partner vessel')} ({c['track_id']})", 'type': 'VESSEL',
                        'lat': c['lat'], 'lon': c['lon'], 'country': c.get('nation'), 'coalition': True,
                        'defended_by': {'GBR': 'Sea Viper / Sea Ceptor', 'FRA': 'Aster 15/30', 'AUS': 'ESSM / SM-2', 'DEU': 'ESSM / RAM', 'JPN': 'ESSM / SM-2'}.get(c.get('nation'), 'own point defence') + '; US escort if in company'})
        return out

    def _on_coalition(self, topic, rec):
        """Coalition naval tracks from the partner C2 feed: the simulator learns where partner ships are the same way
        everyone else does, by subscribing."""
        if rec.get('track_id') and rec.get('lat') is not None:
            with self._coalition_lock:
                self._coalition_ships[rec['track_id']] = rec

    def start(self):
        if not self.connect():
            return False
        self.running = True
        # Missiles and one-way-attack drones, with the operator's ENGAGE closing the loop.
        from missile_threats import MissileThreats
        self.threats = MissileThreats(self._threat_targets, lambda stream, rec: self._produce(stream, rec.get('track_id') or rec.get('interceptor_id'), rec))
        self.producer.subscribe_actions(self._on_action)
        self._coalition_ships, self._coalition_lock = {}, threading.Lock()
        self.producer.subscribe(['cop/intel/coalition-naval/v1/>'], self._on_coalition)
        self.threats.start()
        threads = [
            self._air_loop, self._naval_loop, self._ground_loop,
            self._cyber_loop, self._sigint_loop, self._satellite_loop,
            self._facility_gps_loop, self._jsir_loop, self._stats_loop,
            # New DISA-specific EW/Spectrum/NAVWAR threads
            self._cyber_ew_convergence_loop,
            self._spectrum_management_loop,
            self._navwar_pnt_loop,
        ]
        for fn in threads:
            threading.Thread(target=fn, daemon=True).start()
        return True

    def stop(self):
        self.running = False
        if self.producer:
            self.producer.close()


def main():
    print("\n" + "="*60)
    print("COP multi-domain simulator -> Solace event mesh")
    print("tracks: direct messaging | incidents/intercepts/imagery/jamming: guaranteed")
    print("ground tracks also published over MQTT")
    print("="*60 + "\n")
    gen = Generator()
    if not gen.start():
        return 1
    print(f"✓ Running - {len(gen.aircraft)} aircraft, {len(gen.naval)} ships, {len(gen.ground_units)} ground units\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        gen.stop()
    return 0

if __name__ == '__main__':
    exit(main())

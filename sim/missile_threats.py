"""
Missile and one-way-attack UAS threats for the multi-domain simulator.

Launch sites in the theater fire at friendly ships and facilities. A launch is a
guaranteed event (someone must act on it); the in-flight track is a direct stream
(the next position supersedes the last). Phases: BOOST -> MIDCOURSE -> TERMINAL,
then INTERCEPTED or IMPACT. Drones fly in waves of 2-3 at low speed. Tempo: one
launch event every 1-2 minutes.

Closed loop with the COP: the simulator subscribes to cop/c2/action/v1/> and an
"engage" or "weapons_free" on a track in flight fires an interceptor from the
defending asset. The interceptor is its own track (the defender's fire control
reports it), flies out to meet the inbound, and reports HIT or MISS; a miss is
followed by a second shot while there is time (shoot-look-shoot). The decision
an operator takes on the map changes what the sensors report next, and every
consumer on the mesh sees the whole engagement.

Topics (direct):      cop/air/missile/v1/{aor}/hostile/{track}/{phase}
                      cop/air/uas/v1/{aor}/hostile/{track}/{phase}
                      cop/air/interceptor/v1/{aor}/friend/{interceptor}/{phase}       launch | closing | hit | miss
Topics (guaranteed):  cop/air/missile-event/v1/{aor}/hostile/{track}/{launch|terminal|intercepted|impact}
                      cop/air/uas-event/v1/{aor}/hostile/{track}/{launch|terminal|intercepted|impact}
                      cop/air/interceptor-event/v1/{aor}/friend/{interceptor}/{launch|hit|miss}
"""
import math
import os
import random
import threading
import time
import uuid

LAUNCH_SITES = [
    {'id': 'LS-IR-01', 'name': 'Bandar Abbas coastal battery', 'country': 'IR', 'lat': 27.10, 'lon': 56.20, 'weapons': ['ASBM', 'SRBM', 'CM', 'UAS']},
    {'id': 'LS-IR-02', 'name': 'Shiraz missile base', 'country': 'IR', 'lat': 29.45, 'lon': 52.70, 'weapons': ['MRBM', 'SRBM']},
    {'id': 'LS-IR-03', 'name': 'Chabahar launch site', 'country': 'IR', 'lat': 25.35, 'lon': 60.60, 'weapons': ['ASBM', 'UAS', 'CM']},
    {'id': 'LS-IR-04', 'name': 'Kermanshah underground complex', 'country': 'IR', 'lat': 34.30, 'lon': 47.10, 'weapons': ['MRBM']},
    {'id': 'LS-YE-01', 'name': 'Sanaa launch area', 'country': 'YE', 'lat': 15.40, 'lon': 44.25, 'weapons': ['MRBM', 'CM', 'UAS']},
    {'id': 'LS-YE-02', 'name': 'Hodeidah coastal site', 'country': 'YE', 'lat': 14.78, 'lon': 42.98, 'weapons': ['ASBM', 'UAS', 'CM']},
    {'id': 'LS-YE-03', 'name': 'Saada hills', 'country': 'YE', 'lat': 16.95, 'lon': 43.75, 'weapons': ['SRBM', 'UAS']},
]
# type: (class, name, speed_kts, range_km, warhead)
WEAPONS = {
    'SRBM': [('SRBM', 'Fateh-110', 2900, 300, 'HE 500 kg'), ('SRBM', 'Zolfaghar', 3100, 700, 'HE 580 kg')],
    'MRBM': [('MRBM', 'Qiam-1', 3600, 800, 'HE 750 kg'), ('MRBM', 'Emad', 4200, 1700, 'HE 750 kg'), ('MRBM', 'Burkan-2H', 3600, 1000, 'HE 500 kg')],
    'ASBM': [('ASBM', 'Khalij Fars', 2800, 300, 'HE 650 kg, EO seeker'), ('ASBM', 'Asef', 2800, 450, 'HE 500 kg')],
    'CM':   [('CM', 'Quds-2', 480, 1350, 'HE 50 kg'), ('CM', 'Hoveyzeh', 500, 1350, 'HE 450 kg')],
    'UAS':  [('OWA-UAS', 'Shahed-136', 100, 2000, 'HE 40 kg'), ('OWA-UAS', 'Samad-3', 110, 1500, 'HE 18 kg'), ('OWA-UAS', 'Shahed-238', 320, 1000, 'HE 50 kg')],
}
# interceptor: (name, speed_kts, max_range_nm, pk) -- the shooter is the asset under attack (ship or base air defense)
# Drones far outside SAM range are run down by the combat air patrol: a fighter launched from the asset itself
# (carrier air wing) or the base, which is how the Red Sea drone fights are actually fought.
INTERCEPTORS_SHIP = {'missile': [('SM-6 (Aegis)', 2300, 130, 0.85), ('SM-2 (Aegis)', 2000, 90, 0.8), ('ESSM', 2600, 27, 0.75)],
                     'uas':     [('ESSM', 2600, 27, 0.9), ('SM-2 (Aegis)', 2000, 90, 0.85), ('CIWS', 1500, 2, 0.7), ('F/A-18E CAP (AIM-9X)', 550, 500, 0.9)]}
INTERCEPTORS_LAND = {'missile': [('THAAD', 5400, 110, 0.9), ('Patriot PAC-3', 3300, 20, 0.85), ('NASAMS', 1600, 25, 0.7)],
                     'uas':     [('Coyote C-UAS', 500, 10, 0.9), ('Avenger (Stinger)', 1500, 4, 0.75), ('NASAMS', 1600, 25, 0.85), ('F-15E CAP (AIM-9X)', 550, 600, 0.9)]}
def is_cap(spec):
    return 'CAP' in spec[0]
# Flight time is compressed per class so a shot is watchable: a 300 km ballistic
# shot lasts about a minute, a drone wave several minutes.
TIME_SCALE = {'SRBM': 3.5, 'MRBM': 3.5, 'ASBM': 3.5, 'CM': 12.0, 'OWA-UAS': 55.0}


def nm(lat1, lon1, lat2, lon2):
    r = 3440.065
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def bearing(lat1, lon1, lat2, lon2):
    p1, p2, dl = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


class Threat:
    def __init__(self, kind, site, weapon, target, wave_id=None, wave_n=1):
        wclass, wname, speed, rng, warhead = weapon
        self.kind = kind                        # 'missile' | 'uas'
        self.track_id = f"{'MSL' if kind == 'missile' else 'UAS'}-{uuid.uuid4().hex[:6].upper()}"
        self.site, self.weapon_class, self.weapon, self.speed_kts, self.warhead = site, wclass, wname, speed, warhead
        self.target = target                    # {'id','name','type','lat','lon','defended_by'}
        self.lat, self.lon = site['lat'] + random.uniform(-0.03, 0.03), site['lon'] + random.uniform(-0.03, 0.03)
        self.launch_lat, self.launch_lon = self.lat, self.lon
        self.total_nm = nm(self.lat, self.lon, target['lat'], target['lon'])
        self.flown_nm = 0.0
        self.phase = 'BOOST'
        self.outcome = None
        self.engaged_by = None
        self.launched_at = time.time()
        self.wave_id, self.wave_n = wave_id, wave_n
        self.apogee_km = {'SRBM': 100, 'MRBM': 300, 'ASBM': 120, 'CM': 0.05, 'OWA-UAS': 1.5}[wclass]
        self.intercept_at = None                # fraction of flight where the defender's own fire control engages (None = relies on the operator)
        if random.random() < (0.55 if kind == 'missile' else 0.45):
            self.intercept_at = random.uniform(0.5, 0.8)
        self.engagement = None                  # the interceptor currently assigned (Interceptor) or None
        self.shots = 0
        self.engaged_mode = None                # 'operator' | 'auto'
        self.killed_at = None                   # (lat, lon) where it was intercepted

    def tti_s(self):
        remaining = max(0.0, self.total_nm - self.flown_nm)
        return round(remaining / self.speed_kts * 3600 / TIME_SCALE[self.weapon_class], 1)

    def advance(self, dt):
        step = self.speed_kts / 3600 * dt * TIME_SCALE[self.weapon_class]
        self.flown_nm = min(self.total_nm, self.flown_nm + step)
        f = self.flown_nm / self.total_nm if self.total_nm else 1
        self.lat = self.launch_lat + (self.target['lat'] - self.launch_lat) * f
        self.lon = self.launch_lon + (self.target['lon'] - self.launch_lon) * f
        if self.kind == 'missile' and self.weapon_class != 'CM':
            self.alt_km = round(self.apogee_km * math.sin(math.pi * f), 1)
        else:
            self.alt_km = self.apogee_km
        if self.outcome:
            return
        if f >= 1.0:
            self.outcome = 'IMPACT'
        self.phase = 'BOOST' if f < 0.15 else 'MIDCOURSE' if f < 0.8 else 'TERMINAL'

    def record(self):
        f = self.flown_nm / self.total_nm if self.total_nm else 1
        return {
            'track_id': self.track_id, 'kind': self.kind, 'weapon_class': self.weapon_class, 'weapon': self.weapon,
            'warhead': self.warhead, 'country': self.site['country'], 'iff_mode': 'HOSTILE',
            'launch_site_id': self.site['id'], 'launch_site': self.site['name'], 'launch_lat': self.launch_lat, 'launch_lon': self.launch_lon,
            'target_id': self.target['id'], 'target_name': self.target['name'], 'target_type': self.target['type'],
            'target_lat': self.target['lat'], 'target_lon': self.target['lon'], 'defended_by': self.target.get('defended_by'),
            'target_country': self.target.get('country', 'US'), 'target_host': self.target.get('host'), 'target_coalition': bool(self.target.get('coalition')),
            'lat': round(self.lat, 6), 'lon': round(self.lon, 6), 'alt_km': self.alt_km if hasattr(self, 'alt_km') else 0,
            'heading_deg': round(bearing(self.lat, self.lon, self.target['lat'], self.target['lon']), 1),
            'speed_kts': self.speed_kts, 'range_nm': round(self.total_nm, 1), 'flown_pct': round(100 * f, 1),
            'tti_s': self.tti_s(), 'phase': self.phase, 'outcome': self.outcome, 'engaged_by': self.engaged_by,
            'wave_id': self.wave_id, 'wave_size': self.wave_n, 'launched_at': int(self.launched_at * 1000),
            'engaged_mode': self.engaged_mode, 'shots': self.shots,
            'engagement': self.engagement.summary() if self.engagement else None,
            'killed_lat': self.killed_at[0] if self.killed_at else None, 'killed_lon': self.killed_at[1] if self.killed_at else None,
            'timestamp': int(time.time() * 1000),
        }


class Interceptor:
    """One shot from the defending asset at one inbound. Flies a pursuit course at its own speed (same time
    compression as the inbound so the two are watchable together) and resolves HIT or MISS at the merge."""
    def __init__(self, threat, spec, shooter, mode, operator=None):
        name, speed, rng, pk = spec
        self.threat, self.name, self.speed_kts, self.range_nm, self.pk = threat, name, speed, rng, pk
        self.shooter = shooter                  # {'id','name','lat','lon'}
        self.mode, self.operator = mode, operator
        self.int_id = f"INT-{uuid.uuid4().hex[:6].upper()}"
        self.lat, self.lon = shooter['lat'], shooter['lon']
        # assigned beyond the interceptor's reach: it stands by on the rail and launches when the inbound closes to range
        self.phase = 'LAUNCH' if nm(self.lat, self.lon, threat.lat, threat.lon) <= rng * 1.4 else 'STANDBY'
        self.result = None                      # 'HIT' | 'MISS'
        self.launched_at = time.time()
        self.flown_nm = 0.0

    def _plan(self):
        """Fix the flyout duration at launch so the engagement is watchable: a real SM-6 shot at a cruise missile
        is over in a couple of ticks at the compressed time scale, so the shot is paced to 12-40 s wall clock."""
        t = self.threat
        dist = nm(self.lat, self.lon, t.lat, t.lon)
        real_closing = (self.speed_kts + t.speed_kts) / 3600 * TIME_SCALE[t.weapon_class]      # nm per wall-clock second
        self.duration = max(12.0, min(40.0, dist / max(real_closing, 0.01)))
        self.step_nm = dist / self.duration

    def advance(self, dt):
        t = self.threat
        if self.phase == 'STANDBY':
            if nm(self.lat, self.lon, t.lat, t.lon) <= self.range_nm * 1.4:
                self.phase = 'LAUNCH'; self.launched_at = time.time(); self._plan()
                return 'LAUNCH'
            return None
        if not hasattr(self, 'step_nm'):
            self._plan()
        step = self.step_nm * dt
        closing = step + t.speed_kts / 3600 * dt * TIME_SCALE[t.weapon_class]
        dist = nm(self.lat, self.lon, t.lat, t.lon)
        if dist <= closing:                     # the merge
            self.result = 'HIT' if random.random() < self.pk else 'MISS'
            self.phase = self.result
            self.lat, self.lon = t.lat, t.lon
            return self.result
        if self.flown_nm >= self.range_nm * 1.5:    # ran out of range before the merge
            self.result = self.phase = 'MISS'
            return self.result
        f = step / dist
        self.lat += (t.lat - self.lat) * f
        self.lon += (t.lon - self.lon) * f
        self.flown_nm += step
        self.phase = 'CLOSING'
        return None

    def summary(self):
        return {'interceptor_id': self.int_id, 'interceptor': self.name, 'interceptor_type': 'CAP' if 'CAP' in self.name else 'SAM', 'shooter': self.shooter['name'], 'shooter_id': self.shooter['id'],
                'phase': self.phase, 'result': self.result, 'mode': self.mode, 'lat': round(self.lat, 6), 'lon': round(self.lon, 6)}

    def record(self):
        t = self.threat
        return {
            'interceptor_id': self.int_id, 'interceptor': self.name, 'interceptor_type': 'CAP' if 'CAP' in self.name else 'SAM', 'iff_mode': 'FRIENDLY', 'country': 'US',
            'shooter_id': self.shooter['id'], 'shooter': self.shooter['name'], 'shooter_lat': self.shooter['lat'], 'shooter_lon': self.shooter['lon'], 'shooter_country': self.shooter.get('country', 'US'),
            'engaging': t.track_id, 'engaging_kind': t.kind, 'engaging_weapon': t.weapon, 'engaging_wave_id': t.wave_id,
            'mode': self.mode, 'operator': self.operator, 'shot': t.shots,
            'lat': round(self.lat, 6), 'lon': round(self.lon, 6), 'speed_kts': self.speed_kts,
            'heading_deg': round(bearing(self.lat, self.lon, t.lat, t.lon), 1),
            'range_to_target_nm': round(nm(self.lat, self.lon, t.lat, t.lon), 1), 'phase': self.phase, 'result': self.result,
            'pk': self.pk, 'time_to_merge_s': round(nm(self.lat, self.lon, t.lat, t.lon) / max(getattr(self, 'step_nm', 1), 0.01), 1) if self.phase in ('LAUNCH', 'CLOSING') else None,
            'launched_at': int(self.launched_at * 1000), 'timestamp': int(time.time() * 1000),
        }


class MissileThreats:
    """Runs inside the generator. `targets()` returns live friendly ships/facilities; `emit(stream, rec)` publishes."""
    def __init__(self, targets_fn, emit):
        self.targets_fn, self.emit = targets_fn, emit
        self.active = []
        self.lock = threading.Lock()
        self.running = False
        self.launched = 0

    # --- the closed loop: operator actions from the mesh
    def on_action(self, topic, action):
        tid = str(action.get('target_id', ''))
        act = action.get('action')
        if act in ('engage', 'weapons_free', 'intercept'):
            with self.lock:
                for t in self.active:
                    if t.track_id == tid and not t.outcome and not t.engagement:
                        t.intercept_at = None
                        if action.get('shooter'):                     # coalition workflow: the operator named the US unit that shoots
                            t.shooter_override = str(action['shooter'])
                        self._fire(t, 'operator', action.get('operator'))
        elif act == 'partner_engaged':                                # the partner's own defences engage (the operator recorded it)
            with self.lock:
                for t in self.active:
                    if t.track_id == tid and not t.outcome and not t.engagement:
                        t.intercept_at = None
                        self._fire(t, 'partner', action.get('operator'))

    def _fire(self, t, mode, operator=None):
        """Launch one interceptor: from the asset under attack (its own defences), or from the US unit the
        operator named (coalition workflow). Caller holds the lock."""
        shooter_asset = t.target
        override = getattr(t, 'shooter_override', None)
        if override:
            shooter_asset = next((x for x in self.targets_fn() if x['id'].lower() == override.lower()), t.target)
        table = INTERCEPTORS_SHIP if shooter_asset['type'] in ('VESSEL', 'CARRIER') else INTERCEPTORS_LAND
        dist = nm(shooter_asset['lat'], shooter_asset['lon'], t.lat, t.lon)
        reach = [x for x in table[t.kind] if x[2] * 1.4 >= dist]                 # can shoot now
        sams = [x for x in reach if not is_cap(x)]
        specs = sams or [x for x in reach if is_cap(x)] or [max(table[t.kind], key=lambda x: x[2])]   # SAM first, CAP beyond SAM range, else stand by
        spec = random.choice(specs)
        shooter = {'id': shooter_asset['id'], 'name': shooter_asset['name'], 'lat': shooter_asset['lat'], 'lon': shooter_asset['lon'], 'country': shooter_asset.get('country', 'US')}
        t.shots += 1
        t.engaged_mode = t.engaged_mode or mode
        t.engaged_by = spec[0] + ({'operator': ' (operator engage)', 'partner': ' (partner self-defense)'}.get(mode, ' (auto, self-defense)')) + (f" from {shooter['name']}" if override else '')
        t.engagement = Interceptor(t, spec, shooter, mode, operator)
        if t.engagement.phase == 'LAUNCH':
            self.emit('interceptor-event', {**t.engagement.record(), 'event': 'launch'})
        self.emit('interceptor', t.engagement.record())
        print(f"[IAMD] {spec[0]} {'away from' if t.engagement.phase == 'LAUNCH' else 'assigned, standing by at'} {shooter['name']} at {t.track_id} ({t.weapon}) · shot {t.shots} · {mode}" + (f" by {operator}" if operator else "") + f" · {dist:.0f} nm")

    @staticmethod
    def in_reach(site, x):
        """Geography, not just range: the Houthis fight in the Red Sea, Bab el-Mandeb, the Gulf of Aden and
        southern Saudi Arabia; Iran's sites cover the Gulf, the Gulf of Oman, the northern Arabian Sea, Iraq,
        the GCC states and the Levant. A Houthi drone at a destroyer in the Strait of Hormuz is not a scenario."""
        if site['country'] == 'YE':
            return x['lon'] < 48.5 and x['lat'] < 30.5
        if site['country'] == 'IR':
            return (x['lon'] > 40.0 and x['lat'] > 19.0) or (x['lat'] > 30.0 and x['lon'] > 32.0)
        return True

    def _pick_target(self, site, rng_km):
        cands = [x for x in self.targets_fn() if self.in_reach(site, x) and nm(site['lat'], site['lon'], x['lat'], x['lon']) * 1.852 < rng_km]
        return random.choice(cands) if cands else None

    def _launch(self, kind, target_id=None, n=None, site_id=None, want_kind=None, want_class=None):
        """A launch. Random site/weapon/target by default; the exercise injector may fix the target, the
        wave size, the launch site or the target kind ('ship' / 'base')."""
        sites = [s for s in LAUNCH_SITES if ('UAS' in s['weapons'] if kind == 'uas' else any(w != 'UAS' for w in s['weapons']))]
        if site_id:
            sites = [s for s in sites if s['id'].lower() == site_id.lower()] or sites
        target = None
        for _ in range(40):                     # a site/weapon pair with something in range
            site = random.choice(sites)
            wtypes = [w for w in site['weapons'] if w != 'UAS'] if kind != 'uas' else ['UAS']
            if want_class:                                              # 'ballistic' -> SRBM/MRBM/ASBM, 'cruise' -> CM
                wtypes = [w for w in wtypes if (w == 'CM') == (want_class == 'cruise')] or wtypes
            wtype = random.choice(wtypes)
            weapon = random.choice(WEAPONS[wtype])
            if target_id or want_kind:
                def fits(x):
                    if want_kind == 'coalition': return bool(x.get('coalition'))
                    if want_kind == 'us': return not x.get('coalition')
                    if want_kind in ('ship', 'base'): return (x['type'] in ('VESSEL', 'CARRIER')) == (want_kind == 'ship') and not x.get('coalition')
                    return True
                cands = [x for x in self.targets_fn() if (not target_id or x['id'].lower() == target_id.lower()) and fits(x)
                         and self.in_reach(site, x) and nm(site['lat'], site['lon'], x['lat'], x['lon']) * 1.852 < weapon[3]]
                target = random.choice(cands) if cands else None
            else:
                target = self._pick_target(site, weapon[3])
            if target:
                # a shot nobody can react to is not worth simulating: keep looking for a pair with a watchable flight
                tti = nm(site['lat'], site['lon'], target['lat'], target['lon']) / weapon[2] * 3600 / TIME_SCALE[weapon[0]]
                if tti >= (75 if kind == 'uas' else 40) or _ > 30:
                    break
                target = None
        if not target:
            print(f"[MSL] no launch site has {target_id or want_kind or 'a target'} in range")
            return None
        n = n or (random.randint(2, 3) if kind == 'uas' else 1)
        wave = f"WAVE-{uuid.uuid4().hex[:4].upper()}" if n > 1 else None
        for i in range(n):
            t = Threat(kind, site, weapon, target, wave, n)
            t.lat += random.uniform(-0.05, 0.05); t.lon += random.uniform(-0.05, 0.05); t.launch_lat, t.launch_lon = t.lat, t.lon
            with self.lock:
                self.active.append(t)
            self.launched += 1
            # the lead airframe of a wave is the launch everyone acts on; the rest are launch-member (counted, not alerted)
            self.emit(kind + '-event', {**t.record(), 'event': 'launch' if i == 0 else 'launch-member'})
            print(f"[{'MSL' if kind == 'missile' else 'UAS'}] LAUNCH {t.weapon} ({t.weapon_class}) {t.track_id} from {site['name']} -> {target['name']} [{target.get('country', 'US')}{' coalition' if target.get('coalition') else ''}] · {t.total_nm:.0f} nm · TTI {t.tti_s()} s" + (f" · wave {wave} {n}x" if wave else ""))
        return target

    # --- exercise control: scripts/inject.py publishes cop/c2/action/v1/{aor}/exercise/{scenario}/inject
    def inject(self, action):
        scenario = str(action.get('scenario', '')).lower()
        target = action.get('target_id'); n = action.get('n'); site = action.get('site_id')
        print(f"[EXERCISE] inject {scenario} target={target or '-'} n={n or '-'} site={site or '-'} by {action.get('operator')}")
        if scenario == 'missile':
            self._launch('missile', target_id=target, site_id=site, want_kind=None if target else action.get('target_kind'), want_class=action.get('weapon_class'))
        elif scenario == 'uas':
            self._launch('uas', target_id=target, n=int(n) if n else None, site_id=site, want_kind=None if target else action.get('target_kind'))
        elif scenario == 'coalition-attack':            # the coalition workflow on cue: a wave or a missile at a partner asset
            kind = 'uas' if random.random() < 0.6 else 'missile'
            self._launch(kind, target_id=target, n=int(n) if n else None, want_kind=None if target else 'coalition')
        elif scenario == 'ship-attack':                 # the naval workflows on cue: a drone wave at a ship, then a missile at it
            tgt = self._launch('uas', target_id=target, n=int(n) if n else 3, want_kind='ship')
            threading.Timer(25.0, lambda: self._launch('missile', target_id=(tgt or {}).get('id') or target, want_kind='ship')).start()
        elif scenario == 'site-active':                 # three quick launches from one site -> launch-site alert -> targeting agent
            sid = site or random.choice([s['id'] for s in LAUNCH_SITES if any(w != 'UAS' for w in s['weapons'])])
            for i in range(3):
                threading.Timer(i * 20.0, lambda: self._launch('missile', site_id=sid, target_id=target)).start()
        else:
            print(f"[EXERCISE] unknown scenario '{scenario}' (missile | uas | ship-attack | coalition-attack | site-active)")

    # Tempo: one event every 1-2 minutes -- a single missile, or a drone wave of 2-3, occasionally both --
    # busy enough to keep a demo moving, with a gap so each engagement can be watched end to end.
    EVENT_EVERY_S = (60, 120)

    def _launch_loop(self):
        time.sleep(random.uniform(30, 60))
        while self.running:
            r = random.random()
            if r < 0.45:
                self._launch('missile')
            elif r < 0.85:
                self._launch('uas')
            else:
                self._launch('missile'); time.sleep(random.uniform(10, 20)); self._launch('uas')
            time.sleep(random.uniform(*self.EVENT_EVERY_S))

    def _fly_loop(self):
        last = time.time()
        while self.running:
            time.sleep(1.0)
            now = time.time(); dt = now - last; last = now
            with self.lock:
                live = list(self.active)
            for t in live:
                if t.outcome:
                    continue
                prev = t.phase
                t.advance(dt)
                f = t.flown_nm / t.total_nm if t.total_nm else 1
                with self.lock:
                    if t.intercept_at is not None and not t.engagement and not t.outcome and f >= t.intercept_at:
                        self._fire(t, 'auto')                                  # the defender's own fire control
                    i = t.engagement
                if i and not t.outcome:
                    res = i.advance(dt)
                    irec = i.record()
                    if res == 'LAUNCH':
                        self.emit('interceptor-event', {**irec, 'event': 'launch'}); self.emit('interceptor', irec)
                        print(f"[IAMD] {i.name} away from {i.shooter['name']} at {t.track_id} · in range")
                    elif res == 'HIT':
                        t.outcome = 'INTERCEPTED'; t.killed_at = (round(t.lat, 6), round(t.lon, 6))
                        self.emit('interceptor-event', {**irec, 'event': 'hit'}); self.emit('interceptor', irec)
                    elif res == 'MISS':
                        self.emit('interceptor-event', {**irec, 'event': 'miss'}); self.emit('interceptor', irec)
                        print(f"[IAMD] MISS {i.name} on {t.track_id} · shot {t.shots}")
                        with self.lock:
                            t.engagement = None
                            if f < 0.93 and t.shots < 3:
                                self._fire(t, i.mode, i.operator)                  # shoot-look-shoot
                    else:
                        self.emit('interceptor', irec)
                rec = t.record()
                if t.outcome:
                    self.emit(t.kind + '-event', {**rec, 'event': t.outcome.lower()})
                    print(f"[{'MSL' if t.kind == 'missile' else 'UAS'}] {t.outcome} {t.track_id} {t.weapon} -> {t.target['name']}" + (f" by {t.engaged_by}" if t.engaged_by else ""))
                    threading.Timer(20.0, self._retire, args=(t,)).start()
                else:
                    if t.phase == 'TERMINAL' and prev != 'TERMINAL':
                        self.emit(t.kind + '-event', {**rec, 'event': 'terminal'})
                    self.emit(t.kind, rec)

    def _retire(self, t):
        with self.lock:
            if t in self.active:
                self.active.remove(t)

    def start(self):
        self.running = True
        # Launches are staged from the exercise page (map/exercise.html) or scripts/inject.py. The random tempo is
        # off unless MISSILE_AUTO=1, so a demo is not interrupted by a salvo nobody asked for.
        loops = (self._fly_loop,) + ((self._launch_loop,) if os.environ.get('MISSILE_AUTO', '0') == '1' else ())
        for fn in loops:
            threading.Thread(target=fn, daemon=True).start()
        print("[MSL] launches: " + ("random tempo (MISSILE_AUTO=1)" if len(loops) > 1 else "on demand only -- map/exercise.html or scripts/inject.py"))

    def stats(self):
        with self.lock:
            inflight = [t for t in self.active if not t.outcome]
        return f"missiles/uas in flight={len(inflight)} launched={self.launched}"

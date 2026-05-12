"""Generate simulated PVE game event logs.

The script is intentionally command driven: data is generated only when the
user runs this file, and the requested scale/date/output path are controlled by
CLI arguments.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - kept for light local environments.
    yaml = None


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"


RARITY_WEIGHT = {
    "common": 1.00,
    "uncommon": 0.78,
    "rare": 0.52,
    "epic": 0.28,
    "legendary": 0.14,
}


@dataclass
class PlayerState:
    player_id: str
    hero: dict[str, str]
    x: float
    y: float
    attrs: dict[str, float]
    weapons: dict[str, dict[str, float]] = field(default_factory=dict)
    cards: list[str] = field(default_factory=list)
    tags: Counter[str] = field(default_factory=Counter)
    hp: float = 1.0
    shield: float = 0.0
    alive: bool = True
    death_wave: int | None = None
    survival_wave: int = 0


class EventWriter:
    def __init__(self) -> None:
        self.events_by_date: dict[str, list[dict[str, Any]]] = {}
        self.count = 0

    def add(self, event: dict[str, Any]) -> None:
        dt = str(event["dt"])
        self.events_by_date.setdefault(dt, []).append(event)
        self.count += 1


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def read_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:
        return parse_simple_yaml(path)
    with path.open(encoding="utf-8") as file:
        data = yaml.safe_load(file)
    return data or {}


def parse_simple_yaml(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = {}
    with path.open(encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            value = value.strip().strip('"')
            if value.isdigit():
                data[key.strip()] = int(value)
            else:
                try:
                    data[key.strip()] = float(value)
                except ValueError:
                    data[key.strip()] = value
    return data


def as_float(value: str | None, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    return float(value)


def as_int(value: str | None, default: int = 0) -> int:
    if value is None or value == "":
        return default
    return int(float(value))


def split_tags(value: str | None) -> list[str]:
    if not value:
        return []
    return [part for part in value.split(";") if part]


def parse_affinity(value: str | None) -> dict[str, float]:
    result: dict[str, float] = {}
    if not value:
        return result
    for part in value.split(";"):
        if not part or ":" not in part:
            continue
        key, raw = part.split(":", 1)
        result[key] = float(raw)
    return result


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def event_time(base: datetime, wave: int, offset_seconds: int = 0) -> str:
    return (base + timedelta(seconds=wave * 45 + offset_seconds)).isoformat(timespec="seconds")


def choose_weighted(items: list[Any], weights: list[float], rng: random.Random) -> Any:
    total = sum(max(0.0, weight) for weight in weights)
    if total <= 0:
        return rng.choice(items)
    pick = rng.random() * total
    running = 0.0
    for item, weight in zip(items, weights):
        running += max(0.0, weight)
        if pick <= running:
            return item
    return items[-1]


def weighted_sample_without_replacement(
    items: list[Any], weights: list[float], count: int, rng: random.Random
) -> list[Any]:
    pool = list(zip(items, weights))
    selected: list[Any] = []
    for _ in range(min(count, len(pool))):
        choice = choose_weighted([item for item, _ in pool], [weight for _, weight in pool], rng)
        selected.append(choice)
        pool = [(item, weight) for item, weight in pool if item is not choice]
    return selected


def row_by_id(rows: list[dict[str, str]], key: str) -> dict[str, dict[str, str]]:
    return {row[key]: row for row in rows}


def create_player(player_id: str, hero: dict[str, str], rng: random.Random) -> PlayerState:
    preferred_tags = split_tags(hero["preferred_tags"])
    attrs = {
        "power": as_float(hero["base_power"], 1.0),
        "defense": as_float(hero["base_defense"], 1.0),
        "luck": as_float(hero["base_luck"], 1.0),
        "move_speed": as_float(hero["base_move_speed"], 1.0),
        "heal_power": as_float(hero["base_heal_power"], 0.0),
        "survival_factor": as_float(hero["survival_factor"], 1.0),
    }
    x = clamp(500 + rng.gauss(0, 120), 0, 999)
    y = clamp(500 + rng.gauss(0, 120), 0, 999)
    return PlayerState(
        player_id=player_id,
        hero=hero,
        x=x,
        y=y,
        attrs=attrs,
        tags=Counter(preferred_tags),
    )


def init_weapon(weapon: dict[str, str]) -> dict[str, float]:
    return {
        "level": 1.0,
        "damage": as_float(weapon["base_damage"]),
        "cooldown": as_float(weapon["base_cooldown"]),
        "attack_count": as_float(weapon["base_attack_count"]),
        "frequency": as_float(weapon["base_frequency"]),
        "range": as_float(weapon["base_range"]),
        "aoe_factor": as_float(weapon["aoe_factor"]),
    }


def point_in_zone(x: float, y: float, zone: dict[str, str]) -> bool:
    return (
        as_float(zone["x_min"]) <= x <= as_float(zone["x_max"])
        and as_float(zone["y_min"]) <= y <= as_float(zone["y_max"])
    )


def zone_at(x: float, y: float, zones: list[dict[str, str]]) -> dict[str, str] | None:
    for zone in zones:
        if point_in_zone(x, y, zone):
            return zone
    return None


def random_spawn(zones: list[dict[str, str]], rng: random.Random) -> tuple[float, float, dict[str, str] | None]:
    if zones and rng.random() < 0.65:
        zone = rng.choice(zones)
        x = rng.uniform(as_float(zone["x_min"]), as_float(zone["x_max"]))
        y = rng.uniform(as_float(zone["y_min"]), as_float(zone["y_max"]))
        return x, y, zone
    return rng.uniform(0, 999), rng.uniform(0, 999), None


def move_player(player: PlayerState, zones: list[dict[str, str]], rng: random.Random) -> None:
    speed = player.attrs["move_speed"]
    if zones and rng.random() < 0.22:
        zone = rng.choice(zones)
        target_x = rng.uniform(as_float(zone["x_min"]), as_float(zone["x_max"]))
        target_y = rng.uniform(as_float(zone["y_min"]), as_float(zone["y_max"]))
        player.x += (target_x - player.x) * rng.uniform(0.04, 0.10) * speed
        player.y += (target_y - player.y) * rng.uniform(0.04, 0.10) * speed
    else:
        player.x += rng.gauss(0, 26) * speed
        player.y += rng.gauss(0, 26) * speed
    player.x = clamp(player.x, 0, 999)
    player.y = clamp(player.y, 0, 999)


def card_is_available(card: dict[str, str], player: PlayerState, wave: int) -> bool:
    min_wave = as_int(card["min_wave"], 1)
    max_wave = as_int(card["max_wave"], 0)
    if wave < min_wave:
        return False
    if max_wave and wave > max_wave:
        return False
    prerequisite = card.get("prerequisite", "")
    if prerequisite.startswith("has_weapon:"):
        weapon_id = prerequisite.split(":", 1)[1]
        return weapon_id in player.weapons
    return True


def card_weight(card: dict[str, str], player: PlayerState, wave: int) -> float:
    if not card_is_available(card, player, wave):
        return 0.0

    weight = as_float(card["base_weight"], 1.0)
    weight *= RARITY_WEIGHT.get(card["rarity"], 1.0)
    weight *= 1.0 + min(0.35, max(0.0, player.attrs["luck"] - 1.0) * 0.28)

    if wave >= 35 and card["card_type"] == "global_affix":
        weight *= 1.6
    elif card["card_type"] == "global_affix":
        weight *= 0.15

    affinity = parse_affinity(card["hero_affinity"])
    weight *= affinity.get(player.hero["hero_id"], affinity.get("default", 1.0))

    card_tags = split_tags(card["build_tags"])
    synergy_hits = sum(player.tags.get(tag, 0) for tag in card_tags)
    weight *= 1.0 + min(0.75, synergy_hits * 0.12)
    return max(0.0, weight)


def card_pick_score(card: dict[str, str], player: PlayerState, wave: int, rng: random.Random) -> float:
    tags = split_tags(card["build_tags"])
    score = as_float(card["effect_value"], 0.05) * 100
    score += sum(player.tags.get(tag, 0) for tag in tags) * 5.0

    if card["card_type"] == "new_weapon" and card["target_weapon"] not in player.weapons:
        score += 18.0
    if card["card_type"] == "weapon_upgrade":
        score += 10.0 + len(player.weapons) * 1.5
    if card["card_type"] == "instant_effect" and player.hp < 0.55:
        score += 25.0
    if card["card_type"] == "global_affix":
        score += max(0, wave - 35) * 1.2
        score -= abs(as_float(card["negative_value"], 0.0)) * 38
        if player.hp < 0.65:
            score -= 10

    affinity = parse_affinity(card["hero_affinity"])
    score *= affinity.get(player.hero["hero_id"], affinity.get("default", 1.0))
    score += rng.gauss(0, 4.5)
    return score


def apply_card(card: dict[str, str], player: PlayerState, weapon_map: dict[str, dict[str, str]]) -> None:
    player.cards.append(card["card_id"])
    for tag in split_tags(card["build_tags"]):
        player.tags[tag] += 1

    effect = as_float(card["effect_value"], 0.0)
    card_type = card["card_type"]
    target_attr = card["target_attr"]

    if card_type == "permanent_buff" and target_attr:
        player.attrs[target_attr] = player.attrs.get(target_attr, 1.0) + effect
    elif card_type == "instant_effect":
        if target_attr == "hp":
            player.hp = clamp(player.hp + effect, 0, 1.25)
        elif target_attr == "shield":
            player.shield += effect
        elif target_attr:
            player.attrs[target_attr] = player.attrs.get(target_attr, 1.0) + effect * 0.45
    elif card_type == "new_weapon":
        weapon_id = card["target_weapon"]
        if weapon_id and weapon_id not in player.weapons and weapon_id in weapon_map:
            player.weapons[weapon_id] = init_weapon(weapon_map[weapon_id])
            for tag in split_tags(weapon_map[weapon_id]["weapon_tags"]):
                player.tags[tag] += 1
    elif card_type == "weapon_upgrade":
        weapon_id = card["target_weapon"]
        upgrade_attr = card["upgrade_attr"]
        if weapon_id in player.weapons and upgrade_attr:
            weapon = player.weapons[weapon_id]
            if upgrade_attr == "cooldown":
                weapon[upgrade_attr] = max(0.3, weapon[upgrade_attr] * (1.0 - effect))
            else:
                weapon[upgrade_attr] = weapon.get(upgrade_attr, 0.0) * (1.0 + effect)
            weapon["level"] += 1
    elif card_type == "global_affix":
        if target_attr:
            player.attrs[target_attr] = player.attrs.get(target_attr, 1.0) + effect
        negative_attr = card["negative_attr"]
        negative_value = as_float(card["negative_value"], 0.0)
        if negative_attr:
            if negative_attr == "hp":
                player.hp = clamp(player.hp + negative_value, 0.15, 1.25)
            else:
                player.attrs[negative_attr] = max(0.2, player.attrs.get(negative_attr, 1.0) + negative_value)


def weapon_damage(player: PlayerState) -> float:
    if not player.weapons:
        return 35 * player.attrs["power"]
    total = 0.0
    for weapon in player.weapons.values():
        cooldown_factor = 1.0 / max(0.3, weapon["cooldown"])
        total += (
            weapon["damage"]
            * weapon["attack_count"]
            * weapon["frequency"]
            * weapon["aoe_factor"]
            * cooldown_factor
        )
    return total * player.attrs["power"]


def maybe_level_up(wave: int) -> bool:
    return wave in {1, 2, 3, 5, 8, 12, 16, 22, 28, 35, 42, 50, 60, 70}


def add_event(
    writer: EventWriter,
    event_type: str,
    base: datetime,
    dt: str,
    match_id: str,
    room_id: str,
    map_id: str,
    wave: int,
    seq: int,
    player: PlayerState | None = None,
    x: float | None = None,
    y: float | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    writer.add(
        {
            "event_id": f"{match_id}_{seq:08d}_{event_type}",
            "event_type": event_type,
            "event_time": event_time(base, wave, seq % 37),
            "dt": dt,
            "match_id": match_id,
            "room_id": room_id,
            "map_id": map_id,
            "player_id": player.player_id if player else "",
            "hero_id": player.hero["hero_id"] if player else "",
            "wave": wave,
            "x": round(x if x is not None else (player.x if player else 0.0), 3),
            "y": round(y if y is not None else (player.y if player else 0.0), 3),
            "extra": json.dumps(extra or {}, ensure_ascii=False, separators=(",", ":")),
        }
    )


def simulate_match(
    match_index: int,
    dt: str,
    player_pool: list[str],
    configs: dict[str, Any],
    rng: random.Random,
    writer: EventWriter,
) -> None:
    heroes = configs["heroes"]
    cards = configs["cards"]
    weapons = configs["weapons"]
    weapon_map = configs["weapon_map"]
    map_zones = configs["map_zones"]
    sim = configs["simulation"]

    map_id = map_zones[0]["map_id"]
    team_size = rng.randint(int(sim["team_size_min"]), int(sim["team_size_max"]))
    selected_players = rng.sample(player_pool, team_size)
    selected_heroes = [choose_weighted(heroes, [1.0] * len(heroes), rng) for _ in selected_players]
    match_id = f"match_{dt.replace('-', '')}_{match_index:06d}"
    room_id = f"room_{match_index % 128:03d}"
    base = datetime.fromisoformat(dt) + timedelta(seconds=rng.randint(0, 20 * 3600))
    seq = 0

    add_event(
        writer,
        "battle_start",
        base,
        dt,
        match_id,
        room_id,
        map_id,
        0,
        seq,
        extra={"team_size": team_size, "target_wave": int(sim["target_wave"])},
    )
    seq += 1

    players = [create_player(player_id, hero, rng) for player_id, hero in zip(selected_players, selected_heroes)]
    starter_weapons = [card for card in cards if card["card_type"] == "new_weapon" and as_int(card["min_wave"], 1) <= 1]

    for player in players:
        add_event(writer, "player_join", base, dt, match_id, room_id, map_id, 0, seq, player=player)
        seq += 1
        starter = choose_weighted(
            starter_weapons,
            [card_weight(card, player, 1) for card in starter_weapons],
            rng,
        )
        apply_card(starter, player, weapon_map)
        add_event(
            writer,
            "card_pick",
            base,
            dt,
            match_id,
            room_id,
            map_id,
            0,
            seq,
            player=player,
            extra={"card_id": starter["card_id"], "reason": "starter_weapon"},
        )
        seq += 1

    final_wave = 0
    max_wave = int(sim["max_wave"])
    for wave in range(1, max_wave + 1):
        alive_players = [player for player in players if player.alive]
        if not alive_players:
            break
        final_wave = wave

        add_event(writer, "wave_start", base, dt, match_id, room_id, map_id, wave, seq, extra={"alive": len(alive_players)})
        seq += 1

        spawn_x, spawn_y, spawn_zone = random_spawn(map_zones, rng)
        density = as_float(spawn_zone["spawn_density_multiplier"], 1.0) if spawn_zone else 1.0
        enemy_count = int((team_size * (8 + wave * 2.2)) * density * rng.uniform(0.85, 1.18))
        add_event(
            writer,
            "enemy_spawn",
            base,
            dt,
            match_id,
            room_id,
            map_id,
            wave,
            seq,
            x=spawn_x,
            y=spawn_y,
            extra={"enemy_count": enemy_count, "danger_zone": spawn_zone["danger_zone_id"] if spawn_zone else ""},
        )
        seq += 1

        for player in alive_players:
            move_player(player, map_zones, rng)
            add_event(writer, "position_tick", base, dt, match_id, room_id, map_id, wave, seq, player=player)
            seq += 1

            if maybe_level_up(wave):
                available_cards = [card for card in cards if card_is_available(card, player, wave)]
                weights = [card_weight(card, player, wave) for card in available_cards]
                choices = weighted_sample_without_replacement(available_cards, weights, 3, rng)
                choice_ids = [card["card_id"] for card in choices]
                add_event(
                    writer,
                    "card_choice",
                    base,
                    dt,
                    match_id,
                    room_id,
                    map_id,
                    wave,
                    seq,
                    player=player,
                    extra={"candidate_card_ids": choice_ids},
                )
                seq += 1
                picked = max(choices, key=lambda card: card_pick_score(card, player, wave, rng))
                apply_card(picked, player, weapon_map)
                add_event(
                    writer,
                    "card_pick",
                    base,
                    dt,
                    match_id,
                    room_id,
                    map_id,
                    wave,
                    seq,
                    player=player,
                    extra={
                        "card_id": picked["card_id"],
                        "card_type": picked["card_type"],
                        "target_weapon": picked["target_weapon"],
                        "build_tags": picked["build_tags"],
                    },
                )
                seq += 1

            zone = zone_at(player.x, player.y, map_zones)
            risk = as_float(zone["death_risk_multiplier"], 1.0) if zone else 1.0
            damage = weapon_damage(player) * rng.uniform(0.82, 1.22)
            kill_count = max(0, int(damage / (72 + wave * 7.5) * rng.uniform(0.85, 1.15)))
            heal_done = max(0.0, player.attrs["heal_power"] * (10 + wave * 1.5) * rng.uniform(0.7, 1.25))
            incoming = (0.020 + wave * 0.0048) * risk * rng.uniform(0.75, 1.25)
            mitigation = player.attrs["defense"] * player.attrs["survival_factor"]
            damage_taken = max(0.0, incoming / max(0.35, mitigation))
            absorbed = min(player.shield, damage_taken)
            player.shield -= absorbed
            player.hp = clamp(player.hp - (damage_taken - absorbed) + heal_done / 260.0, 0.0, 1.25)
            player.survival_wave = wave

            add_event(
                writer,
                "player_wave_stat",
                base,
                dt,
                match_id,
                room_id,
                map_id,
                wave,
                seq,
                player=player,
                extra={
                    "damage_dealt": round(damage, 3),
                    "kill_count": kill_count,
                    "heal_done": round(heal_done, 3),
                    "damage_taken": round(damage_taken * 100, 3),
                    "weapon_count": len(player.weapons),
                    "card_count": len(player.cards),
                    "hp_after_wave": round(player.hp, 4),
                },
            )
            seq += 1

            if kill_count:
                add_event(
                    writer,
                    "enemy_kill",
                    base,
                    dt,
                    match_id,
                    room_id,
                    map_id,
                    wave,
                    seq,
                    player=player,
                    extra={"kill_count": kill_count},
                )
                seq += 1

            death_roll = rng.random()
            death_threshold = 0.018 + wave * 0.0018
            if player.hp <= 0.05 or (death_roll < death_threshold * risk / max(0.6, player.attrs["survival_factor"])):
                player.alive = False
                player.death_wave = wave
                add_event(
                    writer,
                    "player_death",
                    base,
                    dt,
                    match_id,
                    room_id,
                    map_id,
                    wave,
                    seq,
                    player=player,
                    extra={"death_wave": wave, "hp": round(player.hp, 4)},
                )
                seq += 1

    add_event(
        writer,
        "battle_end",
        base,
        dt,
        match_id,
        room_id,
        map_id,
        final_wave,
        seq,
        extra={
            "final_wave": final_wave,
            "success_50": final_wave >= int(sim["target_wave"]),
            "team_size": team_size,
            "survivor_count": sum(1 for player in players if player.alive),
            "team_heroes": [player.hero["hero_id"] for player in players],
        },
    )


def write_events(writer: EventWriter, output_dir: Path, overwrite: bool, append: bool) -> list[Path]:
    output_paths: list[Path] = []
    for dt, events in sorted(writer.events_by_date.items()):
        partition = output_dir / f"dt={dt}"
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / "events.jsonl"
        if path.exists() and not overwrite and not append:
            raise FileExistsError(f"{path} already exists. Use --overwrite or --append.")
        mode = "a" if append and not overwrite else "w"
        with path.open(mode, encoding="utf-8", newline="\n") as file:
            for event in events:
                file.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
        output_paths.append(path)
    return output_paths


def build_configs(args: argparse.Namespace) -> dict[str, Any]:
    simulation = read_yaml(CONFIG_DIR / "simulation_config.yaml")
    simulation["match_count"] = args.matches if args.matches is not None else int(simulation["match_count"])
    simulation["player_pool_size"] = args.players if args.players is not None else int(simulation["player_pool_size"])
    simulation["start_date"] = args.start_date or str(simulation["start_date"])
    simulation["days"] = args.days if args.days is not None else int(simulation["days"])
    simulation["seed"] = args.seed if args.seed is not None else int(simulation["seed"])
    simulation["output_path"] = args.output_dir or str(simulation["output_path"])

    heroes = read_csv(CONFIG_DIR / "hero_config.csv")
    cards = read_csv(CONFIG_DIR / "card_config.csv")
    weapons = read_csv(CONFIG_DIR / "weapon_config.csv")
    map_zones = read_csv(CONFIG_DIR / "map_config.csv")

    return {
        "simulation": simulation,
        "heroes": heroes,
        "cards": cards,
        "weapons": weapons,
        "weapon_map": row_by_id(weapons, "weapon_id"),
        "map_zones": map_zones,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate simulated PVE balance event logs.")
    parser.add_argument("--matches", type=int, help="Number of matches to generate.")
    parser.add_argument("--players", type=int, help="Size of the simulated player pool.")
    parser.add_argument("--start-date", help="First generated date, in YYYY-MM-DD format.")
    parser.add_argument("--days", type=int, help="Number of date partitions to generate.")
    parser.add_argument("--seed", type=int, help="Random seed for reproducible output.")
    parser.add_argument("--output-dir", help="Output root directory for dt=YYYY-MM-DD partitions.")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing output files.")
    parser.add_argument("--append", action="store_true", help="Append to existing output files.")
    parser.add_argument("--dry-run", action="store_true", help="Print the generation plan without writing files.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace, configs: dict[str, Any]) -> None:
    simulation = configs["simulation"]
    if args.overwrite and args.append:
        raise ValueError("--overwrite and --append cannot be used together.")
    if int(simulation["match_count"]) <= 0:
        raise ValueError("match_count must be positive.")
    if int(simulation["player_pool_size"]) < int(simulation["team_size_max"]):
        raise ValueError("player_pool_size must be at least team_size_max.")
    if int(simulation["days"]) <= 0:
        raise ValueError("days must be positive.")
    datetime.fromisoformat(str(simulation["start_date"]))


def main() -> None:
    args = parse_args()
    configs = build_configs(args)
    validate_args(args, configs)

    simulation = configs["simulation"]
    output_dir = ROOT / str(simulation["output_path"])
    match_count = int(simulation["match_count"])
    days = int(simulation["days"])
    start_date = datetime.fromisoformat(str(simulation["start_date"])).date()

    print("Generation plan")
    print(f"  matches: {match_count}")
    print(f"  players: {simulation['player_pool_size']}")
    print(f"  start_date: {start_date.isoformat()}")
    print(f"  days: {days}")
    print(f"  seed: {simulation['seed']}")
    print(f"  output_dir: {output_dir}")
    if args.dry_run:
        return

    rng = random.Random(int(simulation["seed"]))
    player_pool = [f"player_{index:05d}" for index in range(1, int(simulation["player_pool_size"]) + 1)]
    writer = EventWriter()

    for match_index in range(1, match_count + 1):
        dt = (start_date + timedelta(days=(match_index - 1) % days)).isoformat()
        simulate_match(match_index, dt, player_pool, configs, rng, writer)

    output_paths = write_events(writer, output_dir, args.overwrite, args.append)
    print(f"Generated events: {writer.count}")
    for path in output_paths:
        print(f"  wrote: {path}")


if __name__ == "__main__":
    main()

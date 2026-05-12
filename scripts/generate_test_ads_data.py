"""Generate sample ADS data for Streamlit visualization testing."""

import numpy as np
import pandas as pd
from pathlib import Path


def generate_hero_balance_report(output_path: Path) -> None:
    """生成职业平衡分析测试数据。"""
    heroes = [
        ("warrior", "Warrior", "tank"),
        ("guardian", "Guardian", "tank"),
        ("berserker", "Berserker", "dps"),
        ("mage", "Mage", "dps"),
        ("archmage", "Archmage", "dps"),
        ("ranger", "Ranger", "dps"),
        ("assassin", "Assassin", "dps"),
        ("engineer", "Engineer", "support"),
        ("summoner", "Summoner", "support"),
        ("cleric", "Cleric", "healer"),
        ("oracle", "Oracle", "healer"),
        ("alchemist", "Alchemist", "support"),
    ]

    np.random.seed(42)
    rows = []
    for hero_id, hero_name, role_type in heroes:
        use_count = np.random.randint(80, 200)

        if role_type == "tank":
            survival_wave = np.random.uniform(35, 45)
            kill_per_wave = np.random.uniform(0.8, 1.2)
            success_rate = np.random.uniform(0.35, 0.50)
            high_perf_rate = np.random.uniform(0.18, 0.28)
        elif role_type == "dps":
            survival_wave = np.random.uniform(28, 38)
            kill_per_wave = np.random.uniform(1.2, 1.8)
            success_rate = np.random.uniform(0.30, 0.48)
            high_perf_rate = np.random.uniform(0.20, 0.35)
        elif role_type == "healer":
            survival_wave = np.random.uniform(32, 42)
            kill_per_wave = np.random.uniform(0.5, 1.0)
            success_rate = np.random.uniform(0.40, 0.55)
            high_perf_rate = np.random.uniform(0.22, 0.32)
        else:  # support
            survival_wave = np.random.uniform(30, 40)
            kill_per_wave = np.random.uniform(0.7, 1.1)
            success_rate = np.random.uniform(0.35, 0.50)
            high_perf_rate = np.random.uniform(0.20, 0.30)

        perf_score = 0.35 * np.random.uniform(0.3, 0.8) + 0.25 * np.random.uniform(0.3, 0.8) + \
                    0.20 * np.random.uniform(0.3, 0.8) + 0.20 * np.random.uniform(0.3, 0.8)
        balance_score = 0.40 * success_rate + 0.25 * (survival_wave / 50.0) + \
                       0.20 * (kill_per_wave / 2.0) + 0.15 * high_perf_rate

        rows.append({
            "dt": "all",
            "hero_id": hero_id,
            "hero_name": hero_name,
            "role_type": role_type,
            "use_count": use_count,
            "avg_survival_wave": round(survival_wave, 4),
            "avg_kill_per_wave": round(kill_per_wave, 4),
            "success_50_rate": round(success_rate, 6),
            "high_performance_rate": round(high_perf_rate, 6),
            "avg_performance_score": round(perf_score, 6),
            "balance_score": round(balance_score, 6),
            "balance_rank": 0,
        })

    df = pd.DataFrame(rows)
    df["balance_rank"] = df["balance_score"].rank(ascending=False, method="dense").astype(int)

    output_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path / "hero_balance_report.csv", index=False)
    df.to_parquet(output_path / "hero_balance_report.parquet", index=False)
    print(f"✓ hero_balance_report: {len(df)} rows")


def generate_map_heatmap_report(output_path: Path) -> None:
    """生成地图热力分析测试数据。"""
    np.random.seed(42)
    rows = []

    for map_id in ["map_001", "map_002"]:
        grid_count = np.random.randint(30, 50)
        for _ in range(grid_count):
            grid_x = np.random.randint(0, 100)
            grid_y = np.random.randint(0, 100)
            death_count = np.random.randint(0, 50)
            enemy_count = np.random.randint(50, 300)
            stay_duration = np.random.uniform(10, 200)

            danger_score = 0.45 * (death_count / 50) + 0.25 * (stay_duration / 200) + \
                          0.30 * (enemy_count / 300)

            if danger_score >= 0.8:
                danger_level = "S"
            elif danger_score >= 0.6:
                danger_level = "A"
            elif danger_score >= 0.4:
                danger_level = "B"
            elif danger_score >= 0.2:
                danger_level = "C"
            else:
                danger_level = "D"

            rows.append({
                "dt": "all",
                "map_id": map_id,
                "display_grid_x": grid_x,
                "display_grid_y": grid_y,
                "death_count": death_count,
                "stay_duration": round(stay_duration, 4),
                "enemy_spawn_count": enemy_count,
                "position_tick_count": np.random.randint(100, 500),
                "unique_player_count": np.random.randint(5, 30),
                "danger_score": round(danger_score, 6),
                "danger_level": danger_level,
                "danger_rank": 0,
            })

    df = pd.DataFrame(rows)
    for map_id in df["map_id"].unique():
        df.loc[df["map_id"] == map_id, "danger_rank"] = \
            df[df["map_id"] == map_id]["danger_score"].rank(ascending=False, method="dense").astype(int)

    output_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path / "map_grid_heatmap_report.csv", index=False)
    df.to_parquet(output_path / "map_grid_heatmap_report.parquet", index=False)
    print(f"✓ map_grid_heatmap_report: {len(df)} rows")


def generate_card_strength_report(output_path: Path) -> None:
    """生成卡牌强度分析测试数据。"""
    card_types = ["permanent_buff", "instant_effect", "new_weapon", "weapon_upgrade", "global_affix"]
    rarities = ["common", "uncommon", "rare", "epic", "legendary"]

    np.random.seed(42)
    rows = []

    for i in range(30):
        card_id = f"card_{i:03d}"
        card_type = np.random.choice(card_types)
        rarity = np.random.choice(rarities)
        show_count = np.random.randint(100, 500)
        pick_count = np.random.randint(20, show_count)
        pick_rate = pick_count / show_count

        success_lift = np.random.uniform(-0.1, 0.2)
        wave_lift = np.random.uniform(-1, 5)

        strength_score = 0.35 * pick_rate + 0.40 * max(0, success_lift + 0.1) + \
                        0.25 * max(0, (wave_lift + 1) / 6)

        rows.append({
            "dt": "all",
            "card_id": card_id,
            "card_name": f"Card {i+1}",
            "card_type": card_type,
            "rarity": rarity,
            "target_attr": "power",
            "target_weapon": f"weapon_{i % 5}",
            "upgrade_attr": "damage",
            "effect_value": np.random.uniform(0.1, 0.5),
            "min_wave": np.random.randint(1, 10),
            "max_wave": np.random.randint(30, 50),
            "show_count": show_count,
            "pick_count": pick_count,
            "pick_rate": round(pick_rate, 6),
            "pick_player_count": np.random.randint(10, 100),
            "success_50_lift": round(success_lift, 6),
            "wave_lift": round(wave_lift, 6),
            "damage_lift": round(np.random.uniform(-10, 50), 4),
            "kill_lift": round(np.random.uniform(-0.5, 1.5), 4),
            "heal_lift": round(np.random.uniform(-5, 20), 4),
            "damage_taken_lift": round(np.random.uniform(-20, 30), 4),
            "strength_score": round(strength_score, 6),
            "is_overpowered_tag": False,
            "build_tags": "common",
        })

    df = pd.DataFrame(rows)
    output_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path / "card_strength_report.csv", index=False)
    df.to_parquet(output_path / "card_strength_report.parquet", index=False)
    print(f"✓ card_strength_report: {len(df)} rows")


def generate_weapon_growth_curve_report(output_path: Path) -> None:
    """生成武器成长曲线测试数据。"""
    np.random.seed(42)
    rows = []

    for i in range(10):
        base_value = np.random.uniform(50, 200)
        upgrade_effect = np.random.uniform(0.1, 0.3)

        rows.append({
            "dt": "all",
            "card_id": f"upgrade_card_{i:03d}",
            "card_name": f"Upgrade Card {i+1}",
            "target_weapon": f"weapon_{i % 5}",
            "weapon_name": f"Weapon {i % 5}",
            "upgrade_attr": "damage",
            "upgrade_effect_value": round(upgrade_effect, 6),
            "weapon_base_value": round(base_value, 6),
            "weapon_growth_value": round(base_value * 0.2, 6),
            "value_after_1_pick": round(base_value * (1 + upgrade_effect), 6),
            "value_after_3_pick": round(base_value * ((1 + upgrade_effect) ** 3), 6),
            "value_after_5_pick": round(base_value * ((1 + upgrade_effect) ** 5), 6),
        })

    df = pd.DataFrame(rows)
    output_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path / "card_weapon_growth_curve_report.csv", index=False)
    df.to_parquet(output_path / "card_weapon_growth_curve_report.parquet", index=False)
    print(f"✓ card_weapon_growth_curve_report: {len(df)} rows")


def generate_build_combination_report(output_path: Path) -> None:
    """生成流派组合测试数据。"""
    np.random.seed(42)
    rows = []

    hero_combos = [
        ["Warrior", "Mage", "Cleric"],
        ["Guardian", "Ranger", "Oracle"],
        ["Berserker", "Assassin", "Engineer"],
        ["Mage", "Archmage", "Summoner"],
        ["Ranger", "Ranger", "Alchemist"],
    ]

    for combo in hero_combos:
        support = np.random.uniform(0.05, 0.15)
        confidence = np.random.uniform(0.6, 0.9)
        lift = np.random.uniform(1.2, 2.5)
        avg_wave = np.random.uniform(40, 55)
        success_rate = np.random.uniform(0.4, 0.7)

        rows.append({
            "dt": "all",
            "combination_type": "hero_team_set",
            "items": combo,
            "antecedent": None,
            "consequent": None,
            "support": round(support, 6),
            "confidence": round(confidence, 6),
            "lift": round(lift, 6),
            "avg_final_wave": round(avg_wave, 2),
            "success_50_rate": round(success_rate, 6),
            "sample_count": np.random.randint(50, 200),
            "item_count": len(combo),
            "items_text": " + ".join(combo),
        })

    card_combos = [
        ["Buff", "Weapon", "Heal"],
        ["Damage", "Speed", "Crit"],
        ["Shield", "Sustain", "Control"],
    ]

    for combo in card_combos:
        support = np.random.uniform(0.03, 0.12)
        confidence = np.random.uniform(0.5, 0.8)
        lift = np.random.uniform(1.1, 2.0)
        avg_wave = np.random.uniform(35, 50)
        success_rate = np.random.uniform(0.35, 0.60)

        rows.append({
            "dt": "all",
            "combination_type": "card_rule",
            "items": combo,
            "antecedent": combo[:1],
            "consequent": combo[1:],
            "support": round(support, 6),
            "confidence": round(confidence, 6),
            "lift": round(lift, 6),
            "avg_final_wave": round(avg_wave, 2),
            "success_50_rate": round(success_rate, 6),
            "sample_count": np.random.randint(30, 150),
            "item_count": len(combo),
            "items_text": " + ".join(combo),
        })

    df = pd.DataFrame(rows)
    output_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path / "build_combination_report.csv", index=False)
    df.to_parquet(output_path / "build_combination_report.parquet", index=False)
    print(f"✓ build_combination_report: {len(df)} rows")


def main() -> None:
    output_path = Path("data/ads_export")
    print("生成测试 ADS 数据...")
    generate_hero_balance_report(output_path)
    generate_map_heatmap_report(output_path)
    generate_card_strength_report(output_path)
    generate_weapon_growth_curve_report(output_path)
    generate_build_combination_report(output_path)
    print("\n✓ 所有测试数据已生成到 data/ads_export/")


if __name__ == "__main__":
    main()

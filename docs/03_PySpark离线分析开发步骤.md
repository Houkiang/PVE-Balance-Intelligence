# PySpark 离线分析开发步骤

## 1. 目标

使用 PySpark 完成从 ODS 到 DWD、DWS、ADS 的离线处理，产出职业平衡、地图热力、卡牌强度和流派组合四类分析结果。

## 2. 任务顺序

推荐开发顺序：

1. `clean_events.py`：读取 ODS 原始事件，生成 DWD 明细。
2. `build_player_match_summary.py`：生成玩家对局汇总和对局汇总。
3. `analyze_hero_balance.py`：计算职业平衡指标。
4. `analyze_map_heatmap.py`：计算地图网格危险度。
5. `analyze_card_strength.py`：计算卡牌强度指标。
6. `mine_build_combinations.py`：使用 FP-Growth 挖掘卡牌和职业组合。
7. `export_ads_to_local.py`：将 HDFS ADS 导出到本地 `data/ads_export/`。

## 3. DWD 明细处理

输入：

```text
hdfs:///game_balance/ods/game_event_log/dt=yyyy-MM-dd/
```

输出：

| 输出 | 内容 |
| --- | --- |
| `dwd_game_event_detail` | 标准事件公共字段 |
| `dwd_player_event_detail` | 玩家加入、死亡、升级等玩家事件 |
| `dwd_player_wave_stat_detail` | 玩家每轮表现 |
| `dwd_card_pick_detail` | 卡牌候选和选择 |
| `dwd_position_tick_detail` | 玩家位置采样 |
| `dwd_enemy_spawn_detail` | 敌人刷新 |

重点逻辑：

1. 解析 `extra` JSON 字段。
2. 过滤缺失 `match_id`、`event_type`、`event_time` 的异常数据。
3. 将数值字段转换为正确类型。
4. 将坐标限制在 `0-999`。

本地样例调试：

```bash
spark-submit spark_jobs/clean_events.py \
  --input data/sample/generated_events \
  --output data/sample/dwd \
  --overwrite \
  --show-counts
```

HDFS 正式运行：

```bash
spark-submit spark_jobs/clean_events.py \
  --input hdfs:///game_balance/ods/game_event_log \
  --output hdfs:///game_balance/dwd \
  --overwrite \
  --show-counts
```

输出目录：

```text
game_event_detail/dt=yyyy-MM-dd/
player_event_detail/dt=yyyy-MM-dd/
player_wave_stat_detail/dt=yyyy-MM-dd/
card_pick_detail/dt=yyyy-MM-dd/
position_tick_detail/dt=yyyy-MM-dd/
enemy_spawn_detail/dt=yyyy-MM-dd/
```

## 4. DWS 汇总处理

核心输出：

| 输出 | 粒度 | 说明 |
| --- | --- | --- |
| `dws_match_summary` | 每局一条 | 最终轮次、是否达成 50 波、队伍人数、团队职业集合 |
| `dws_player_match_summary` | 每玩家每局一条 | 职业、存活轮次、伤害、击杀、治疗、承伤、卡牌集合 |
| `dws_player_card_set` | 每玩家每局一条 | FP-Growth 使用的卡牌集合 |
| `dws_map_grid_summary` | 每地图每网格一条 | 死亡次数、停留时长、刷新数量 |

队伍成功定义：

```text
match_success = final_wave >= 50
```

## 5. ADS 分析任务

### 职业平衡

输入：

```text
dws_player_match_summary
```

输出：

```text
ads_hero_balance_report
```

指标：

| 指标 | 说明 |
| --- | --- |
| `avg_survival_wave` | 平均存活轮次 |
| `avg_kill_per_wave` | 每轮平均击杀数 |
| `success_50_rate` | 50 波达成率 |
| `high_performance_rate` | 高表现率 |
| `balance_score` | 综合平衡评分 |

### 地图热力

输入：

```text
dwd_position_tick_detail
dwd_player_event_detail
dwd_enemy_spawn_detail
```

输出：

```text
ads_map_grid_heatmap_report
```

网格规则：

```text
grid_x = floor(x)
grid_y = floor(y)
grid_id = concat(map_id, '_', grid_x, '_', grid_y)
```

### 卡牌强度

输入：

```text
dwd_card_pick_detail
dws_player_match_summary
```

输出：

```text
ads_card_strength_report
```

指标：

| 指标 | 说明 |
| --- | --- |
| `pick_rate` | 被选择次数 / 出现在候选池次数 |
| `success_50_lift` | 携带后 50 波达成率提升 |
| `wave_lift` | 携带后平均轮次提升 |
| `strength_score` | 综合强度评分 |

### 流派组合

输入：

```text
dws_player_card_set
dws_match_summary
```

输出：

```text
ads_build_combination_report
```

使用 Spark MLlib `FPGrowth` 输出支持度、置信度、提升度，并补充组合对应的平均最终轮次和 50 波达成率。

## 6. 高表现率计算

高表现率按职业内计算，避免坦克、输出、治疗之间直接比较造成偏差。

建议表现分：

```text
performance_score =
  0.35 * normalized_damage_dealt
+ 0.25 * normalized_kill_count
+ 0.20 * normalized_heal_done
+ 0.20 * role_survival_adjusted_tank_score
```

承伤修正：

| 职业类型 | 处理方式 |
| --- | --- |
| 坦克 | 承伤是贡献，但需要乘以生存率或存活轮次修正 |
| 脆皮输出 | 承伤过高通常是风险，可降低权重或转为负向惩罚 |
| 治疗/辅助 | 承伤权重较低，以治疗和辅助贡献为主 |

同一职业内表现分进入前 25% 的玩家记为高表现玩家。

## 7. 验收标准

1. 所有任务都能用 `spark-submit` 独立运行。
2. DWD、DWS、ADS 均写入 HDFS Parquet。
3. 职业、地图、卡牌、流派四类 ADS 表均可生成。
4. ADS 可以导出为本地 CSV/Parquet。
5. 小样本和大样本都能跑通。

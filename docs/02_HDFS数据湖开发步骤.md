# HDFS 数据湖开发步骤

## 1. 目标

使用 Hadoop HDFS 作为离线分析主存储，保存 ODS、DWD、DWS、ADS 四层数据。当前阶段暂不使用 Hive，后续可将 HDFS 上的 Parquet 文件映射成 Hive 外部表。

## 2. HDFS 目录

推荐根目录：

```text
/game_balance/
```

推荐分层目录：

```text
/game_balance/ods/game_event_log/dt=yyyy-MM-dd/
/game_balance/dwd/game_event_detail/dt=yyyy-MM-dd/
/game_balance/dwd/player_wave_stat_detail/dt=yyyy-MM-dd/
/game_balance/dwd/card_pick_detail/dt=yyyy-MM-dd/
/game_balance/dwd/position_tick_detail/dt=yyyy-MM-dd/
/game_balance/dwd/enemy_spawn_detail/dt=yyyy-MM-dd/
/game_balance/dws/match_summary/dt=yyyy-MM-dd/
/game_balance/dws/player_match_summary/dt=yyyy-MM-dd/
/game_balance/dws/player_card_set/dt=yyyy-MM-dd/
/game_balance/dws/map_grid_summary/dt=yyyy-MM-dd/
/game_balance/ads/hero_balance_report/dt=yyyy-MM-dd/
/game_balance/ads/map_grid_heatmap_report/dt=yyyy-MM-dd/
/game_balance/ads/card_strength_report/dt=yyyy-MM-dd/
/game_balance/ads/build_combination_report/dt=yyyy-MM-dd/
```

## 3. 数据格式

| 层级 | 格式 | 说明 |
| --- | --- | --- |
| ODS | JSON Lines | 保留原始事件 |
| DWD | Parquet | 标准化明细数据 |
| DWS | Parquet | 汇总宽表 |
| ADS | Parquet | 分析结果表 |
| Streamlit 本地展示 | CSV 或 Parquet | 从 ADS 导出到 `data/ads_export/` |

## 4. 上传步骤

本地生成数据后，将 JSON Lines 上传到 HDFS：

```bash
hdfs dfs -mkdir -p /game_balance/ods/game_event_log/dt=2026-05-12
hdfs dfs -put -f data/local_raw/game_event_log/dt=2026-05-12/events.jsonl /game_balance/ods/game_event_log/dt=2026-05-12/
```

检查上传结果：

```bash
hdfs dfs -ls /game_balance/ods/game_event_log/dt=2026-05-12
hdfs dfs -cat /game_balance/ods/game_event_log/dt=2026-05-12/events.jsonl | head
```

## 5. PySpark 读写约定

PySpark 读取 ODS：

```python
df = spark.read.json("hdfs:///game_balance/ods/game_event_log/dt=2026-05-12/")
```

PySpark 写入 Parquet：

```python
df.write.mode("overwrite").parquet("hdfs:///game_balance/dwd/game_event_detail/dt=2026-05-12/")
```

## 6. ADS 本地导出

Streamlit 不直接依赖 HDFS。PySpark 先从 HDFS 读取 ADS，再导出一份本地文件：

```text
data/ads_export/hero_balance_report.parquet
data/ads_export/map_grid_heatmap_report.parquet
data/ads_export/card_strength_report.parquet
data/ads_export/build_combination_report.parquet
```

也可以导出 CSV，便于人工查看：

```text
data/ads_export/hero_balance_report.csv
```

## 7. 验收标准

1. HDFS 上存在 ODS、DWD、DWS、ADS 分层目录。
2. ODS 能保存原始 JSON Lines。
3. DWD、DWS、ADS 能保存 Parquet。
4. PySpark 能从 HDFS 读取和写入。
5. ADS 能导出到本地 `data/ads_export/`，供 Streamlit 使用。


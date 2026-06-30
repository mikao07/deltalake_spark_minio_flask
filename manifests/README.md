# Pipeline manifest（守護神定案基準）

`scripts/pipeline_guardian.py` 會讀取此目錄的 `{dataset_id}.json`。

## 停用詞雙版本

| 類型 | 目錄 | manifest 欄位 |
|------|------|----------------|
| 黃金發行 | `dic/stop_words/v1.0.0/` | `gold.release_lexicon_version`、`gold.lexicon_content_hash`、`gold.approved_snapshot_at` |
| 探索測試 | `dic/stop_words/dev/` | `gold.exploration_lexicon_version`（可變，不觸發 FAIL） |

## 快速使用

```powershell
python scripts/pipeline_guardian.py --dataset drinks --offline
python scripts/pipeline_guardian.py --dataset drinks --print-hashes
```

## 日常改探索停用詞

1. 編輯 `dic/stop_words/dev/drinks.txt`
2. 重跑 Gold（Docker 須 `up --build` 若用映像內 dic）
3. **不必**更新 manifest 黃金 hash

## 升級黃金發行（v1 → v2）

1. 將測試滿意的 dev 詞表合併進 `v1.0.0/`（或 bump 為 `v1.0.1/`）
2. `--print-hashes` → 更新 manifest
3. 重跑 Gold → 新 `topic_snapshot` 帶 `release_lexicon_version` + `lexicon_content_hash`
4. **核准快照**（寫入 `gold.approved_snapshot_at`）：

```powershell
python scripts/pipeline_guardian.py --dataset drinks --approve-snapshot
# 或指定時間：--approve-snapshot --snapshot-at "2026-06-23T12:00:00"
```

`approved_snapshot_at` 為對外簡報／模型應使用的痛點快照時間戳；守護神會檢查其是否存在且 lexicon 與 manifest 一致。

**核准前 Bronze 熔斷檢查**：若最近一次 `silver_ocr_etl` 觸發 Bronze **軟熔斷**（隔離占比 >10%）或 **硬熔斷**，`--approve-snapshot` 會以繁中錯誤拒絕。請先處理 `bronze/quarantine/` 與資料問題、重跑 Silver，再核准。

**開發期撤回發行**（僅清 manifest 指標，不刪 Delta 快照列）：

```powershell
python scripts/pipeline_guardian.py --dataset drinks --revoke-snapshot
```

或手動將 `manifests/drinks.json` 的 `gold.approved_snapshot_at`、`gold.processed_image_count` 改回 `null`。

exit code：`0`=PASS，`1`=FAIL（含改動 v1.0.0 未更新 manifest），`2`=僅 WARN。

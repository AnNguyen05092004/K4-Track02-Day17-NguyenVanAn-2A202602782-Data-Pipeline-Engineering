# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Văn An / 2A202602782
**Repo:** [<điền URL repo public>](https://github.com/AnNguyen05092004/K4-Track02-Day17-NguyenVanAn-2A202602782-Data-Pipeline-Engineering.git)
**Commit bài nộp:** <điền mã commit>
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Sonnet 5.5): giải thích khái niệm, hướng dẫn từng checkpoint, đề xuất code cho ba bản sửa và soạn nháp report. Tôi tự dán, chạy lại kiểm tra và đọc hiểu từng thay đổi.
**Nguồn tham khảo khác (nếu có):** Không. Ngoài ba lỗi, tôi bọc ngoặc kép biến `DBT` trong `Makefile` vì đường dẫn thư mục có dấu cách (`make dbt` báo lỗi 127).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `24 rows for 12 tickets`; T-91 ra 3 hàng | u05 ngày 08-12 ra `(2, 0)` thay vì `(5, 1)`; checksum `c50b8851affe` ≠ full recompute `8630e04a61d1` | T-97 còn 2 hàng `is_deleted=False` có `user_id` ở Silver, còn trong snapshot mới nhất và 2 chunk RAG |
| **Nguyên nhân gốc** | `INSERT` thuần: không khoá, không so LSN với bảng đã có | `LOOKBACK_DAYS = 0`, dựa trên giả định sai "event tới trong vài giây"; partition 08-12 không bao giờ được tính lại | Staging lấy `ticket_id` từ `after`; `op='d'` có `after = null` nên bị `WHERE ticket_id IS NOT NULL` loại |
| **Cách sửa** | `silver.py`: `MERGE ... ON ticket_id`, `UPDATE` khi `s._lsn > t._lsn`, ngược lại `INSERT` | `config.py`: `LOOKBACK_DAYS = 3` | `staging.py`: `coalesce(after.ticket_id, before.ticket_id)` |
| **Khái niệm trên slide** | Silver có khoá; MERGE; LSN guard | event time ≠ ingest time; lookback = ceil(P99) | CDC log-based; "xoá phải lan"; tombstone |

Lỗi Silver còn làm `gold_doc_chunks` nhân đôi (22 hàng / 9 chunk), nên khi sửa lỗi 1 thì chunk cũng gọn lại.

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là thực thể có khoá và LSN mới hơn thắng; feature là tổng hợp theo ngày nên tính lại cả partition từ Silver, chạy lại bao nhiêu lần cũng cùng kết quả.
- Tombstone thay vì xoá hẳn hàng: hàng giữ LSN nên chạy lại batch cũ không hồi sinh được ticket (đã thử với 08-12); đánh đổi là hàng đó tồn tại mãi.
- Snapshot dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: tái lập được, có version, không rò rỉ tương lai.
- DuckDB / dbt thay vì Spark: dữ liệu vài chục dòng trên một máy; dbt thêm contract, test và là bản cài độc lập để đối chiếu checksum.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến và quyền được xoá.** `v2026-08-12`..`v2026-08-14` vẫn chứa tên "Nguyễn Văn An". Tôi không sửa tại chỗ (phá bất biến, đổi checksum) mà: ghi yêu cầu xoá vào một sổ để mọi snapshot mới đều lọc theo; huỷ các version cũ chứa dữ liệu đó và dựng lại bản sạch dưới version mới; ghi lineage để retrain model đã học từ chúng. Quyền xoá được ưu tiên hơn tái lập hoàn hảo.
2. **PII ngoài regex.** Hai chốt: ở Silver, nhận diện tên người tiếng Việt kèm so khớp với tên khách hàng đã biết; ở Gold, contract test cuối cùng mở rộng cho tên. Đo bằng precision/recall trên tập mẫu tự gán nhãn và số lượng khớp theo ngày để cảnh báo khi tăng vọt.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 0.63s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
(rút gọn, chỉ phần tổng kết)
01:22:15  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.36 seconds (0.36s).
01:22:15  
01:22:15  Completed successfully
01:22:15  
01:22:15  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Output của "trước khi sửa" (để đối chiếu): `make verify` ra `8/18 checks — FAILURES ABOVE`, `make test` ra `9 failed, 25 passed`, `make rerun3` ra `FAIL`.

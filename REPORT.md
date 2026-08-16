# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Ha Duyen Hung  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Output nguyên văn ba lần chạy</summary>

```text

  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 26.5s
  run 2/3 … 26.4s
  run 3/3 … 26.3s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau lượt pipeline đầu có 13.790 hàng; rerun cùng dữ liệu tăng lên 26.270; baseline verifier ba lượt kết thúc ở 38.750. |
| **Nguyên nhân** | Grain là entity `ticket_id`, nhưng incremental model không có `unique_key` nên dbt dùng append/insert. Retry, rerun hoặc backfill vì thế ghi thêm cùng ticket thay vì thay thế state cũ; source CDC có update nên xóa theo partition ngày cũng không đúng grain entity. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: `unique_key='ticket_id'`, strategy `merge`, giữ nguyên `run_date`; `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. DAG config giảm concurrency/backfill rủi ro, còn sink upsert xử lý root cause. |
| **Bằng chứng** | Trước: 13.790 → 26.270 → 38.750 hàng · sau: **12.480** · duplicate ticket: **0** · checksum 3 lượt: `8dd7c98653` / `8dd7c98653` / `8dd7c98653` · DAG checker: `False / 1`, PASS. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định nhưng chỉ có 8.645 thay vì 9.100 grain; thiếu các event date cũ được ingest muộn. |
| **P99 độ trễ đo được** | **2,725833 ngày** (P50 0,128090; P95 1,813693; max 2,944688; tỷ lệ trễ trên 1 ngày 5,0509%). |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên measured P99 thành policy bounded, operationally reasonable. |
| **Nguyên nhân** | Filter cũ chỉ nhận `event_date > max(event_date)` trong target. Event có event date cũ nhưng `_ingested_at` mới không bao giờ vượt watermark event date, nên bị bỏ sót. |
| **Cách khắc phục** | `gold_feature_daily.sql` đọc lại từ `max(event_date) - interval 3 day`, dùng composite `unique_key=['event_date','customer_id']` và `merge` để reprocessing thay thế cùng grain. |
| **Bằng chứng** | Trước: **8.645** · sau: **9.100** · duplicate grain: **0** · checksum 3 lượt: `3db448685c` cả ba lần. |

P99 bao phủ gần như toàn bộ late arrivals trong khi giới hạn lượng dữ liệu phải tính lại mỗi run. Dùng `max` nhạy với outlier và làm chi phí reprocessing thường trực tăng; P99 chấp nhận một tail rất nhỏ để giữ compute cost dự đoán được. Dataset này có max dưới 3 ngày nên policy 3 ngày cũng hấp thụ toàn bộ sample đã đo.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Silver baseline có 6.606 priority sai: 6.488 NULL và 118 giá trị `-1/0/5`; quarantine rỗng dù source có bad CDC rows. |
| **Nguyên nhân** | `try_cast` biến valid labels thành NULL nhưng lại cho numeric ngoài 1..4 đi qua; quarantine bị tắt. Nếu rank latest CDC trước rồi mới filter invalid, bad update mới nhất sẽ che prior valid state và làm mất cả ticket. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Numeric `'1'..'4'` → integer tương ứng; label trim/case-insensitive `urgent/high/medium/low` → `1/2/3/4`; mọi giá trị khác (`P1`, `P2`, `unknown`, `0`, `5`, `-1`, rỗng, NULL và các giá trị ngoài domain khác) → NULL và quarantine. |
| **Cách khắc phục** | Macro CASE là nguồn logic chung; `silver_tickets` normalize/filter invalid trước `row_number`; quarantine giữ 1 row/source CDC; contract `enforced: true` với `INTEGER`; thêm `not_null` và `accepted_values` tests. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng · `silver_tickets` = **12.480** · priority bad/null = **0** · `dbt test` **11/11 pass** · training/feature/chunks = **12.480 / 9.100 / 31.200**. |

Bronze nên giữ nguyên raw fidelity để audit và replay; Silver mới normalize, enforce contract và tách bad rows sang quarantine. Không nên dừng toàn pipeline vì 312 bad updates không được chặn 12.480 valid ticket, 130.683 event và 31.200 chunks; quarantine vừa giữ evidence để xử lý, vừa cho dữ liệu tốt tiếp tục phục vụ.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **A — Done; B — Done** |
| **Nguyên nhân** | **A:** 5.000 Parquet files không partition cộng predicate bọc `event_time` trong `strftime` không sargable buộc mở/quét mọi file. **B:** commit offset trước sink write tạo at-most-once và mất batch khi crash; chỉ đảo thứ tự với plain INSERT sẽ chuyển loss thành duplicate. |
| **Cách khắc phục** | **A:** compact theo 14 partition `event_date`, sort `customer_name,event_time`, row group 2.048; query dùng Hive partitioning và predicate `event_date = DATE '2026-08-09'`. **B:** `event_id` primary key, atomic multi-row `ON CONFLICT DO UPDATE`, durable DB commit trước offset commit; replay cho effectively-once sink result. `DO UPDATE` giữ payload mới nhất nếu cùng ID được replay với nội dung thay đổi, trong khi `DO NOTHING` sẽ giữ payload cũ. |
| **Bằng chứng** | **A:** files **5.000 → 14**, rows scanned **5.000.000 → 9.324** (**536,3×**), rows on disk **130.683 → 130.683**, hash `4379e4c5d9f3` không đổi. **B:** baseline restart **19.500/20.000** (mất 500); sau fix **20.000/20.000**, duplicate `event_id` **0**, loss **0**, crash test **PASS**. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Correctness/grain/idempotency: xác định row grain, duplicate, unique key, incremental strategy và checksum sau rerun/retry. |
| 2 | Freshness/completeness: phân biệt event time và ingestion time, đo delay distribution, kiểm tra watermark và bounded lookback. |
| 3 | Data quality/contract: schema/type/domain/null, vị trí filter so với CDC ranking, quarantine và observability. |
| 4 | Performance: scan profile, file count/layout, partition pruning, sort/row-group statistics và small-file overhead. |
| 5 | Streaming: delivery semantics, transaction order, durable write, replay behavior và idempotency key của sink. |

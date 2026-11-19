# End-to-End Retail Data Engineering Pipeline on Databricks

Proyek *data engineering* berbasis **Medallion Architecture** (Bronze, Silver, Gold) menggunakan **Databricks Lakehouse**, **Delta Lake**, dan **Unity Catalog** pada skema `workspace.pipeline_demo`.

## Arsitektur Pipeline

1. **Bronze Layer (`02_bronze` & `06_autoloader`)**
   - Melakukan *ingestion* data mentah pesanan (`orders` CSV) dan pelanggan (`customers` JSON) dari Unity Catalog Volume (`/Volumes/workspace/pipeline_demo/landing`) ke tabel `bronze_orders` dan `bronze_customers`.
   - Mendukung pemrosesan inkremental menggunakan *Auto Loader* (`bronze_orders_incr`) serta dilengkapi metadata `_ingested_at` dan `_source_file`.
2. **Silver Layer (`03_silver_clean`)**
   - Pembersihan data, standardisasi format tanggal dan tipe data, deduplikasi, serta validasi aturan bisnis untuk menghasilkan tabel bersih `silver_orders` dan `silver_customers`.
   - Data yang tidak lolos validasi dipisahkan ke tabel `quarantine_orders`.
3. **Gold Layer (`04_gold` & Star Schema)**
   - Tabel agregasi bisnis siap pakai: `gold_daily_revenue`, `gold_top_products`, `gold_revenue_by_city`, `gold_revenue_by_segment`, dan `gold_aov_by_segment`.
   - Pemodelan dimensi (*Star Schema*) terdiri dari `gold_dim_product` (menggunakan *surrogate key* `xxhash64`), `gold_dim_customer`, dan `gold_fact_orders`.
4. **Data Quality & Observability (`05_data_quality_check`)**
   - Pemeriksaan kualitas data otomatis yang mencatat metrik ke tabel `dq_results`.
5. **Orchestration (`retail_pipeline` Job)**
   - Otomatisasi alur kerja berurutan: `bronze` → `silver` → `gold` → `quality` menggunakan komputasi *Serverless*.

## Struktur File

- `00_setup.py` — Konfigurasi katalog, skema `workspace.pipeline_demo`, dan path Unity Catalog Volume.
- `01_generate_sources.py` — Generator dataset sintetis transaksi ritel dan pelanggan.
- `02_bronze.py` — *Batch ingestion* lapisan Bronze dengan parameter dinamis (*widgets*).
- `03_silver_clean.py` — Transformasi pembersihan data dan karantina anomali.
- `04_gold.py` — Pembuatan tabel agregasi analitik lapisan Gold.
- `05_data_quality_check.py` — Pengujian kualitas data dan *freshness check*.
- `06_autoloader.py` — *Structured Streaming ingestion* inkremental menggunakan Auto Loader.
- `07_performance_benchmark.py` — Pengujian skalabilitas komputasi Spark (200K vs 2M baris).

## Implementasi Tantangan (Challenges)

1. **Star Schema Modeling:** Membangun `gold_dim_product` dengan `xxhash64(product)` sebagai *surrogate key*, `gold_dim_customer`, serta `gold_fact_orders` yang terhubung secara relasional.
2. **Data Freshness Check:** Menambahkan validasi umur data pada `05_data_quality_check` yang otomatis memicu `AssertionError` dan menghentikan *pipeline* jika `order_date` terbaru lebih tua dari 45 hari.
3. **Dynamic Notebook Parameterization:** Menambahkan `dbutils.widgets.text("landing_folder", ...)` pada `02_bronze` yang terhubung ke parameter *task* pada Job `retail_pipeline` sehingga sumber direktori dapat diubah tanpa memodifikasi kode.
4. **Performance Benchmark:** Membandingkan waktu eksekusi agregasi pada 200.000 baris versus 2.000.000 baris untuk memverifikasi efisiensi pemrosesan paralel Spark.
5. **Documentation & Version Control:** Dokumentasi lengkap arsitektur serta publikasi kode proyek ke GitHub.

## Cara Menjalankan Proyek

1. Impor seluruh *notebook* ke dalam folder Databricks Workspace.
2. Jalankan `00_setup` dan `01_generate_sources` untuk menyiapkan skema dan data awal di Unity Catalog Volume.
3. Jalankan Databricks Workflow Job **`retail_pipeline`** dengan urutan *task*: `bronze` → `silver` → `gold` → `quality`.
4. Pastikan parameter `landing_folder` pada *task* `bronze` mengarah ke `/Volumes/workspace/pipeline_demo/landing`.

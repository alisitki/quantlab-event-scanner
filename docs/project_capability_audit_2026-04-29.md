# QuantLab Event Scanner - Mevcut Kabiliyet Audit

Tarih: 2026-04-29

Repo durumu: `13c1c82 Split Phase 3A BTC trial finalization`

## Kapsam ve Kanit Tabanı

Bu audit kod kalitesi, mimari refactor, style, maintainability veya algoritma
dogrulugu auditi degildir. Amaç, projenin bugun urun/kabiliyet olarak ne
yapabildigini ve neyi henuz ispatlamadigini netlestirmektir.

Kanit tabani repo icidir:

- `README.md`
- `docs/*.md`
- `databricks.yml`
- `configs/*.yaml`
- `STATE_V2_CONSUMER.md`
- `jobs/` job entrypoint envanteri
- `tests/` test envanteri

Canli Databricks run metadata veya S3 icerigi bu raporda kanit olarak
kullanilmadi. Repo dokumanlarinda kayda gecmis run id, row count ve S3 path
bilgileri "repo tarafindan kayda gecirilmis sonuc" olarak ele alindi.

## Yonetici Karari

Proje artik yalnizca bir iskelet degil. Repo dokumanlarina gore BTCUSDT icin
manifest okuyan, trade/BBO verisini sekillendiren, hareket adaylarini bulan,
trial event map yazan, pre-event market snapshot ureten, event-vs-normal profil
karsilastirmasi yapan, 10 normal ornekli dagilim karsilastirmasi ureten ve
Phase 3A'da 10 eventlik multi-event trial/finalize akisini tamamlamis bir
arastirma pipeline'i durumunda.

Fakat proje henuz production urun degil. Kabul edilmis ciktilar `_trial` ve
`_probe` koklerinde, BTC odakli ve Databricks/S3 operasyonuna bagimli. Dokumanlar
acik sekilde ML egitimi, trading/execution, istatistiksel sinyal ispati,
lead-lag sonucu ve production dataset uretimini kapsam disi birakiyor.

## Durum Matrisi

| Alan | Bugunku durum | Kanitli sonuc | Production durumu |
|---|---|---:|---|
| Phase 0/0.5 bootstrap | Repo, config, test, bundle ve CLI auth talimatlari var | Calistirma hazirligi tanimli | Production degil |
| Phase 1A manifest probe | Manifest v2 semantigiyle BTC trade partition probe | Log-only probe | Production degil |
| Phase 1B trade profiling | 3 borsa BTC trade kalite/time profili | `5,578,439` trade row, `3/3` coverage | Production degil |
| Phase 1C move scan | 1s OHLCV ve +/-1% / 60s aday tarama | `216,633` 1s row, `60` raw candidate, `1` grouped candidate | Log-only |
| Phase 1D event map write | Trial raw candidates ve grouped event map S3 write/readback | `60` raw, `1` grouped event | Trial-only |
| Phase 2A trade window | Tek event icin pre-event trade window | `15,072` row | Trial-only |
| Phase 2B BBO window | Tek event icin pre-event BBO window | `103,067` row | Trial-only |
| Phase 2C market snapshots | 3 borsa x 300 saniye dense 1s panel | `900` snapshot row | Trial-only |
| Phase 2D profile report | Exchange, cross-exchange, bucket profile | `30/30/48` rows | Descriptive trial |
| Phase 2E normal-time trial | Tek normal 5dk pencere ve profil | `900` normal snapshot row | Single-normal trial |
| Phase 2F comparison | Event profilini normal profil ile karsilastirir | `660/150/96` comparison rows | Descriptive trial |
| Phase 2G/2I top diffs | Metric taxonomy ve group-separated top diff | `180/120/156` top-diff rows | Inspection-only |
| Phase 2J multi-normal | Tek evente karsi 10 normal dagilimi | `9000` snapshot, `660/150/96` comparison | Trial-only |
| Phase 2J P1 perf | Multi-normal runtime iyilestirme | `8m08s` accepted benchmark | Scale hazirligi |
| Phase 2K content review | Mevcut top-diff icerik yorumu | Stable metric shortlist onerisi | Sinyal ispati degil |
| Phase 3A multi-event | Job A/B split ile 10 event trial/finalize | `selected_event_count=10`, top-diff ve summary ciktilari | Accepted trial, production degil |

## Bugun Yapabildikleri

### 1. Manifest ve Veri Kapsami

Proje compacted manifest v2 semantigini tuketebiliyor. Tuketici kurali net:
`available=true` olan entry okunur, `available=false` okunmaz ve path
uretilmez; parquet pathleri `artifacts.*` alanlarindan gelir.

`configs/dev.yaml` ve `configs/prod.yaml` ayni temel sekli tasiyor:

- Input root: `s3://quantlab-compact-stk-euc1`
- Manifest: `s3://quantlab-compact-stk-euc1/compacted/_manifest.json`
- Output root: `s3://quantlab-research`
- Ana borsalar: `binance`, `bybit`, `okx`
- Stream listesi: `bbo`, `trade`, `mark_price`, `funding`, `open_interest`

Kanitli accepted akislarda asil islenen scope BTC trade ve BBO verisidir.
`mark_price`, `funding` ve `open_interest` config seviyesinde gorunur, fakat
accepted pipeline kabiliyeti olarak one cikmiyor.

### 2. BTC Trade Profiling ve Event Candidate Scan

Proje BTC trade verisini kalite ve zaman davranisi acisindan profilleyebiliyor.
Repo dokumanina gore 2026-04-26 tarihli Phase 1B accepted run, `20260423`
tarihini coverage-aware secmis, `binance/bybit/okx` icin `3/3` coverage bulmus,
`5,578,439` trade row okumustur. Bilinen trade kolonlarinda null yok, duplicate
trade-id group yok ve out-of-order count her borsada sifir kaydedilmistir.

Phase 1C, sabit tarihli BTC trade verisini 1 saniyelik OHLCV seviyesine indirip
`+/-1%` hareketin 60 saniye icinde gerceklesip gerceklesmedigini tarar. Accepted
run `216,633` adet 1 saniyelik price row, `60` raw candidate, `0` ambiguous
candidate ve yaklasik `1` grouped candidate kaydetmistir.

### 3. Trial Event Map

Phase 1D, Phase 1C hareket adaylarini ilk kez S3'e trial artifact olarak
yaziyor:

- `raw_candidates/_trial/run_id=phase1d_20260427T063442Z`
- `events_map/_trial/run_id=phase1d_20260427T063442Z`

Readback sonucu `60` raw candidate ve `1` grouped event dogrulanmistir. Trial
event id: `binance_btcusdt_20260423_down_001`.

Bu halen production `events_map` degildir. Kanitli sonuc, yaz/readback
kontratinin trial pathte calistigidir.

### 4. Pre-Event Trade/BBO Window Extraction

Phase 2A ve Phase 2B, ayni trial event icin event oncesi 5 dakikalik UTC
pencereyi cikarir:

- Window: `2026-04-23 09:59:10 <= ts_event_ts < 2026-04-23 10:04:10`
- Trade window: `15,072` row
- BBO window: `103,067` row
- BBO coverage: `binance`, `bybit`, `okx`
- BBO null summary: timestamp, BBO ve derived BBO alanlarinda `0`
- Negative spread row: `0`

Bu kabiliyet, event map rowundan veri partition secimi ve pencere bazli row
extraction yapabildigini gosterir.

### 5. 1 Saniyelik Market Snapshot Paneli

Phase 2C, trade ve BBO pencerelerini ayni event/window metadata'si altinda
birlestirip dense 1 saniyelik market snapshot paneli uretir.

Accepted output:

- `900` snapshot row
- `binance=300`, `bybit=300`, `okx=300`
- `second_before_event` araligi: `1..300`
- BBO update seconds: `871`
- Forward-filled BBO seconds: `29`
- Pre-first-update seconds: `0`
- Negative spread rows: `0`

Bu, projenin event oncesi 3 borsa x 300 saniye panel uretebildigini kanitlar.

### 6. Event Profile Report

Phase 2D, snapshot panelinden uc profil alt tablosu uretir:

- `exchange_profile`
- `cross_exchange_mid_diff`
- `bucket_change_profile`

Accepted row contract:

- `exchange_profile=30`
- `cross_exchange_mid_diff=30`
- `bucket_change_profile=48`

Profil pencereleri hem trailing cumulative (`last_300s`, `last_120s`,
`last_60s`, `last_30s`, `last_10s`) hem de non-overlap bucket
(`bucket_300_120s`, `bucket_120_60s`, `bucket_60_30s`, `bucket_30_10s`,
`bucket_10_0s`) mantigini kapsar.

### 7. Normal-Time Trial ve Event-vs-Normal Comparison

Phase 2E, ayni tarih uzerinde eventlerden uzak bir normal 5 dakikalik pencere
secer ve Phase 2A-2D'nin normal-time esdegirini uretir. Accepted run ilk
denemede `2026-04-23 00:00:00 <= ts_event_ts < 2026-04-23 00:05:00` penceresini
kabul etmistir.

Accepted normal artifacts:

- Normal trade rows: `28,017`
- Normal BBO rows: `162,689`
- Normal snapshot rows: `900`
- Normal profile rows: `30/30/48`

Phase 2F, accepted event profile ve normal profile'i curated metric setleriyle
karsilastirir. Accepted comparison rows:

- `exchange_profile_comparison=660`
- `cross_exchange_mid_diff_comparison=150`
- `bucket_change_profile_comparison=96`

Karsilastirma descriptive niteliktedir. Sinyal, ML, istatistiksel anlamlilik ve
lead-lag sonucu uretmez.

### 8. Metric Taxonomy ve Top-Diff Inspection

Phase 2G ve Phase 2I, comparison layer'i insan incelemesine uygun hale getirir.
Metric taxonomy su gruplari kullanir:

- `context`
- `signal_candidate`
- `price_dislocation`
- `unstable`

Accepted dynamic top-diff rows:

- `exchange_profile_top_diffs=180`
- `cross_exchange_mid_diff_top_diffs=120`
- `bucket_change_profile_top_diffs=156`

Phase 2K review, context/activity metriklerinin global siralamayi domine
ettigini, buna ragmen 10-normal karsilastirmanin stable price/return,
cross-exchange dislocation ve OKX book/spread metriklerini okunabilir hale
getirdigini kayda gecirir. Bu review de sinyal ispati degildir.

### 9. Multi-Normal Distribution Comparison

Phase 2J, tek normal ornek yerine `N=10` activity-matched normal pencere secer.
Normal secimi aday eventlere yakin anchorlari dislar, `3/3` coverage ve
`900` row snapshot kalite kontratini uygular, aktivite benzerligini event
`last_300s` exchange profile uzerinden hesaplar.

Accepted Phase 2J output:

- Selected normal count: `10`
- Multi-normal snapshots: `9000`
- Profiles: `exchange_profile=300`, `cross_exchange_mid_diff=300`,
  `bucket_change_profile=480`
- Distribution comparison: `660/150/96`
- Top diffs: `180/120/156`
- Validation: invalid `normal_sample_count=0`, `metric_group` null count `0`,
  `absolute_diff_vs_mean` negative count `0`

P1 performance benchmark ayni output kontratlarini koruyarak runtime'i `8m08s`
seviyesine indirmistir. Bu, production degil ama scale-out icin onemli bir
hazirlik seviyesidir.

### 10. Phase 3A Multi-Event Trial ve Finalization

Phase 3A artik repo dokumanina gore sadece taslak degil. Iki job'a bolunmus
accepted trial akisi vardir:

- Job A: `phase3a_btc_multi_event_trial_classic`
- Job B: `phase3a_btc_multi_event_finalize_classic`

Job A, BTC `%1 / 60s` grouped event secer, her event icin activity-matched
normal windows secer, event/normal snapshots ve profiles uretir, comparison
reports yazar ve validation yapar. Job B, Job A comparison reports'u taze okur,
`top_diffs/` ve `summary/` altinda final inspection tablolarini yazar.

Repo README'sindeki 2026-04-29 accepted retry kaydi:

- Source run ID: `phase3a_20260429T085638Z`
- Output root:
  `s3://quantlab-research/btc_multi_event_trials/_trial/run_id=phase3a_20260429T085638Z`
- Job A run: `358829999567665`
- Job B run: `159108292725392`
- `selected_event_count=10`
- Comparison rows: exchange `6600`, cross-exchange `1500`, bucket-change `960`
- Final top diffs: exchange `1800`, cross-exchange `1200`, bucket-change `1560`
- Summary outputs: `event_counts_by_direction=2`, `metric_group_summary=80`,
  `normal_selection_quality=10`, `event_processing_status=10`
- Validation: invalid `normal_sample_count=0`, `metric_group` null rows `0`,
  `absolute_diff_vs_mean` negative rows `0`, top-diff dynamic mismatch count `0`

Bu, projenin 1 eventten 10 eventlik trial multi-event analize gecis yaptigini
kanıtlar. Ancak output kokleri halen `_trial`; bu production event-scanner
servisi veya production dataset anlamina gelmez.

## Deploy ve Operasyon Yuzeyi

`databricks.yml` hem serverless hem classic hedefleri icerir. Serverless yuzey
Phase 1 probe ve move-candidate scan icin vardir. Ana trial pipeline classic
cluster uzerinden isler ve `--var cluster_id=<classic_cluster_id>` bekler.

Job entrypoint envanteri Phase 1'den Phase 3A'ya kadar lineer bir urun yuzeyi
gosterir:

- `01_detect_events.py`
- `02_profile_trade_data.py`
- `03_scan_trade_move_candidates.py`
- `04_write_trial_event_map.py`
- `05_extract_pre_event_windows_trial.py`
- `06_extract_pre_event_bbo_windows_trial.py`
- `07_build_pre_event_market_snapshots_trial.py`
- `08_profile_pre_event_market_snapshots_trial.py`
- `09_build_normal_time_trial.py`
- `10_compare_event_vs_normal_profiles_trial.py`
- `11_inspect_profile_comparison_top_diffs_trial.py`
- `12_build_multi_normal_trial.py`
- `13_build_btc_multi_event_trial.py`
- `14_probe_btc_event_coverage.py`
- `15_finalize_btc_multi_event_trial.py`

Operasyonel not: `databricks.yml` icindeki Phase 3A default task parametreleri
smoke/trial sekle yakindir (`20260423`, `max-events=5`) ve finalize job icin
`phase3a_SOURCE_RUN_ID` placeholder kullanir. README'deki accepted Phase 3A
kosusu genis tarih araligi ve runtime override parametreleriyle
calistirilmistir. Bu nedenle accepted runbook ile bundle defaultlari ayni sey
olarak yorumlanmamalidir.

## Test Yuzeyi

Repo test envanteri config, manifest, path, profiling, pre-event windows,
BBO, snapshots, profile reports, comparison reports, top diffs, multi-normal ve
BTC multi-event helper davranislarini kapsar. Bu audit testlerin icerigini kod
kalitesi acisindan degerlendirmez; sadece repo-local destekleyici yuzey olarak
kaydeder.

Bu rapor olusturulurken canli test calistirmasi audit kapsaminin zorunlu parcasi
degildi. Markdown degisikligi icin davranissal test gerekmiyor.

## Acikca Kapsam Disi Olanlar

Repo dokumanlarina gore proje su an sunlari yapmiyor veya ispatlamiyor:

- Trading veya execution yapmaz.
- ML modeli egitmez veya skorlamaz.
- Istatistiksel anlamlilik iddiasi uretmez.
- Lead-lag sonucu veya nedensellik iddiasi uretmez.
- Production `events_map` veya production dataset urettigini iddia etmez.
- Yerel PC'de veri output'u saklamaz; output S3'e yazilir.
- BTC disi semboller icin accepted capability gostermez.
- `mark_price`, `funding`, `open_interest` streamleri icin accepted end-to-end
  capability gostermez.
- Surekli calisan monitor, scheduler veya user-facing UI saglamaz.

## Ana Riskler ve Sinirlar

1. Trial path bagimliligi: Kabul edilmis outputlar `_trial` ve `_probe`
   koklerinde duruyor. Production path, retention, versioning ve consumer
   kontrati henuz ayrica tanimli degil.
2. Databricks/S3 operasyon bagimliligi: Calisma Databricks CLI auth, classic
   cluster, cluster kapasitesi ve S3 erisimine bagli.
3. Compute hassasiyeti: Phase 2J performans dokumanlari kucuk compute ile ciddi
   runtime sorunu yasandigini, P0/P1 ile iyilesme geldigini gosteriyor. Phase 3A
   accepted retry icin de buyuk/hybrid compute notu var.
4. Scope siniri: Accepted akisin cekirdegi BTCUSDT ve `binance/bybit/okx`.
   Baska symbol family veya stream icin ayni kalite ve coverage kaniti yok.
5. Metrik yorumu: Context/activity metrikleri outlier siralamalarini domine
   edebiliyor. `*.relative_change` ve denominator-risk flagli metrikler ana
   sinyal shortlist'i olarak kullanilmamali.
6. Runbook farki: Accepted Phase 3A komutlari runtime override ile calismis.
   Bundle defaultlari birebir accepted scale-out komutu degil.
7. Production eksikleri: State registry, artifact lifecycle, data contract
   versiyonlama, alerting, retry policy, run promotion ve downstream consumer
   kontratlari urunlestirme icin henuz raporda kanitlanmis degil.

## Bugun Kullanilabilir Olanlar

- BTC manifest coverage ve trade/BBO partition secimi.
- BTC trade kalite profili ve 1 saniyelik price aggregation.
- `%1 / 60s` move candidate scan ve grouped event trial map.
- Tek event icin pre-event trade/BBO window extraction.
- 3 borsa x 300 saniye dense market snapshot uretimi.
- Event profile, normal profile ve event-vs-normal comparison raporlari.
- Metric taxonomy, denominator-risk flagleri ve group-separated top diffs.
- 10 normal ornekli multi-normal distribution comparison.
- 10 eventlik Phase 3A trial/finalize artifact uretimi ve summary/top-diff
  raporlari.

## Henuz Production Olmayanlar

- `_trial` artifactleri production dataset olarak kabul edilmemeli.
- Phase 3A accepted retry production deployment degil; arastirma/trial
  milestone'udur.
- Metric shortlist sinyal ispati degil, Phase 3A sonrasi analiz icin aday
  listesidir.
- Accepted compute profili operasyonel olarak not edilmis, fakat kalici
  production compute policy degildir.

## Siradaki Mantikli Adimlar

1. Production hedefini netlestir: output path, artifact version, retention,
   consumer contract ve run promotion kurallari.
2. Phase 3A accepted runbook'u bundle defaultlariyla uyumlu hale getir veya
   dokumanda smoke-run ve accepted-scale-run ayrimini resmi olarak ayir.
3. BTC disi scope istenecekse once coverage probe ve event-quality preview
   seviyesinde genislet.
4. Ana metric policy'yi kilitle: context metrikleri kontrol/matching katmaninda,
   stable return/dislocation/book-spread metrikleri descriptive shortlist'te,
   unstable/denominator-risk metrikleri appendix'te kalsin.
5. Production oncesi minimum operasyon katmani ekle: run registry, validation
   summary, failure taxonomy, compute profile, retry strategy ve downstream
   okuma kontrati.

## Nihai Degerlendirme

QuantLab Event Scanner bugun BTCUSDT uzerinde event adayindan multi-event
comparison summary'ye kadar uzanan, repo dokumanlariyla kabul kaydi tutulmus bir
arastirma pipeline'idir. Projenin en guclu noktasi, her fazda S3 artifact ve row
contract ile ilerlemesi ve Phase 3A'da 10 eventlik trial/finalize akisini
tamamlamis olmasidir.

Ana sinir ise ayni: bu henuz production event scanner veya sinyal sistemi
degildir. Mevcut haliyle dogru tanim, "BTC odakli, Databricks/S3 uzerinde
calisan, trial artifact ureten ve descriptive event-vs-normal/multi-normal
analiz yapan research pipeline"dir.

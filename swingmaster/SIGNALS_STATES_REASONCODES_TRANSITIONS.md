# Swingmaster: Signaalit, Tilat, ReasonCodet ja Transition Contract

Tämä dokumentti kuvaa nykyisen toteutuksen perusteella:
1. kaikki signaalit ja niiden syntylogiikan,
2. tilakoneen tilat,
3. reasoncodet ja niiden merkityksen,
4. transition contract matrixin.

Lähteet (koodi):
- `swingmaster/core/signals/enums.py`
- `swingmaster/app_api/providers/osakedata_signal_provider_v2.py`
- `swingmaster/app_api/providers/signals_v2/*.py`
- `swingmaster/core/domain/enums.py`
- `swingmaster/core/domain/transition_graph.py`
- `swingmaster/core/policy/guardrails.py`
- `swingmaster/core/policy/rule_v1/*.py`
- `swingmaster/core/policy/rule_v2/policy.py`

## 1. Signaalit ja miten ne syntyvät

## 1.1 SignalKey-lista
- `TREND_STARTED`
- `SLOW_DECLINE_STARTED`
- `TREND_MATURED`
- `SELLING_PRESSURE_EASED`
- `STABILIZATION_CONFIRMED`
- `ENTRY_SETUP_VALID`
- `EDGE_GONE`
- `INVALIDATED`
- `DOW_TREND_UP`
- `DOW_TREND_DOWN`
- `DOW_TREND_NEUTRAL`
- `DOW_TREND_CHANGE_UP_TO_NEUTRAL`
- `DOW_TREND_CHANGE_DOWN_TO_NEUTRAL`
- `DOW_TREND_CHANGE_NEUTRAL_TO_UP`
- `DOW_TREND_CHANGE_NEUTRAL_TO_DOWN`
- `DOW_LAST_LOW_L`
- `DOW_LAST_LOW_HL`
- `DOW_LAST_LOW_LL`
- `DOW_LAST_HIGH_H`
- `DOW_LAST_HIGH_HH`
- `DOW_LAST_HIGH_LH`
- `DOW_NEW_LL`
- `DOW_NEW_HH`
- `DOW_RESET`
- `DOW_BOS_BREAK_DOWN`
- `DOW_BOS_BREAK_UP`
- `DATA_INSUFFICIENT`
- `NO_SIGNAL`

## 1.2 Nykyinen v2-signaaliputki
Provider: `OsakeDataSignalProviderV2`.

- `DATA_INSUFFICIENT`:
  - jos historiaa ei ole vaadittua minimimäärää.

- `TREND_STARTED`:
  - `trend_started.py`-ehdot täyttyvät (SMA20/regime + breakdown).
  - tai force-tilanne: `DOW_TREND_CHANGE_UP_TO_NEUTRAL` + `DOW_LAST_LOW_LL`.

- `SLOW_DECLINE_STARTED`:
  - `slow_decline_started.py`: staircase-lasku (`t-10 > t-5 > t-2 > t0`), minimi-pudotusprosentti ja optionaalinen MA-filtteri.
  - provider lisää tämän primäärisignaalina ennen muita pääsignaaleja.

- `TREND_MATURED`:
  - `trend_matured.py`: rakenne + aika + momentum yhdessä.

- `STABILIZATION_CONFIRMED`:
  - `stabilization_confirmed.py`: volatiliteetin supistuminen + low-rakenne + upper-close-ehdot.

- `ENTRY_SETUP_VALID`:
  - `entry_setup_valid.py`: base range tai reclaim-MA20 + riski- ja support-ehdot.

- `INVALIDATED`:
  - `invalidated.py`: päivän low rikkoo prior-lown (lookback).
  - kun tämä syttyy, provider poistaa samalta päivältä `STABILIZATION_CONFIRMED` ja `ENTRY_SETUP_VALID`.

- `DOW_*`-signaalit:
  - `dow_structure.py` / `compute_dow_signal_facts`.
  - trendi (`UP/DOWN/NEUTRAL`), viime pivotti-tyypit (`HH/HL/LH/LL`), trendimuutokset, resetit ja BoS-breakit.

- `NO_SIGNAL`:
  - kun primäärisignaaleja ei synny eikä `INVALIDATED` ole aktiivinen.

## 1.3 Huomiot
- `SELLING_PRESSURE_EASED` ja `EDGE_GONE` ovat enumissa, mutta eivät nykyisessä v2-providerissa normaalisti emittoitavia pääsignaaleja.
- Policy v2 lisää invalidaation: jos prev state on `STABILIZING` tai `ENTRY_WINDOW` ja `DOW_NEW_LL` löytyy, policy lisää `INVALIDATED`-signaalin ennen päätöstä.
- Policy v2: `NO_TRADE -> DOWNTREND_EARLY` voi tapahtua myös `SLOW_DECLINE_STARTED`-signaalista, jos `DOW_TREND_UP` ei ole aktiivinen.

## 2. Tilakoneen tilat ja merkitys

Tilat (`State`):
- `NO_TRADE`: neutraali, ei aktiivista setupia.
- `DOWNTREND_EARLY`: downtrend havaittu varhaisessa vaiheessa.
- `DOWNTREND_LATE`: downtrend kypsynyt.
- `STABILIZING`: laskun jälkeinen stabiloitumisvaihe.
- `ENTRY_WINDOW`: sisääntuloikkuna.
- `PASS`: välitila ennen neutralointia.

Tyypillinen elinkaari:
- `NO_TRADE -> DOWNTREND_EARLY -> DOWNTREND_LATE -> STABILIZING -> ENTRY_WINDOW -> PASS -> NO_TRADE`

## 3. ReasonCodet ja merkitys

ReasonCodet (`ReasonCode`) ja semantiikka:
- `TREND_STARTED`: trendi alkoi.
- `TREND_MATURED`: trendi kypsyi.
- `SELLING_PRESSURE_EASED`: myyntipaine hellitti.
- `STABILIZATION_CONFIRMED`: stabiloituminen vahvistui.
- `ENTRY_CONDITIONS_MET`: sisääntuloehdot täyttyivät.
- `EDGE_GONE`: setup/edge ei enää validi.
- `INVALIDATED`: setup mitätöityi.
- `INVALIDATION_BLOCKED_BY_LOCK`: invalidaatio estetty lockilla.
- `DISALLOWED_TRANSITION`: ehdotettu siirtymä ei sallittu.
- `PASS_COMPLETED`: pass-vaihe valmis.
- `ENTRY_WINDOW_COMPLETED`: entry-ikkuna valmis.
- `RESET_TO_NEUTRAL`: palautus neutraaliin.
- `CHURN_GUARD`: sahauksenesto blokkaa muutoksen.
- `MIN_STATE_AGE_LOCK`: minimi-ikä guardrail estää muutoksen.
- `DATA_INSUFFICIENT`: data ei riitä päätökseen.
- `NO_SIGNAL`: ei toimintakelpoista signaalia.
- `SLOW_DECLINE_STARTED`: hidas laskutrendi käynnistyi (policy-trigger reason).

Persistointi:
- reasont tallennetaan prefiksillä `POLICY:` (ks. `reason_to_persisted`).

## 4. Transition Contract Matrix

## 4.1 Sallitut siirtymät (`ALLOWED_TRANSITIONS`)
- `NO_TRADE` -> `NO_TRADE`, `DOWNTREND_EARLY`
- `DOWNTREND_EARLY` -> `DOWNTREND_EARLY`, `DOWNTREND_LATE`, `STABILIZING`, `NO_TRADE`
- `DOWNTREND_LATE` -> `DOWNTREND_LATE`, `STABILIZING`, `NO_TRADE`
- `STABILIZING` -> `STABILIZING`, `ENTRY_WINDOW`, `NO_TRADE`
- `ENTRY_WINDOW` -> `ENTRY_WINDOW`, `PASS`, `NO_TRADE`
- `PASS` -> `PASS`, `NO_TRADE`

## 4.2 Guardrail-contract
Guardrailit ajetaan ehdotetun siirtymän päälle:
- jos siirtymä ei ole sallittu: `DISALLOWED_TRANSITION`
- jos tilan ikä alle minimin: `MIN_STATE_AGE_LOCK`

`MIN_STATE_AGE`:
- `NO_TRADE=0`
- `DOWNTREND_EARLY=2`
- `DOWNTREND_LATE=3`
- `STABILIZING=2`
- `ENTRY_WINDOW=1`
- `PASS=1`

## 4.3 Rule-contract (v1)
Ydinjärjestys:
1. Hard exclusions (`DATA_INSUFFICIENT` > `INVALIDATED`) -> `NO_TRADE`
2. Edge/churn/entry/reset-helperit voivat overridea
3. Per-state rule match
4. Fallback (stay + fallback reason)

Per-state signaalisäännöt:
- `NO_TRADE`:
  - `TREND_STARTED` -> `DOWNTREND_EARLY`
  - `SLOW_DECLINE_STARTED` + ei `DOW_TREND_UP` -> `DOWNTREND_EARLY` (v2)
- `DOWNTREND_EARLY`:
  - `TREND_MATURED` -> `DOWNTREND_LATE`
  - `STABILIZATION_CONFIRMED` tai `SELLING_PRESSURE_EASED` -> `STABILIZING`
- `DOWNTREND_LATE`:
  - `STABILIZATION_CONFIRMED` tai `SELLING_PRESSURE_EASED` -> `STABILIZING`
- `STABILIZING`:
  - `STABILIZATION_CONFIRMED` -> stay `STABILIZING`
  - helper voi antaa `ENTRY_CONDITIONS_MET` -> `ENTRY_WINDOW`
- `ENTRY_WINDOW`:
  - `ENTRY_SETUP_VALID` -> stay `ENTRY_WINDOW`
  - muuten `PASS`
- `PASS`:
  - `NO_TRADE`

Policy-helperit (v1) käytännössä:
- `_edge_gone_decision`:
  - pitkä `ENTRY_WINDOW` -> `PASS` (`EDGE_GONE`)
  - pitkä `STABILIZING` -> `NO_TRADE` (`EDGE_GONE`) tietyin ehdoin
- `_churn_guard_decision`:
  - estää churniä ja pitää nykytilassa (`CHURN_GUARD`)
- `_entry_conditions_decision`:
  - `STABILIZING` + `ENTRY_SETUP_VALID` + recency/freshness ehdot -> `ENTRY_WINDOW` (`ENTRY_CONDITIONS_MET`)
- `_should_reset_to_neutral`:
  - hiljaisuus/churn/edge-ehto -> `NO_TRADE` (`RESET_TO_NEUTRAL`)

Lisähuomio (v2):
- kun `NO_TRADE -> DOWNTREND_EARLY`, policy voi asettaa state_attrin `downtrend_origin`:
  - `TREND` (jos triggeri `TREND_STARTED`)
  - `SLOW` (jos triggeri `SLOW_DECLINE_STARTED`)

---

Tämä dokumentti kuvaa nykyisen kooditilan; jos signaalimoduuleja tai policy-rulesettejä muutetaan, myös tämä dokumentti pitää päivittää.

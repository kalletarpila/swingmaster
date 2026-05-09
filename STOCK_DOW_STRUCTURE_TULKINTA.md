# Stock Dow Structure - tulkintaopas tietokannan käyttäjälle

## Kenelle tämä dokumentti on

Tämä dokumentti on tarkoitettu henkilölle, joka tulkitsee tietokannan tauluja ja haluaa ymmärtää, mitä Stock Dow Structure -tapahtumat tarkoittavat liiketoiminnan tai markkina-analyysin näkökulmasta.

Painotus on tulkinnassa, ei ohjelmakoodissa.

## Mitä Dow-rakenne tässä tarkoittaa

Stock Dow Structure kuvaa osakkeen rakenteellista käyttäytymistä tapahtumina:

- Pivot-huiput ja pivot-pohjat
- Trendin suunta (UP, DOWN, NEUTRAL)
- Rakennerikot (BOS_UP, BOS_DOWN)
- Nollaukset (RESET)
- Trendin vaihtumiset (TREND_CHANGE)

Tärkeä periaate:

- Laskenta on close-pohjainen.
- Tapahtumia syntyy vain, kun rakenne muuttuu tai vahvistuu.
- Taulu ei ole päivittäinen aikarivi jokaiselle kaupankäyntipäivälle.

## Laskennan periaatteet selkokielellä

### 1) Laskennassa käytetään vain close-hintaa

Rakenne muodostetaan päätöshinnan perusteella. Open, high ja low tallennetaan riveille taustatiedoksi, mutta trendi- ja rakennepäätelmät tehdään close-arvosta.

### 2) Pivot tunnistetaan vahvistusikkunalla

Pivot radius on oletuksena 3.

Se tarkoittaa käytännössä:

- Pivot High ehdokas syntyy, jos keskimmäinen close on uniikki korkein arvo 7 close-hinnan ikkunassa (3 ennen, itse päivä, 3 jälkeen).
- Pivot Low ehdokas syntyy, jos keskimmäinen close on uniikki matalin arvo samassa ikkunassa.

Pivot kirjataan vasta, kun vahvistusikkuna on täynnä. Siksi:

- event_date on pivotin päivä
- confirmed_as_of_date on päivä, jolloin pivot voidaan vahvistaa

### 3) Dow-labelit (HH, LH, HL, LL)

Pivot High saa labelin:

- H: ensimmäinen huippu
- HH: uusi huippu ylittää edellisen huipun (>)
- LH: uusi huippu ei ylitä edellistä huippua (≤); tasatilanne käsitellään LH:nä

Pivot Low saa labelin:

- L: ensimmäinen pohja
- HL: uusi pohja ylittää edellisen pohjan (>)
- LL: uusi pohja ei ylitä edellistä pohjaa ylöspäin (≤); tasatilanne käsitellään LL:nä

### 4) Trendin tila johdetaan labelien yhdistelmästä

- UP, jos viimeisin huippulabel on HH ja viimeisin pohjalabel on HL
- DOWN, jos viimeisin huippulabel on LH ja viimeisin pohjalabel on LL
- Muulloin NEUTRAL

### 5) Aktiivinen BOS-taso

Kun trendi on UP, aktiivinen rikkoutumistaso on viimeisin HL-pohja.

Kun trendi on DOWN, aktiivinen rikkoutumistaso on viimeisin LH-huippu.

Näitä käytetään rakenteen murtumisen tulkintaan.

### 6) BOS ja RESET-logiikka

UP-trendissä:

- Jos close menee aktiivisen HL-tason alle, syntyy BOS_DOWN
- Jos tällainen rikkominen tapahtuu toisen kerran peräkkäin ennen rakenteen palautumista, syntyy RESET

DOWN-trendissä:

- Jos close menee aktiivisen LH-tason yli, syntyy BOS_UP
- Jos toinen vastaava rikkominen tapahtuu peräkkäin, syntyy RESET

RESET tarkoittaa rakenteen nollausta:

- trend_state palautuu NEUTRAL-tilaan
- bos-laskurit nollautuvat
- structure_epoch_id kasvaa yhdellä

### 7) TREND_CHANGE

TREND_CHANGE-rivi syntyy aina, kun trend_state muuttuu pivot- tai reset-käsittelyn seurauksena.

## Analyysipäivän sääntö — no-lookahead

Tämä on kriittinen sääntö, jota on noudatettava aina kun Dow-rakennetta tulkitaan tiettyä analyysipäivää varten.

**Ainoat sallitut eventit analyysipäivälle ovat rivit, joilla:**

```
confirmed_as_of_date <= analysis_as_of_date
```

Ehto `event_date <= analysis_as_of_date` **ei riitä** yksin.

Miksi:

Pivotin event_date on pivotin todellinen päivä, mutta pivot ei olisi ollut vahvistettavissa ennen kuin vahvistusikkuna on täyttynyt. Confirmed_as_of_date on päivä, jolloin tieto olisi ollut oikeasti käytettävissä. Pelkän event_date:n käyttö johtaa tulevaisuuden tiedon vuotamiseen analyysiin (lookahead bias).

Esimerkki:

Pivot High event_date = 2024-03-10. Pivot radius = 3. Confirmed_as_of_date = 2024-03-15 (kolme kaupankäyntipäivää myöhemmin). Jos analyysipäivä on 2024-03-12, tätä pivotia ei saa käyttää, vaikka event_date on ennen analyysipäivää.

## Tapahtumataulu on event-pohjainen

stock_dow_structure_events sisältää vain rakenteelliset tapahtumat, ei jokaista päivää.

Siksi:

- Viimeisin event_date voi olla vanha, vaikka laskenta olisi ajan tasalla.
- Ajan tasalla -arvio ei perustu viimeisimpään event_date-arvoon.

## Ajan tasalla -tulkinta

Ajan tasalla -tulkinta tehdään status-taulun kentän calculated_through_date avulla.

Vertailukohta hinnan puolella on:

latest_valid_close_date = MAX(pvm), jossa close IS NOT NULL

Tämä on tärkeää, koska pelkkä MAX(pvm) voi viitata päivään, jolla close on NULL.

## Ticker-luokat puuttuvien ja vanhentuneiden laskennassa

Kun järjestelmä käy tickereitä läpi, ticker kuuluu yhteen seuraavista luokista:

- up_to_date
- outdated
- registered_without_status
- missing
- no_valid_close_data

Luokat selitettynä:

- up_to_date: status-rivi on olemassa ja calculated_through_date kattaa latest_valid_close_date
- outdated: status-rivi on olemassa, mutta calculated_through_date on vanhempi kuin latest_valid_close_date
- registered_without_status: event-rivejä on, mutta status-rivi puuttuu
- missing: ei event-rivejä eikä status-riviä
- no_valid_close_data: ticker on skoopissa, mutta yhtään close IS NOT NULL -riviä ei löydy

no_valid_close_data-käytös:

- ticker ohitetaan turvallisesti
- sille ei kirjoiteta event-rivejä
- sille ei kirjoiteta harhaanjohtavaa OK-statusriviä

## Laskentamoodit käytännössä

### Bounded initial

Käytetään tyypillisesti missing-tickerille.

Oletusraja:

- laskenta aloitetaan päivästä 2024-01-01
- sitä vanhemmat rivit eivät osallistu bounded initial -laskentaan

### Incremental

Käytetään tyypillisesti outdated- ja registered_without_status-tickereille.

Oletus:

- recalc_tail_trading_days = 30
- vanhoja rivejä poistetaan recalc-rajasta eteenpäin
- rakenne lasketaan tästä rajasta uudelleen

### Fallback full

Jos inkrementaalinen jatko ei ole luotettavasti rekonstruoitavissa, tehdään fallback full.

## Taulu 1: stock_dow_structure_events

Alla taulun skeema tulkintamuodossa.

| Kenttä | Tyyppi | Selitys |
|---|---|---|
| id | INTEGER | Juokseva tekninen tunniste |
| ticker | TEXT | Ticker, johon tapahtuma liittyy |
| market | TEXT | Markkina, esim. usa tai omxh |
| event_date | TEXT | Päivä, jolle itse rakenne-event kohdistuu |
| confirmed_as_of_date | TEXT | Päivä, jolloin event on vahvistettavissa |
| open | REAL | Open-arvo event-päivältä, taustatieto |
| high | REAL | High-arvo event-päivältä, taustatieto |
| low | REAL | Low-arvo event-päivältä, taustatieto |
| close | REAL | Close-arvo event-päivältä |
| volume | INTEGER | Volyymi event-päivältä |
| price_source | TEXT | Hintalähde, tässä close |
| structure_price | REAL | Rakennepäätöksessä käytetty hinta, käytännössä close |
| pivot_radius | INTEGER | Pivotin vahvistusikkunan säde |
| event_type | TEXT | PIVOT_HIGH, PIVOT_LOW, BOS_UP, BOS_DOWN, RESET, TREND_CHANGE |
| is_pivot_high | INTEGER | 1 jos event on Pivot High, muuten 0 |
| is_pivot_low | INTEGER | 1 jos event on Pivot Low, muuten 0 |
| pivot_high_date | TEXT | Pivot High -päivä, jos relevantti |
| pivot_high_price | REAL | Pivot High -hinta, jos relevantti |
| pivot_low_date | TEXT | Pivot Low -päivä, jos relevantti |
| pivot_low_price | REAL | Pivot Low -hinta, jos relevantti |
| dow_label_high | TEXT | Huipun label: H, HH, LH |
| dow_label_low | TEXT | Pohjan label: L, HL, LL |
| trend_state | TEXT | Trendin tila eventin jälkeen: UP, DOWN, NEUTRAL |
| active_bos_high_date | TEXT | DOWN-trendin aktiivisen BOS-tason päivämäärä |
| active_bos_high_price | REAL | DOWN-trendin aktiivinen BOS-taso |
| active_bos_low_date | TEXT | UP-trendin aktiivisen BOS-tason päivämäärä |
| active_bos_low_price | REAL | UP-trendin aktiivinen BOS-taso |
| last_high_label | TEXT | Viimeisin huippulabel |
| last_high_label_date | TEXT | Viimeisimmän huippulabelin päivä |
| last_high_label_price | REAL | Viimeisimmän huippulabelin hinta |
| last_low_label | TEXT | Viimeisin pohjalabel |
| last_low_label_date | TEXT | Viimeisimmän pohjalabelin päivä |
| last_low_label_price | REAL | Viimeisimmän pohjalabelin hinta |
| bos_up_count | INTEGER | Peräkkäisten BOS_UP-rikkomisten laskuri |
| bos_down_count | INTEGER | Peräkkäisten BOS_DOWN-rikkomisten laskuri |
| break_signal | TEXT | UP tai DOWN silloin, kun event on BOS tai RESET |
| break_level_date | TEXT | Rikotun rakennetason päivä |
| break_level_price | REAL | Rikotun rakennetason hinta |
| break_close_price | REAL | Sulkuhinta, jolla rikkominen tapahtui |
| reset_marker | TEXT | RESET-tapahtuman merkki, käytännössä R |
| reset_reason | TEXT | RESET-syy: DOUBLE_BOS_UP tai DOUBLE_BOS_DOWN |
| structure_epoch_id | INTEGER | Rakennerekisterin aikakausinumero, kasvaa RESETissä |
| structure_epoch_start_date | TEXT | Nykyisen epochin aloituspäivä |
| calc_version | TEXT | Laskentaversion tunniste, esim. stock_dow_v1 |
| run_id | TEXT | Yhden laskenta-ajon tunniste |
| created_at_utc | TEXT | Rivin luontiaika UTC:ssa |

Yksikäsitteisyysrajoite:

Yksi event-rivi on uniikki yhdistelmällä:

- ticker
- confirmed_as_of_date
- event_type
- event_date
- pivot_radius
- price_source

## Taulu 2: stock_dow_structure_status

Status-taulu kertoo laskennan kattavuuden ja viimeisimmän ajon yhteenvedon ticker-tasolla.

| Kenttä | Tyyppi | Selitys |
|---|---|---|
| ticker | TEXT | Ticker |
| market | TEXT | Markkina |
| price_source | TEXT | Hintalähde, tässä close |
| pivot_radius | INTEGER | Käytetty pivot-radius |
| calculated_from_date | TEXT | Mistä päivästä tämän ajon laskenta käytännössä alkoi |
| calculated_through_date | TEXT | Mihin päivään laskenta kattaa datan |
| latest_ohlcv_date_at_run | TEXT | Viimeisin validi OHLCV-päivä ajon hetkellä |
| latest_event_date | TEXT | Viimeisimmän kirjoitetun event-rivin event_date |
| latest_event_confirmed_as_of_date | TEXT | Viimeisimmän event-rivin confirmed_as_of_date |
| last_run_id | TEXT | Viimeisimmän ajon tunniste |
| last_run_mode | TEXT | Viimeisimmän ajon moodi, esim. incremental tai bounded_initial |
| last_rows_deleted | INTEGER | Kuinka monta vanhaa event-riviä poistettiin viime ajossa |
| last_rows_inserted | INTEGER | Kuinka monta event-riviä lisättiin viime ajossa |
| last_status | TEXT | Ajon tila, yleensä OK |
| last_error_message | TEXT | Viime virheen kuvaus, jos virhettä on |
| updated_at_utc | TEXT | Status-rivin viimeisin päivitysaika UTC |

Pääavain:

- ticker
- price_source
- pivot_radius

## Miten tauluja tulkitaan yhdessä

### 1) Onko ticker ajan tasalla

Käytä status-taulua:

- calculated_through_date kertoo kattavuuden
- vertaa sitä latest_valid_close_date-arvoon osakedata-taulusta

Älä käytä tähän latest_event_date tai latest_event_confirmed_as_of_date -kenttää.

### 2) Mitä trendi on analyysipäivänä

Etsi viimeisin event-rivi tickerille, jolla `confirmed_as_of_date <= analyysipäivä`. Järjestä uusimmasta vanhimpaan: `confirmed_as_of_date DESC, id DESC`.

- trend_state kertoo trenditulkinnan analyysipäivänä
- active_bos_high ja active_bos_low kertovat relevantin murtotason

SQL-esimerkki:

```sql
SELECT *
FROM stock_dow_structure_events
WHERE ticker = ?
  AND price_source = 'close'
  AND pivot_radius = 3
  AND confirmed_as_of_date <= ?
ORDER BY confirmed_as_of_date DESC, id DESC
LIMIT 1;
```

Huomio järjestyksestä: `id DESC` käytetään `event_date` sijaan, koska samalle confirmed_as_of_date-päivälle voi syntyä useampi eventti ja id kertoo kirjoitusjärjestyksen yksiselitteisemmin.

### 3) Milloin rakenne rikkoutui

Katso event_type:

- BOS_UP: DOWN-rakenteen yläraja rikottiin ylöspäin
- BOS_DOWN: UP-rakenteen alaraja rikottiin alaspäin
- RESET: toinen peräkkäinen rikkominen, rakenne nollataan

### 4) Milloin trendi vaihtui

TREND_CHANGE-rivit kertovat trend_state-muutoksista.

## Kentät, joita käytetään raportoinnissa usein

Usein hyödynnettävät event-kentät:

- ticker, market
- event_type
- event_date
- confirmed_as_of_date
- trend_state
- dow_label_high, dow_label_low
- break_signal, break_level_price, break_close_price
- structure_epoch_id

Usein hyödynnettävät status-kentät:

- calculated_through_date
- last_run_mode
- last_rows_deleted
- last_rows_inserted
- last_status
- updated_at_utc

## Coverage-statuksen tulkinta

Ennen kuin Dow-rakennetta tulkitaan analyysissa, on syytä tarkistaa, onko laskenta ajan tasalla ja luotettava. Alla kuvataan viisi tilaa, joihin ticker voi kuulua.

| Tila | Merkitys | Suositus |
|---|---|---|
| OK | calculated_through_date kattaa latest_valid_close_date | Voi käyttää analyysissa |
| STALE | calculated_through_date on vanhempi kuin latest_valid_close_date | Odota uudelleenlaskentaa ennen tulkintaa |
| MISSING_STATUS | Event-rivejä on, mutta status-rivi puuttuu | Käytä varoen; laskenta on olemassa mutta kattavuus tuntematon |
| NO_VALID_CLOSE_DATA | Tickerillä ei ole yhtään close IS NOT NULL -riviä | Älä käytä; hintalaskenta ei onnistu |
| ERROR | last_status ei ole OK | Tarkista last_error_message ennen käyttöä |

Miten OK-tila tarkistetaan käytännössä SQL:llä:

```sql
SELECT
    s.ticker,
    s.calculated_through_date,
    MAX(o.pvm) FILTER (WHERE o.close IS NOT NULL) AS latest_valid_close_date,
    CASE
        WHEN s.calculated_through_date >= MAX(o.pvm) FILTER (WHERE o.close IS NOT NULL)
            THEN 'OK'
        ELSE 'STALE'
    END AS coverage_status
FROM stock_dow_structure_status s
JOIN osakedata o ON UPPER(o.osake) = UPPER(s.ticker)
WHERE s.price_source = 'close'
  AND s.pivot_radius = 3
GROUP BY s.ticker, s.calculated_through_date;
```

**Huomio eri tietokannoista:** stock_dow_structure_status sijaitsee analysis.db:ssä ja osakedata on erillisessä osakedata.db-tiedostossa. Suora JOIN toimii vain, jos osakedata.db on liitetty ATTACH-komennolla samaan yhteyteen. Vaihtoehtoisesti latest_valid_close_date haetaan erikseen osakedata-kannasta ja verrataan calculated_through_date-arvoon ohjelmakoodissa.

## Miksi event_date voi olla vanha vaikka laskenta on ajan tasalla

Koska taulu tallentaa tapahtumia, ei päivittäistä tilannekuvaa.

Jos markkina liikkuu ilman uusia pivot- tai BOS-tapahtumia, uusia rivejä ei synny, vaikka laskenta olisi käynyt uusimman validin close-datan läpi.

## Fundamenttianalyysin tulkintamatriisi

Dow-rakenne on tekninen näkökulma hintakäyttäytymiseen. Se ei itse tuota ostosuositusta, mutta se antaa kontekstin sille, onko tekninen hintarakenne linjassa tai ristiriidassa fundamenttikuvan kanssa.

Taulukko on tarkoitettu neutraaliksi taustatulkinnaksi, ei normatiiviseksi ostosuositukseksi.

| Tilanne | Tulkinta |
|---|---|
| trend_state = UP | Hintarakenne tukee positiivista fundamenttikuvaa, jos fundamentit ovat myös vahvat |
| trend_state = DOWN | Hintarakenne on ristiriidassa positiivisen fundamenttikuvan kanssa tai varoittaa ajoitusriskistä |
| trend_state = NEUTRAL | Hintarakenne ei anna vahvaa vahvistusta kumpaankaan suuntaan |
| BOS_DOWN tai RESET (reason = DOUBLE_BOS_DOWN) lähellä analyysipäivää | Tekninen rakenne on heikentynyt; positiivinen fundamenttikuva ei poista ajoitusriskiä |
| BOS_UP tai RESET (reason = DOUBLE_BOS_UP) lähellä analyysipäivää | Laskeva rakenne on mahdollisesti murtumassa ylöspäin; voi vahvistaa käännekohtaa |
| Viimeisin event on vanha, trend_state = UP, kattavuus OK | Hintarakenne on säilynyt, ei uusia negatiivisia signaaleja |
| MISSING_STATUS tai STALE | Rakenteen tila tuntematon tai vanhentunut; älä käytä rakennetta fundamenttikuvan vahvistajana |

Lisähuomio ajoituksesta:

Sijoituspäätöksessä trend_state heijastaa mennyttä hintakäyttäytymistä analyysipäivään asti. Vasta kun uusia vahvistettuja pivoteja tai BOS-tapahtumia syntyy, rakenne päivittyy.

## Yhteenveto tulkitsijalle

- Rakenne on close-pohjainen.
- Event-taulu kertoo muutoksista, ei jokaisesta päivästä.
- Ajan tasalla -päätelmä tehdään status-taulun calculated_through_date-kentästä.
- no_valid_close_data tarkoittaa turvallista ohitusta, ei laskentavirhettä.
- RESET kertoo rakenteen nollauksesta kahden peräkkäisen murtuman jälkeen.
- structure_epoch_id auttaa jaksottamaan rakennetta reset-jaksoihin.

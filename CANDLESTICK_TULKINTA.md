# Kynttiläsignaalit - tulkintaopas tietokannan käyttäjälle

## Kenelle tämä dokumentti on

Tämä dokumentti on tarkoitettu henkilölle tai lukukerrokselle, joka tulkitsee kynttiläsignaaleja analyysin, raportoinnin tai muun päätöksenteon taustaksi.

Painotus on tulkinnassa, ei ohjelmakoodissa. Mukana on kuitenkin riittävästi täsmällisiä sääntöjä, jotta myös koneellinen lukukerros voi käyttää taulua turvallisesti.

## Mitä kynttiläsignaali tässä järjestelmässä tarkoittaa

Kynttiläsignaali on yksittäinen havainto, joka kirjataan päivälle, jos kyseisen päivän kynttilä tai useamman päivän kynttiläyhdistelmä täyttää ennalta määritellyn kuviosäännön.

Käytetyt peruskynttiläkuviot ovat:

- Hammer
- Bullish Engulfing
- Piercing Pattern
- Three White Soldiers
- Morning Star
- Dragonfly Doji
- Bearish Engulfing
- Shooting Star
- Dark Cloud Cover
- Evening Star
- Hanging Man

Lisäksi tauluun voidaan kirjoittaa yhdistelmäkuvioita, joissa samalle päivälle osuu sekä bullish divergence että tietty bullish kynttiläkuvio:

- BullDiv & Hammer
- BullDiv & Bullish Engulfing
- BullDiv & Piercing Pattern
- BullDiv & Three White Soldiers
- BullDiv & Morning Star
- BullDiv & Dragonfly Doji

## Tärkeä perusperiaate

`analysis_findings` on tapahtumataulu, ei päiväkohtainen kattavuustaulu.

Siksi:

- rivi syntyy vain, jos jokin kuvio löytyy
- rivin puuttuminen ei yksin kerro, että päivä olisi jäänyt analysoimatta
- rivin puuttuminen voi tarkoittaa myös sitä, ettei kyseisenä päivänä ollut yhtään ehtoja täyttävää kynttiläkuviota

Tämä on kriittinen ero verrattuna esimerkiksi `divergence_data`-tauluun, jossa on rivi jokaiselle käsitellylle päivälle.

## Laskennan periaatteet selkokielellä

### 1) Signaali kirjataan havaintopäivälle

Kynttiläsignaali kirjataan sille päivälle, jolla kuvio valmistuu.

Esimerkkejä:

- Hammer kirjataan samalle päivälle kuin Hammer-kynttilä
- Bullish Engulfing kirjataan toiselle päivälle, eli päivälle jolla engulfing valmistuu
- Morning Star kirjataan kolmannen kynttilän päivälle
- Evening Star kirjataan kolmannen kynttilän päivälle

Siksi `analysis_findings.date` on myös käytännössä signaalin käyttöpäivä.

### 2) Kynttiläkuviot käyttävät vain nykyistä ja aiempia kynttilöitä

Tässä toteutuksessa kynttiläkuviot eivät käytä tulevia päiviä. Siksi ne ovat turvallisia käyttää analyysipäivälle ehdolla:

```sql
date <= analysis_as_of_date
```

Tämä koskee myös useamman päivän kuvioita:

- kahden kynttilän kuviot käyttävät nykyistä ja edellistä päivää
- kolmen kynttilän kuviot käyttävät nykyistä ja kahta edellistä päivää

### 3) Bullish-kuvioiden laskutrendivaatimus riippuu analyysin asetuksista

Bullish-puolen peruskuvioille voidaan käyttää erillistä `downtrend_filter`-suodatinta.

Tärkeä täsmennys:

- bullish-kuvio ei itsessään aina vaadi laskutrendiä
- laskutrendivaatimus syntyy vain silloin, kun analyysi on ajettu `downtrend_filter=True`
- jos suodatin ei ole käytössä, sama bullish-kuvio voidaan kirjata myös ilman laskutrendikontekstia

Kun suodatin on käytössä, bullish-kuvio hyväksytään vain, jos taustalla on laskutrendi.

Käytännön ehdot:

- porrastava lasku: `t-10 > t-5 > t-2 > t0`
- minimilasku prosentteina
- valinnainen MA-suodatin: `t0 < MA(10)` ja `MA(5) < MA(10)`
- valinnainen volyymisuodatin

Tulkinta:

- bullish kynttiläsignaali ei aina tarkoita samaa asiaa ilman trendisuodatinta ja trendisuodattimen kanssa
- jos downtrend_filter on ollut käytössä analyysiajossa, bullish-kuvio tarkoittaa käänne- tai helpotussignaalia nimenomaan laskutrendin sisällä
- jos downtrend_filter ei ole ollut käytössä, kynttiläkuvio pitää tulkita yleisempänä hintakäyttäytymisen havaintona ilman varmaa trendikontekstia

Huomio `downtrend_filter`-tulkinnasta:

`analysis_findings`-taulu ei välttämättä sisällä tietoa siitä, millä analyysiasetuksilla rivi on syntynyt. Jos `downtrend_filter`-asetusta ei ole tallennettu tauluun tai saatavilla ajometadatasta, lukukerros ei saa päätellä sitä jälkikäteen.

Tällöin bullish-kuvio tulkitaan yleiseksi kynttilähavainnoksi ilman varmistettua laskutrendikontekstia.

### 4) Bearish-kuviot eivät tässä dokumentissa tarkoita automaattista myyntisignaalia

Bearish-kuviot ovat varoitus- tai heikkenemissignaaleja hintarakenteessa. Ne eivät yksinään tarkoita, että sijoitus pitäisi myydä.

### 5) Signal strength on vahvuusmitta välillä 0..1

Taulun `signal_strength` ei ole vain kyllä/ei-lippu, vaan kuvion vahvuusmitta.

Käytännön tulkinta:

- pieni arvo = heikompi tai epäselvempi kuvio
- suurempi arvo = selkeämpi ja voimakkaampi kuvio

Vahvuus perustuu kuvion geometrian kaltaisiin tekijöihin, kuten:

- rungon koko suhteessa koko kynttilän vaihteluväliin
- varjojen pituus
- joissakin tapauksissa volyymi

## Analyysipäivän sääntö — no-lookahead

Kun kynttiläsignaaleja tulkitaan tietylle analyysipäivälle, saa käyttää vain rivejä, joilla:

```sql
date <= analysis_as_of_date
```

Tämä on turvallinen sääntö, koska kynttiläkuviot käyttävät vain nykyistä ja aiempia kynttilöitä.

Käytännön seuraus:

- Hammer-päivää saa käyttää heti sen päivän päätöksen jälkeen
- Morning Star -signaalia ei saa käyttää ennen kuin kolmas kynttilä on valmis
- BullDiv & Hammer -yhdistelmäsignaalia ei saa käyttää ennen kuin myös saman päivän divergence-ehto on ollut käytettävissä analyysissa

## Coverage-tulkinta kynttilätaululle

Kynttiläsignaaleille ei ole erillistä status-taulua. Tämä on tärkeä rajoite.

Varsinainen sääntö on:

- `analysis_findings` ei yksin pysty todistamaan, että kaikki päivät on analysoitu analyysipäivään asti
- taulusta voi päätellä vain löytyneet signaalit, ei varmasti analysoidut signaalittomat päivät

Siksi koneelliselle lukukerrokselle turvallinen tulkinta on:

- `analysis_findings` kertoo **mitä havaittiin**
- se ei yksin kerro varmasti **mihin asti koko kynttiläanalyysi on ajettu**

Jos coverage pitää arvioida, siihen tarvitaan jokin ulkoinen lähde, esimerkiksi:

- ajoloki
- erillinen ajostatus
- ajettu analyysijakso sovellustasolla
- uusi laskenta juuri ennen raportin muodostusta

Käytännön varoitus:

- viimeisin `analysis_findings.date` ei ole coverage-päivä
- se on vain viimeisin päivä, jolta löytyi vähintään yksi signaali

## Taulu: analysis_findings

Kynttilä- ja muut löydökset tallennetaan tauluun `analysis_findings`.

| Kenttä | Tyyppi | Selitys |
|---|---|---|
| id | INTEGER | Juokseva tekninen tunniste |
| ticker | TEXT | Ticker |
| date | TEXT | Päivä, jolle havainto kirjataan |
| pattern | TEXT | Kuvion nimi |
| signal_strength | REAL | Kuvion vahvuus välillä 0..1 |
| rsi14 | REAL | Päivän RSI(14), jos tallennettu |
| created_at | TIMESTAMP | Rivin luontiaika |

Yksikäsitteisyysrajoite:

Tauluun on määritelty uniikkius yhdistelmälle:

- `ticker`
- `date`
- `pattern`

Tämä tarkoittaa käytännössä:

- samalle tickerille voi olla samalla päivällä useita eri kuvioita
- samaa kuviota ei kirjoiteta samalle tickerille ja päivälle kahta kertaa

## Kuvioluokat käytännössä

### Bullish-kynttiläkuviot

- Hammer
- Bullish Engulfing
- Piercing Pattern
- Three White Soldiers
- Morning Star
- Dragonfly Doji

Tulkinta:

- viittaavat mahdolliseen laskupaineen heikkenemiseen tai nousukäänteen alkuun
- ovat erityisen merkityksellisiä, jos analyysi on ajettu downtrend-suodattimen kanssa

### Bearish-kynttiläkuviot

- Bearish Engulfing
- Shooting Star
- Dark Cloud Cover
- Evening Star
- Hanging Man

Tulkinta:

- viittaavat mahdolliseen nousun heikkenemiseen tai myyntipaineen voimistumiseen

### Yhdistelmäkuviot

Yhdistelmäkuviot syntyvät, jos samalla päivällä on:

- bullish divergence
- ja jokin bullish peruskynttiläkuvio

Tulkinta:

- yhdistelmäsignaali on yleensä informatiivisempi kuin pelkkä kynttiläkuvio yksin
- se ei silti yksinään ole ostosuositus

V1-lukukerroksen rajaus:

Jos combo-rivi on valmiiksi tallennettu `analysis_findings`-tauluun, sen käyttöpäivä on `analysis_findings.date`. Lukukerroksen ei tarvitse rekonstruoida comboa `divergence_data`-taulusta V1-vaiheessa.

## Miten viimeisin käyttökelpoinen kynttiläsignaali haetaan SQL:llä

### Viimeisin mikä tahansa kynttiläsignaali analyysipäivään asti

```sql
SELECT *
FROM analysis_findings
WHERE ticker = ?
  AND date <= ?
  AND pattern IN (
    'Hammer',
    'Bullish Engulfing',
    'Piercing Pattern',
    'Three White Soldiers',
    'Morning Star',
    'Dragonfly Doji',
    'Bearish Engulfing',
    'Shooting Star',
    'Dark Cloud Cover',
    'Evening Star',
    'Hanging Man',
    'BullDiv & Hammer',
    'BullDiv & Bullish Engulfing',
    'BullDiv & Piercing Pattern',
    'BullDiv & Three White Soldiers',
    'BullDiv & Morning Star',
    'BullDiv & Dragonfly Doji'
  )
ORDER BY date DESC, id DESC
LIMIT 1;
```

### Viimeisin bullish kynttiläsignaali analyysipäivään asti

```sql
SELECT *
FROM analysis_findings
WHERE ticker = ?
  AND date <= ?
  AND pattern IN (
    'Hammer',
    'Bullish Engulfing',
    'Piercing Pattern',
    'Three White Soldiers',
    'Morning Star',
    'Dragonfly Doji',
    'BullDiv & Hammer',
    'BullDiv & Bullish Engulfing',
    'BullDiv & Piercing Pattern',
    'BullDiv & Three White Soldiers',
    'BullDiv & Morning Star',
    'BullDiv & Dragonfly Doji'
  )
ORDER BY date DESC, id DESC
LIMIT 1;
```

### Viimeisin bearish kynttiläsignaali analyysipäivään asti

```sql
SELECT *
FROM analysis_findings
WHERE ticker = ?
  AND date <= ?
  AND pattern IN (
    'Bearish Engulfing',
    'Shooting Star',
    'Dark Cloud Cover',
    'Evening Star',
    'Hanging Man'
  )
ORDER BY date DESC, id DESC
LIMIT 1;
```

## Signaalin iän tulkinta

Kynttiläsignaalille voidaan laskea ikä analyysipäivästä taaksepäin.

Ihmistulkinnassa käytännöllinen sääntö on:

- mitä lähempänä analyysipäivää signaali on, sitä ajankohtaisempi se on
- vanha kynttiläsignaali ilman jatkovahvistusta on enemmän historiallinen huomio kuin aktiivinen triggeri

Koneelliselle lukukerrokselle tämä kannattaa pitää neutraalina:

- lukukerros voi palauttaa signaalin iän kalenteripäivinä tai kaupankäyntipäivinä
- V1 ei vielä päätä, mikä ikä tekee kynttiläsignaalista aktiivisen tai vanhentuneen

Mahdollisia myöhempiä johdettuja mittareita:

- `signal_age_calendar_days`
- `signal_age_trading_days`
- `recent_window_days`

## Fundamenttianalyysin tulkintamatriisi

Kynttiläsignaali ei yksinään tuota ostosuositusta. Se antaa teknistä lisäkontekstia sille, tukeeko hintakäyttäytyminen fundamenttikuvaa vai ei.

| Tilanne | Tulkinta |
|---|---|
| Bullish kynttiläkuvio laskutrendin jälkeen | Voi tukea ajatusta siitä, että myyntipaine heikkenee |
| Bullish kynttiläkuvio ilman selvää laskutrendiä | Signaali on heikompi ja kontekstiriippuvaisempi |
| Bearish kynttiläkuvio nousun jälkeen | Varoittaa nousun heikkenemisestä tai ajoitusriskistä |
| BullDiv-combo samalla päivällä | Tekninen signaali on vahvempi kuin pelkkä kynttiläkuvio yksin |
| Useita bullish-kuvioita lyhyessä ajassa | Voi viitata pohjan rakentumiseen, mutta voi sisältää myös kohinaa |
| Ei kynttiläsignaalia, mutta fundamentit vahvat | Kynttilädata ei anna lisävahvistusta; päätös nojaa muihin signaaleihin |

Neutraali käytännön ohje:

- bullish kynttiläsignaali on useammin ajoituksen tukisignaali kuin itsenäinen ostoperuste
- bearish kynttiläsignaali on useammin riskivaroitus kuin itsenäinen myyntipäätös

## Koneelliselle lukukerrokselle suositeltu minimisääntö

Jos `analysis_findings`-taulua käytetään koneellisessa kynttilätulkinnassa, vähintään seuraavat säännöt kannattaa toteuttaa:

1. Käytä vain rivejä, joilla `date <= analysis_as_of_date`.
2. Erottele bullish-, bearish- ja combo-kuviot toisistaan.
3. Älä käytä viimeisintä `analysis_findings.date`-arvoa coverage-päivänä.
4. Jos coverage on kriittinen, varmista se erillisellä ajostatuksella tai uudelleenlaskennalla.
5. Käsittele `signal_strength`-arvoa prioriteettina, ei yksinään päätössääntönä.

## Yhteenveto tulkitsijalle

- `analysis_findings` on event-taulu: siihen tallentuvat vain löytyneet kuviot.
- Kynttiläsignaalin käyttöpäivä on taulun `date`.
- Kynttiläkuviot ovat no-lookahead-turvallisia ehdolla `date <= analyysipäivä`.
- Bullish-kuvioiden merkitys riippuu paljon siitä, oliko analyysissä käytössä laskutrendisuodatin.
- `analysis_findings` ei yksin todista analyysin kattavuutta.
- Combo-kuvio yhdistää bullish divergenssin ja bullish kynttiläsignaalin samalle päivälle.
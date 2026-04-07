# Temperature Bot v2

Bot modulare in Python per analizzare i mercati meteo temperatura su Polymarket.

## Obiettivo

Confrontare probabilita modellate dai provider meteo con probabilita implicite di mercato,
producendo segnali candidati spiegabili.

## Setup rapido

1. Crea e attiva un virtual environment Python.
2. Installa le dipendenze con `pip install -r requirements.txt`.
3. Configura le variabili locali in `.env` e `config/config.yaml`.

## Struttura

- `src/collectors`: raccolta dati meteo e mercati.
- `src/engine`: aggregazione, probabilita, confronto e policy segnali.
- `scripts`: script manuali di test e backtest.

## Nota

Le credenziali e i file locali sensibili non sono versionati.
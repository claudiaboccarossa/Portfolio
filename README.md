<p align="center">
  <img src="src/logo3.0.png" alt="PondLife" width="140">
</p>

<h1 align="center">PondLife TCG Collector</h1>

<p align="center">
  Web app per catalogare, valutare e gestire collezioni di carte collezionabili<br>
  <b>Magic: The Gathering</b> · <b>One Piece Card Game</b>
</p>

<p align="center">
  <a href="https://pond-life-tcg-collector.it/"><b>🌐 Sito live</b></a> ·
  <a href="https://pond-life-tcg-collector.it/magic/library">Catalogo Magic</a> ·
  <a href="https://pond-life-tcg-collector.it/onepiece/catalog">Catalogo One Piece</a> ·
  <a href="https://pond-life-tcg-collector.it/static/demo/analisi-demo-en.html">DEMO ANALYTICS</a>
</p>

---

## Il progetto

PondLife nasce come strumento personale per tenere traccia della mia collezione di Magic ed è diventato
un sito pubblico, con utenti registrati, due giochi supportati, un'app Android e un'interfaccia in italiano e inglese.

Il catalogo è consultabile senza account; registrandosi si possono gestire la propria collezione,
le wishlist, i mazzi e le statistiche sul valore delle carte.

## Funzionalità

**Catalogo**
- ~37.000 carte Magic e il catalogo One Piece, con filtri per set, nome, tipo, artista, colore e ordinamento
- Pagina carta con testo, prezzo normale e foil, storico del prezzo, altre edizioni e link d'acquisto (Cardtrader, TCGplayer)
- Elenco di tutte le espansioni con icone, tipo e data d'uscita

**Per gli utenti registrati**
- **Collezione** — quantità normali e foil, import da Moxfield, valore totale aggiornato
- **Analisi** — dashboard sul valore della collezione: distribuzione per colore, rarità ed espansione, con filtri interattivi
- **Wishlist** — liste pubbliche o private con priorità e condizione della carta
- **Deck builder Commander** — con suggerimenti di carte calcolati sul testo del comandante (TF-IDF + similarità coseno)
- **Binder** — vista a raccoglitore per completare i set
- **Bustine** — apertura giornaliera di bustine virtuali
- **Profilo, note di rilascio, supporto**

**Ovunque**
- Sito bilingue italiano / inglese
- PWA installabile e app Android (Trusted Web Activity), con navigazione dal basso e gesti su mobile

## Scelte tecniche

- **Dati da API esterne** — import e aggiornamento prezzi da Scryfall (Magic) e Cardtrader (One Piece), con storico dei prezzi versionato
- **Ottimizzazione del traffico DB** — il traffico verso Supabase superava il limite del piano (8,9 GB su 5).
  Filtri e paginazione spostati in SQL, selezione delle sole colonne necessarie e frontend riscritto per caricare le carte a pagine:
  le chiamate al catalogo sono passate da decine di migliaia di righe a 48 per volta
- **Multi-gioco** — schemi PostgreSQL separati per gioco (`magic`, `onepiece`) sopra uno schema `core` condiviso per gli utenti,
  URL con prefisso per gioco e template riusati
- **Internazionalizzazione** — testi in `translations/*.json` con un helper `t()` nei template; la lingua segue l'account, poi il cookie, poi il browser

## Stack

| | |
|---|---|
| Backend | Python 3.13, Flask, Flask-Login, SQLAlchemy 2.0 |
| Database | PostgreSQL su Supabase |
| Frontend | Jinja2, JavaScript vanilla, Plotly |
| ML | scikit-learn (suggerimenti del deck builder) |
| Email | Flask-Mailman, Brevo API |
| Deploy | Docker, Gunicorn |

## Struttura

```
├── app.py               # Entry point Flask e route
├── main.py              # Import dati da Scryfall
├── db/
│   ├── base.py          # Base SQLAlchemy condivisa
│   ├── core_orm.py      # Utenti (schema core)
│   ├── magic_orm.py     # Modelli Magic (schema magic)
│   ├── onepiece_orm.py  # Modelli One Piece (schema onepiece)
│   └── db_interaction.py# Caricamenti massivi
├── model/               # KPI della collezione, client Scryfall
├── handlers/            # Config, logging, utilità JSON
├── translations/        # Testi italiano / inglese
├── templates/           # Template Jinja2
└── android_app/         # Pacchetto dell'app Android
```

Lo schema completo del database è in [`src/erd_full.png`](src/erd_full.png).



# VibeKanban (fork personale)

Fork personale di [Vibe Kanban](https://github.com/BloopAI/vibe-kanban) (BloopAI, licenza Apache 2.0).

## Cosa fa

Vibe Kanban è una web app locale (backend Rust + frontend React) per orchestrare agenti di coding AI
(Claude Code, Codex, OpenCode, Gemini CLI, Copilot, Antigravity, ...) tramite una kanban board:

- crei issue/task e li assegni a un agente, che lavora in un **git worktree isolato** per ogni workspace;
- puoi eseguire più agenti in parallelo o in sequenza, seguirne i log in tempo reale e continuare la conversazione;
- revisioni il diff, avvii dev server, fai merge/push o apri PR direttamente dall'interfaccia;
- configurazione centralizzata di agenti, profili e server MCP.

Differenze principali rispetto all'upstream:

- board kanban locale su SQLite (nessun account/servizio remoto necessario);
- usa i binari **già installati nel sistema** (`claude`, `codex`, `opencode`, `agy`) invece di scaricarli via `npx`;
- executor **Antigravity** (`agy`) e profili **DeepSeek** tramite OpenCode con config isolata;
- executor Codex migrato al protocollo app-server v2;
- parser Claude aggiornato per Claude Code 2.1.x;
- follow-up degli agenti ACP tramite `session/load` e ripresa dei turni con comandi in background.

## Installazione su Windows

### 1. Dipendenze

| Strumento | Versione | Installazione |
|-----------|----------|---------------|
| Git for Windows (include Git Bash) | recente | `winget install Git.Git` |
| Visual Studio Build Tools – workload "Sviluppo desktop con C++" (MSVC + Windows SDK) | 2022+ | `winget install Microsoft.VisualStudio.2022.BuildTools` poi aggiungi il workload C++ dal Visual Studio Installer |
| Rust (rustup) | toolchain nightly fissata in `rust-toolchain.toml`, installata in automatico | `winget install Rustlang.Rustup` |
| Node.js | >= 18 (consigliato LTS) | `winget install OpenJS.NodeJS.LTS` |
| pnpm | >= 8 | `npm install -g pnpm` |
| Almeno un agente CLI, installato e autenticato | — | es. `npm i -g @anthropic-ai/claude-code` e poi `claude` per il login; idem per `codex`, `opencode`, `agy` |

Strumenti cargo aggiuntivi (servono per la modalità sviluppo):

```powershell
cargo install cargo-watch
cargo install sqlx-cli
```

### 2. Configura pnpm per usare Git Bash

Gli script in `package.json` usano sintassi bash (`export`, `$(...)`), quindi pnpm deve eseguirli con Git Bash:

```powershell
pnpm config set script-shell "C:\Program Files\Git\bin\bash.exe"
```

### 3. Clona e installa le dipendenze

```powershell
git clone https://github.com/Nicdelsan/VibeKanban.git
cd VibeKanban
pnpm install
```

### 4a. Avvio in modalità sviluppo

```powershell
pnpm run dev
```

Avvia backend (con hot-reload via `cargo watch`) e frontend Vite. La prima compilazione Rust richiede
parecchi minuti. L'URL del frontend viene stampato in console (porta di default 3000). In questa modalità
il database è in `dev_assets/` (copiato da `dev_assets_seed/` al primo avvio).

### 4b. Build "di produzione" (singolo eseguibile)

```powershell
cd frontend
pnpm build
cd ..
cargo build --release --bin server
.\target\release\server.exe
```

Il frontend viene incorporato nell'eseguibile. All'avvio il server stampa l'indirizzo su cui è in
ascolto (porta casuale, oppure fissala con `$env:PORT = "8080"` prima di lanciarlo). I dati
(database, configurazione) vengono salvati in `%APPDATA%\bloop\vibe-kanban\data\`.

Puoi copiare `server.exe` su un altro PC Windows: lì servono solo Git e gli agenti CLI, non Rust/Node.

## Variabili d'ambiente utili

| Variabile | Descrizione |
|-----------|-------------|
| `PORT` | Porta del server (in dev: porta del frontend, il backend usa PORT+1) |
| `HOST` | Host di ascolto, default `127.0.0.1` |
| `VK_ALLOWED_ORIGINS` | Origin consentite se esposto dietro reverse proxy / dominio custom |
| `DISABLE_WORKTREE_CLEANUP` | Disabilita la pulizia automatica dei worktree (debug) |

## Licenza

Apache 2.0 — vedi [LICENSE](LICENSE). Progetto originale © BloopAI.

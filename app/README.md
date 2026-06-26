# BullMQ Client

Cliente/dashboard **local** para conectar a **qualquer** aplicação BullMQ (Pro) e
inspecionar/gerenciar suas filas e grupos — sem rodar worker (não processa os jobs
da fila-alvo, só observa e gerencia).

O diferencial são os **scripts Lua** (`src/queues.ts`), que rodam direto no Redis e
cobrem o ponto cego que as ferramentas atuais não mostram: contagem e deleção de
jobs **delayed por grupo**.

## Como funciona

1. Você informa uma **Redis URL** (`redis://...` ou `rediss://...` com TLS) e um
   prefixo (padrão `bull`).
2. O backend descobre as filas varrendo chaves `<prefixo>:*:meta`.
3. Você escolhe uma fila e conecta — o dashboard passa a operar sobre ela.

## Rodar

```bash
cd app
npm install        # precisa do token do BullMQ Pro (via ~/.npmrc ou app/.npmrc)
npm run dev        # ts-node, hot start
# ou: npm run build && npm start
```

Abra **http://localhost:3001**, cole a Redis URL, clique em *Listar filas*, escolha
uma e *Conectar*. Porta configurável via `PORT`.

## Endpoints (API)

| Método | Rota | Descrição |
|--------|------|-----------|
| POST | `/api/queues/discover` | Lista filas de um Redis (`{redisUrl, prefix}`) |
| POST | `/api/connect` | Conecta a uma fila (`{redisUrl, prefix, queueName}`) |
| POST | `/api/disconnect` | Encerra a conexão ativa |
| GET  | `/api/connection` | Status da conexão |
| GET  | `/api/jobs` | Jobs por estado (waiting/active/delayed/paused/completed/failed) |
| POST | `/api/jobs` · `/api/jobs/batch` | Enfileira job(s) — playground |
| DELETE | `/api/jobs` | Esvazia a fila (obliterate) |
| GET  | `/api/groups` · `/api/groups/count` | Grupos (com delayed por grupo via Lua) |
| POST | `/api/groups/:id/pause` · `/resume` | Pausa/resume grupo |
| DELETE | `/api/groups/:id` | Deleta grupo (waiting nativo + delayed via Lua) |
| GET  | `/api/events` · `/api/events/stream` | Log de ações (SSE) |

## Notas

- **Sem worker:** este cliente nunca processa os jobs da aplicação-alvo. Os botões
  de *playground* (add/batch) servem só para testar contra filas descartáveis.
- **Filas não-Pro:** operações de grupo degradam graciosamente (retornam vazio) se a
  fila não usar BullMQ Pro. Os scripts Lua funcionam em qualquer fila.
- **Segredos:** o token do BullMQ Pro fica em `app/.npmrc` (gitignored). Nada de
  segredo vai para o repositório.

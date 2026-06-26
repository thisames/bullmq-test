# Deploy da app-alvo no Render (free) + Upstash

Objetivo: subir uma app BullMQ Pro que **produz jobs** num Redis público (Upstash),
para você conectar o **BullMQ Client** (local) nesse mesmo Redis e inspecionar.

```
[Render Web Service: demo-app] ──┐
                                 ├──> [Upstash Redis (rediss://, público)]
[BullMQ Client local :3001] ─────┘   conecta na MESMA URL
```

## 1. Criar o Redis no Upstash (grátis)
1. Conta em https://upstash.com → **Create Database** (Redis).
2. Escolha uma região (de preferência perto da região do Render que você usar).
3. Na página do banco, em **Connect**, copie a connection string **`rediss://`** (TLS).
   Formato: `rediss://default:SUA_SENHA@xxxx.upstash.io:6379`
   > ⚠️ Use a URL `rediss://` (TCP/ioredis). **NÃO** use a "REST URL" — o BullMQ não usa REST.

## 2. Colocar o código no GitHub
O Render faz deploy a partir de um repositório Git. Suba este repo para o GitHub
(pode ser público). O token do BullMQ Pro **não vai junto** — `.npmrc` está no
`.gitignore`. O token entra no Render via *Secret File* (passo 4).

## 3. Criar o Web Service no Render
**New + → Web Service →** conecte o repositório. Configure:
- **Root Directory:** `demo-app`
- **Runtime:** Node
- **Build Command:** `npm install && npm run build`
- **Start Command:** `npm start`
- **Instance Type:** Free

(Alternativa: **New → Blueprint** apontando para o repo — ele lê `demo-app/render.yaml`.)

## 4. Variáveis de ambiente + Secret File (token Pro)
Em **Environment**:
- `REDIS_URL` = a URL `rediss://...` do Upstash (marque como secret)
- `QUEUE_NAME` = `demo-queue`
- `QUEUE_PREFIX` = `bull`
- `ENABLE_WORKER` = `false`  (deixa os jobs acumulados; `true` processa devagar)

Em **Secret Files**, adicione um arquivo para o `npm install` autenticar no registry Pro:
- **Filename / Path:** `demo-app/.npmrc`
- **Contents:**
  ```
  @taskforcesh:registry=https://npm.taskforce.sh/
  //npm.taskforce.sh/:_authToken=SEU_TOKEN_TASKFORCE
  ```
  (mesmo conteúdo do seu `demo-app/.npmrc` local)

## 5. Deploy e verificação
- Faça o deploy. Nos **Logs** você deve ver:
  ```
  demo-app na porta 10000 — fila "demo-queue" (prefixo "bull")
  [seed] +12 jobs em group-a, group-b, group-c
  ```
- Acesse a URL pública do serviço em `/` → JSON de status com as contagens.

## 6. Conectar o BullMQ Client local
1. Rode o client: `cd app && npm run dev` → abra http://localhost:3001
2. Cole a **mesma URL `rediss://` do Upstash**, prefixo `bull`, clique **Listar filas**.
3. Selecione `demo-queue` → **Conectar** → veja os grupos e o delayed por grupo.
   (O client já trata `rediss://` com TLS automaticamente.)

## Notas
- **Não precisa de keep-alive** para inspecionar: o client fala direto com o Upstash,
  então mesmo se a app do Render hibernar, os jobs continuam lá. Só ligue um ping
  (UptimeRobot/cron-job.org em `/health`) se quiser que ela **continue gerando** jobs.
- **Consumo de comandos (Upstash free):** baixo, porque não há worker. Se ligar
  `ENABLE_WORKER=true`, o consumo sobe (worker é "tagarela").
- **Segurança:** a URL do Upstash dá acesso total ao Redis — trate como senha.

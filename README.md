# BullMQ Pro Test Suite

Dois mini projetos para testar funcionalidades do BullMQ Pro, especificamente o comportamento de pause/resume em jobs delayed e grupos.

## Estrutura

```
bullmq-test/
  backend/     - API Express com BullMQ Pro
  frontend/    - Dashboard HTML para visualizar jobs
```

## Pre-requisitos

1. **Redis** rodando localmente na porta 6379
   ```bash
   docker run -p 6379:6379 redis
   ```

2. **Token do BullMQ Pro** - Configure a variavel de ambiente:
   ```bash
   export NPM_TASKFORCESH_TOKEN=seu_token_aqui
   ```

## Setup Backend

```bash
cd backend
npm install
npm run dev
```

O backend ira rodar em `http://localhost:3001`

## Setup Frontend

```bash
cd frontend
npm install
npm run dev
```

O frontend ira rodar em `http://localhost:3000`

## Endpoints da API

| Metodo | Endpoint | Descricao |
|--------|----------|-----------|
| POST | /jobs | Adicionar um job |
| POST | /jobs/batch | Adicionar multiplos jobs |
| GET | /jobs | Listar todos os jobs por estado |
| POST | /groups/:groupId/pause | Pausar um grupo |
| POST | /groups/:groupId/resume | Resumir um grupo |
| GET | /groups/:groupId/status | Status de um grupo |
| POST | /worker/pause | Pausar o worker |
| POST | /worker/resume | Resumir o worker |
| DELETE | /jobs | Limpar a queue |
| GET | /events | Historico de eventos |
| GET | /events/stream | SSE para eventos em tempo real |

## Cenarios de Teste

### Cenario 1: Jobs Delayed + Pausar Grupo

1. Clique em "Teste 1" no dashboard
2. Observe os jobs sendo criados com delays escalonados
3. Os jobs aparecerao na coluna "Delayed"
4. Clique em "Pausar Grupo" enquanto os jobs ainda estao em delayed
5. Observe o que acontece com os jobs quando o delay expira

### Cenario 2: Multiplos Grupos

1. Clique em "Teste 2" no dashboard
2. Serao criados 3 grupos com diferentes configuracoes
3. Tente pausar/resumir grupos individualmente
4. Observe como os grupos sao processados independentemente

## O que observar

- **Jobs em Delayed**: Quando um grupo e pausado, os jobs que estao em delayed continuam com o timer rodando, mas quando o delay expira eles vao para um estado especial (nao sao processados enquanto o grupo estiver pausado)
- **Jobs em Waiting**: Jobs que ja estao em waiting nao serao processados enquanto o grupo estiver pausado
- **Grupos independentes**: Pausar um grupo nao afeta outros grupos
- **Worker pause vs Group pause**: Pausar o worker para tudo, pausar um grupo para apenas aquele grupo

## Tecnologias

- Backend: Node.js, TypeScript, Express, BullMQ Pro
- Frontend: HTML, CSS, JavaScript (vanilla)
- Banco: Redis

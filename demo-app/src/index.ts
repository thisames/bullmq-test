import express from 'express';
import { RedisOptions } from 'ioredis';
import { QueuePro, WorkerPro, JobPro } from '@taskforcesh/bullmq-pro';

// =====================================================================
// App BullMQ Pro de exemplo para rodar no Render e servir de ALVO do
// BullMQ Client. Ela PRODUZ jobs em grupos (com delays) e, por padrão,
// NÃO roda worker — assim os jobs ficam acumulados para inspeção e o
// consumo de comandos no Upstash fica baixo.
// =====================================================================

const PORT = Number(process.env.PORT) || 4000;
const REDIS_URL = process.env.REDIS_URL;
const QUEUE_NAME = process.env.QUEUE_NAME || 'demo-queue';
const QUEUE_PREFIX = process.env.QUEUE_PREFIX || 'bull';
const ENABLE_WORKER = process.env.ENABLE_WORKER === 'true';
const SEED_INTERVAL_MS = Number(process.env.SEED_INTERVAL_MS) || 10 * 60 * 1000;

if (!REDIS_URL) {
  console.error('ERRO: REDIS_URL não definido. Configure a connection string do Upstash (rediss://...).');
  process.exit(1);
}

function parseRedisOptions(url: string): RedisOptions {
  const u = new URL(url);
  return {
    host: u.hostname,
    port: u.port ? Number(u.port) : 6379,
    username: u.username ? decodeURIComponent(u.username) : undefined,
    password: u.password ? decodeURIComponent(u.password) : undefined,
    db: u.pathname && u.pathname.length > 1 ? Number(u.pathname.slice(1)) : 0,
    tls: u.protocol === 'rediss:' ? {} : undefined, // Upstash usa TLS (rediss://)
    maxRetriesPerRequest: null, // obrigatório para o BullMQ
  };
}

const connection = parseRedisOptions(REDIS_URL);
const queue = new QueuePro(QUEUE_NAME, { connection: { ...connection }, prefix: QUEUE_PREFIX });

const GROUPS = ['group-a', 'group-b', 'group-c'];

// Semeia jobs distribuídos entre grupos, misturando imediatos e delayed.
async function seed(count: number): Promise<void> {
  for (let i = 0; i < count; i++) {
    const group = GROUPS[i % GROUPS.length];
    const delayed = i % 2 === 0;
    const delayMin = 10 + (i % 6) * 10; // 10..60 min
    await queue.add(
      delayed ? 'scheduled-task' : 'task',
      { i, group, createdAt: new Date().toISOString() },
      { group: { id: group }, ...(delayed ? { delay: delayMin * 60 * 1000 } : {}) }
    );
  }
  console.log(`[seed] +${count} jobs em ${GROUPS.join(', ')}`);
}

// Só semeia se a fila estiver vazia (idempotente em restarts/wakes).
async function seedIfEmpty(): Promise<void> {
  try {
    const counts = await queue.getJobCounts();
    const total = Object.values(counts).reduce((acc: number, n) => acc + (Number(n) || 0), 0);
    let groupCount = 0;
    try {
      groupCount = (await queue.getGroups()).length;
    } catch {
      /* sem grupos ainda */
    }
    if (total === 0 && groupCount === 0) {
      await seed(12);
    }
  } catch (err) {
    console.error('[seed] erro:', (err as Error).message);
  }
}

// Worker OPCIONAL (desligado por padrão). Liga com ENABLE_WORKER=true.
let worker: WorkerPro | undefined;
if (ENABLE_WORKER) {
  worker = new WorkerPro(
    QUEUE_NAME,
    async (_job: JobPro) => {
      await new Promise((r) => setTimeout(r, 3000)); // processa devagar
      return { ok: true, at: new Date().toISOString() };
    },
    { connection: { ...connection }, prefix: QUEUE_PREFIX, concurrency: 2 }
  );
  worker.on('failed', (job, err) => console.error(`[worker] job ${job?.id} falhou:`, err.message));
  console.log('[worker] habilitado (concurrency 2)');
}

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/', async (_req, res) => {
  let counts: Record<string, number> = {};
  try {
    counts = (await queue.getJobCounts()) as Record<string, number>;
  } catch { /* noop */ }
  res.json({
    app: 'demo BullMQ Pro producer',
    queue: QUEUE_NAME,
    prefix: QUEUE_PREFIX,
    groups: GROUPS,
    workerEnabled: ENABLE_WORKER,
    counts,
    hint: 'Conecte o BullMQ Client neste mesmo Redis (Upstash) para inspecionar a fila.',
  });
});

// Semeia mais jobs sob demanda: POST /seed?count=12
app.post('/seed', async (req, res) => {
  const count = Number(req.query.count) || 12;
  await seed(count);
  res.json({ seeded: count });
});

app.listen(PORT, async () => {
  console.log(`demo-app na porta ${PORT} — fila "${QUEUE_NAME}" (prefixo "${QUEUE_PREFIX}")`);
  await queue.waitUntilReady();
  await seedIfEmpty();
  // Re-semeia se a fila ficar vazia (ex.: depois de você limpar pelo client)
  setInterval(seedIfEmpty, SEED_INTERVAL_MS);
});

process.on('SIGINT', async () => {
  console.log('\nEncerrando...');
  if (worker) await worker.close();
  await queue.close();
  process.exit(0);
});

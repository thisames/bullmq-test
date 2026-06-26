import express, { NextFunction, Request, Response } from 'express';
import cors from 'cors';
import path from 'path';
import { connect, disconnect, discoverQueues, getActive, getStatus } from './connection';
import {
  getAllJobs,
  getAllGroups,
  getGroupsCountByStatus,
  getGroupStatus,
  pauseGroup,
  resumeGroup,
  deleteGroupFull,
  addJob,
  clearAllJobs,
} from './queues';
import { eventStore, sseClients, addEvent } from './events';

const app = express();
app.use(cors());
app.use(express.json());

const PORT = Number(process.env.PORT) || 3001;

// Serve o dashboard estático (mesma origem da API -> sem CORS/URL hardcoded)
app.use(express.static(path.join(__dirname, '../public')));

// Exige uma conexão ativa para rotas de dados
function requireConnection(_req: Request, res: Response, next: NextFunction) {
  try {
    getActive();
    next();
  } catch (err) {
    res.status(409).json({ error: (err as Error).message });
  }
}

// Wrapper de erro padrão
function handle(fn: (req: Request, res: Response) => Promise<void>) {
  return async (req: Request, res: Response) => {
    try {
      await fn(req, res);
    } catch (err) {
      const status = (err as { statusCode?: number }).statusCode || 500;
      res.status(status).json({ error: (err as Error).message });
    }
  };
}

// ===== Conexão =====
app.get('/api/connection', (_req, res) => {
  res.json(getStatus());
});

app.post('/api/queues/discover', handle(async (req, res) => {
  const { redisUrl, prefix } = req.body;
  if (!redisUrl) throw Object.assign(new Error('redisUrl é obrigatório'), { statusCode: 400 });
  const queues = await discoverQueues(redisUrl, prefix || 'bull');
  res.json({ queues });
}));

app.post('/api/connect', handle(async (req, res) => {
  const { redisUrl, prefix, queueName } = req.body;
  if (!redisUrl || !queueName) {
    throw Object.assign(new Error('redisUrl e queueName são obrigatórios'), { statusCode: 400 });
  }
  await connect({ redisUrl, prefix: prefix || 'bull', queueName });
  addEvent('connected', { queueName, prefix: prefix || 'bull' });
  res.json(getStatus());
}));

app.post('/api/disconnect', handle(async (_req, res) => {
  await disconnect();
  addEvent('disconnected', {});
  res.json({ connected: false });
}));

// ===== Jobs =====
app.get('/api/jobs', requireConnection, handle(async (_req, res) => {
  res.json(await getAllJobs());
}));

app.post('/api/jobs', requireConnection, handle(async (req, res) => {
  const { name, data, delay, groupId } = req.body;
  const job = await addJob(name || 'manual-job', data || {}, { delay, groupId });
  res.json({ success: true, jobId: job.id });
}));

app.post('/api/jobs/batch', requireConnection, handle(async (req, res) => {
  const { count, groupId, delay, delayIncrement } = req.body;
  const jobIds: (string | undefined)[] = [];
  for (let i = 0; i < (count || 5); i++) {
    const jobDelay = delay ? delay + (delayIncrement || 0) * i : undefined;
    const job = await addJob(`batch-job-${i + 1}`, { index: i }, { delay: jobDelay, groupId });
    jobIds.push(job.id);
  }
  res.json({ success: true, jobIds });
}));

app.delete('/api/jobs', requireConnection, handle(async (_req, res) => {
  await clearAllJobs();
  res.json({ success: true, message: 'Fila esvaziada (obliterate)' });
}));

// ===== Grupos =====
app.get('/api/groups', requireConnection, handle(async (_req, res) => {
  res.json(await getAllGroups());
}));

app.get('/api/groups/count', requireConnection, handle(async (_req, res) => {
  res.json(await getGroupsCountByStatus());
}));

app.get('/api/groups/:groupId/status', requireConnection, handle(async (req, res) => {
  res.json(await getGroupStatus(req.params.groupId));
}));

app.post('/api/groups/:groupId/pause', requireConnection, handle(async (req, res) => {
  await pauseGroup(req.params.groupId);
  res.json({ success: true, message: `Grupo ${req.params.groupId} pausado` });
}));

app.post('/api/groups/:groupId/resume', requireConnection, handle(async (req, res) => {
  await resumeGroup(req.params.groupId);
  res.json({ success: true, message: `Grupo ${req.params.groupId} resumido` });
}));

app.delete('/api/groups/:groupId', requireConnection, handle(async (req, res) => {
  const result = await deleteGroupFull(req.params.groupId);
  res.json({ success: true, message: `Grupo ${req.params.groupId} deletado`, ...result });
}));

// ===== Eventos (log de ações do cliente) =====
app.get('/api/events', (_req, res) => {
  res.json(eventStore);
});

app.get('/api/events/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.listen(PORT, () => {
  console.log(`BullMQ client rodando em http://localhost:${PORT}`);
  console.log('Abra essa URL no navegador, cole a Redis URL e conecte a uma fila.');
});

process.on('SIGINT', async () => {
  console.log('\nEncerrando...');
  await disconnect();
  process.exit(0);
});

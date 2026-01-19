import express from 'express';
import cors from 'cors';
import {
  queue,
  worker,
  addJob,
  pauseGroup,
  resumeGroup,
  getAllJobs,
  getGroupStatus,
  getAllGroups,
  getGroupsCountByStatus,
  clearAllJobs,
  deleteGroupFull,
  eventStore,
  sseClients,
} from './queue.service';

const app = express();
app.use(cors());
app.use(express.json());

const PORT = 3001;

// Adicionar job
app.post('/jobs', async (req, res) => {
  try {
    const { name, data, delay, groupId } = req.body;
    const job = await addJob(name || 'default-job', data || {}, { delay, groupId });
    res.json({ success: true, jobId: job.id });
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Adicionar multiplos jobs de uma vez (para testes)
app.post('/jobs/batch', async (req, res) => {
  try {
    const { count, groupId, delay, delayIncrement } = req.body;
    const jobs = [];
    for (let i = 0; i < (count || 5); i++) {
      const jobDelay = delay ? delay + (delayIncrement || 0) * i : undefined;
      const job = await addJob(`batch-job-${i + 1}`, { index: i }, { delay: jobDelay, groupId });
      jobs.push(job.id);
    }
    res.json({ success: true, jobIds: jobs });
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Listar todos os jobs
app.get('/jobs', async (req, res) => {
  try {
    const jobs = await getAllJobs();
    res.json(jobs);
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Listar todos os grupos (deve vir antes de /groups/:groupId)
app.get('/groups', async (req, res) => {
  try {
    const groups = await getAllGroups();
    res.json(groups);
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Contagem de grupos por status (deve vir antes de /groups/:groupId)
app.get('/groups/count', async (req, res) => {
  try {
    const counts = await getGroupsCountByStatus();
    res.json(counts);
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Pausar um grupo
app.post('/groups/:groupId/pause', async (req, res) => {
  try {
    await pauseGroup(req.params.groupId);
    res.json({ success: true, message: `Group ${req.params.groupId} paused` });
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Resumir um grupo
app.post('/groups/:groupId/resume', async (req, res) => {
  try {
    await resumeGroup(req.params.groupId);
    res.json({ success: true, message: `Group ${req.params.groupId} resumed` });
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Status de um grupo
app.get('/groups/:groupId/status', async (req, res) => {
  try {
    const status = await getGroupStatus(req.params.groupId);
    res.json(status);
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Deletar um grupo completamente (waiting + delayed jobs)
app.delete('/groups/:groupId', async (req, res) => {
  try {
    const result = await deleteGroupFull(req.params.groupId);
    res.json({
      success: true,
      message: `Group ${req.params.groupId} deleted`,
      ...result
    });
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Pausar worker inteiro
app.post('/worker/pause', async (req, res) => {
  try {
    await worker.pause();
    res.json({ success: true, message: 'Worker paused' });
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Resumir worker
app.post('/worker/resume', async (req, res) => {
  try {
    await worker.resume();
    res.json({ success: true, message: 'Worker resumed' });
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Limpar queue
app.delete('/jobs', async (req, res) => {
  try {
    await clearAllJobs();
    res.json({ success: true, message: 'Queue cleared' });
  } catch (error: any) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Historico de eventos
app.get('/events', (req, res) => {
  res.json(eventStore);
});

// SSE para eventos em tempo real
app.get('/events/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  sseClients.add(res);
  console.log('[SSE] Client connected');

  req.on('close', () => {
    sseClients.delete(res);
    console.log('[SSE] Client disconnected');
  });
});

// Healthcheck
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
  console.log('Endpoints:');
  console.log('  POST /jobs - Add a job');
  console.log('  POST /jobs/batch - Add multiple jobs');
  console.log('  GET /jobs - List all jobs');
  console.log('  POST /groups/:groupId/pause - Pause a group');
  console.log('  POST /groups/:groupId/resume - Resume a group');
  console.log('  GET /groups/:groupId/status - Get group status');
  console.log('  POST /worker/pause - Pause worker');
  console.log('  POST /worker/resume - Resume worker');
  console.log('  DELETE /jobs - Clear all jobs');
  console.log('  GET /events - Get event history');
  console.log('  GET /events/stream - SSE stream');
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down...');
  await worker.close();
  await queue.close();
  process.exit(0);
});

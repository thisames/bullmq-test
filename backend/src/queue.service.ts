import { QueuePro, WorkerPro, JobPro, GroupStatus } from '@taskforcesh/bullmq-pro';
import { Response } from 'express';
import Redis from 'ioredis';

const connection = {
  host: 'localhost',
  port: 6379,
  maxRetriesPerRequest: null,
};

// Redis client para scripts Lua customizados
const redis = new Redis({ host: 'localhost', port: 6379 });

export const QUEUE_NAME = 'test-queue';
const QUEUE_PREFIX = `bull:${QUEUE_NAME}:`;

export const queue = new QueuePro(QUEUE_NAME, { connection });

// === Scripts Lua para operacoes eficientes ===

// Conta jobs delayed agrupados por grupo
const countDelayedGroupedScript = `
    local delayedKey = KEYS[1]
    local prefix = ARGV[1]
    
    local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
    local groups = {}
    
    for i, jobId in ipairs(jobIds) do
      local jobKey = prefix .. jobId
      local gid = redis.call('HGET', jobKey, 'gid')
      
      if gid then
        if not groups[gid] then
          groups[gid] = 0
        end
        groups[gid] = groups[gid] + 1
      end
    end
    
    local result = {}
    for gid, count in pairs(groups) do
      table.insert(result, gid)
      table.insert(result, count)
    end
    
    return result
`;

export async function getDelayedCountByGroup(): Promise<Map<string, number>> {
  const delayedKey = `${QUEUE_PREFIX}delayed`;
  const result = await redis.eval(
      countDelayedGroupedScript,
      1,
      delayedKey,
      QUEUE_PREFIX
  ) as string[];


  const map = new Map<string, number>();
  for (let i = 0; i < result.length; i += 2) {
    map.set(result[i], parseInt(result[i + 1]));
  }
  return map;
}

// Busca job IDs delayed de um grupo especifico
const getDelayedIdsByGroupScript = `
    local delayedKey = KEYS[1]
    local prefix = ARGV[1]
    local groupId = ARGV[2]
    
    local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
    local result = {}
    
    for i, jobId in ipairs(jobIds) do
      local jobKey = prefix .. jobId
      local gid = redis.call('HGET', jobKey, 'gid')
      
      if gid == groupId then
        table.insert(result, jobId)
      end
    end
    
    return result
`;

// Deleta jobs delayed de um grupo especifico (remove do ZSET delayed e deleta os dados do job)
const deleteDelayedByGroupScript = `
    local delayedKey = KEYS[1]
    local prefix = ARGV[1]
    local groupId = ARGV[2]
    
    local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
    local deletedCount = 0
    
    for i, jobId in ipairs(jobIds) do
      local jobKey = prefix .. jobId
      local gid = redis.call('HGET', jobKey, 'gid')
      
      if gid == groupId then
        -- Remover do sorted set delayed
        redis.call('ZREM', delayedKey, jobId)
        -- Deletar os dados do job
        redis.call('DEL', jobKey)
        deletedCount = deletedCount + 1
      end
    end
    
    return deletedCount
`;


const existsDelayedInGroupScript = `
    local delayedKey = KEYS[1]
    local prefix = ARGV[1]
    local targetGroupId = ARGV[2]
    
    local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
    
    for i, jobId in ipairs(jobIds) do
      local jobKey = prefix .. jobId
      local gid = redis.call('HGET', jobKey, 'gid')
      
      if gid == targetGroupId then
        return 1
      end
    end
    return 0
`;

export async function checkIfGroupHasDelayed(groupId: string): Promise<boolean> {
  const delayedKey = `${QUEUE_PREFIX}delayed`;

  const result = await redis.eval(
      existsDelayedInGroupScript,
      1,
      delayedKey,    // KEYS[1]
      QUEUE_PREFIX,  // ARGV[1]
      groupId        // ARGV[2]
  );

  return result === 1;
}

export async function getDelayedIdsByGroup(groupId: string): Promise<string[]> {
  const delayedKey = `${QUEUE_PREFIX}delayed`;
  return await redis.eval(
      getDelayedIdsByGroupScript,
      1,
      delayedKey,
      QUEUE_PREFIX,
      groupId
  ) as string[];
}

// Deleta jobs delayed de um grupo (usando Lua script)
export async function deleteDelayedByGroup(groupId: string): Promise<number> {
  const delayedKey = `${QUEUE_PREFIX}delayed`;
  return await redis.eval(
      deleteDelayedByGroupScript,
      1,
      delayedKey,
      QUEUE_PREFIX,
      groupId
  ) as number;
}

// Deleta TODOS os jobs de um grupo (waiting + delayed)
export async function deleteGroupFull(groupId: string): Promise<{ waitingDeleted: boolean; delayedDeleted: number }> {
  // 1. Deletar jobs waiting/prioritized do grupo (metodo nativo do BullMQ Pro)
  await queue.deleteGroup(groupId);

  // 2. Deletar jobs delayed do grupo (nosso script Lua customizado)
  const delayedDeleted = await deleteDelayedByGroup(groupId);

  console.log(`[Queue] Deleted group ${groupId}: delayed jobs removed = ${delayedDeleted}`);
  addEvent('group-deleted', { groupId, delayedDeleted });

  return { waitingDeleted: true, delayedDeleted };
}

// Store para eventos (para SSE)
export const eventStore: { timestamp: Date; type: string; data: any }[] = [];

function addEvent(type: string, data: any) {
  const event = { timestamp: new Date(), type, data };
  eventStore.unshift(event);
  if (eventStore.length > 100) {
    eventStore.pop();
  }
  // Notificar listeners SSE
  sseClients.forEach((client) => {
    client.write(`data: ${JSON.stringify(event)}\n\n`);
  });
}

// SSE clients
export const sseClients: Set<Response> = new Set();

// Worker que processa os jobs
export const worker = new WorkerPro(
    QUEUE_NAME,
    async (job: JobPro) => {
      console.log(`[Worker] Processing job ${job.id} - group: ${job.opts.group?.id || 'none'}`);
      addEvent('processing', {
        jobId: job.id,
        name: job.name,
        group: job.opts.group?.id,
        data: job.data,
      });

      // Simular trabalho (2 segundos)
      await new Promise((resolve) => setTimeout(resolve, 2000));

      console.log(`[Worker] Completed job ${job.id}`);
      addEvent('completed', {
        jobId: job.id,
        name: job.name,
        group: job.opts.group?.id,
      });

      return { result: 'done', processedAt: new Date().toISOString() };
    },
    {
      connection,
      concurrency: 5,
    }
);

// Event listeners do worker
worker.on('active', (job: JobPro) => {
  console.log(`[Event] Job ${job.id} is now active`);
  addEvent('active', { jobId: job.id, name: job.name, group: job.opts.group?.id });
});

worker.on('completed', (job: JobPro) => {
  console.log(`[Event] Job ${job.id} completed`);
});

worker.on('failed', (job: JobPro | undefined, err: Error) => {
  console.log(`[Event] Job ${job?.id} failed:`, err.message);
  addEvent('failed', { jobId: job?.id, error: err.message });
});

worker.on('paused', () => {
  console.log('[Event] Worker paused');
  addEvent('worker-paused', {});
});

worker.on('resumed', () => {
  console.log('[Event] Worker resumed');
  addEvent('worker-resumed', {});
});

// Funcoes de controle da queue/groups
export async function addJob(
    name: string,
    data: any,
    options: { delay?: number; groupId?: string } = {}
) {
  const jobOptions: any = {};

  if (options.delay) {
    jobOptions.delay = options.delay;
  }

  if (options.groupId) {
    jobOptions.group = { id: options.groupId };
  }

  const job = await queue.add(name, data, jobOptions);
  console.log(`[Queue] Added job ${job.id} - delay: ${options.delay}ms, group: ${options.groupId}`);
  addEvent('added', {
    jobId: job.id,
    name,
    data,
    delay: options.delay,
    group: options.groupId,
  });
  return job;
}

export async function pauseGroup(groupId: string) {
  console.log(`[Queue] Pausing group: ${groupId}`);
  await queue.pauseGroup(groupId);
  addEvent('group-paused', { groupId });
}

export async function resumeGroup(groupId: string) {
  console.log(`[Queue] Resuming group: ${groupId}`);
  await queue.resumeGroup(groupId);
  addEvent('group-resumed', { groupId });
}

export async function getAllJobs() {

  const [globalWaiting, active, allDelayed, completed, failed] = await Promise.all([
    queue.getWaiting(),
    queue.getActive(),
    queue.getDelayed(),
    queue.getCompleted(),
    queue.getFailed(),
  ]);

  // Pegar todos os grupos e seus jobs
  const allGroups = await queue.getGroups();

  // Pegar grupos pausados
  const pausedGroups = await queue.getGroupsByStatus(GroupStatus.Paused);
  const pausedGroupIds = new Set(pausedGroups.map(g => g.id));

  // Pegar jobs de cada grupo
  const groupJobsPromises = allGroups.map(async (group) => {
    const jobs = await queue.getGroupJobs(group.id);
    return { groupId: group.id, isPaused: pausedGroupIds.has(group.id), jobs };
  });
  const groupJobsResults = await Promise.all(groupJobsPromises);

  // Separar jobs de grupos em waiting (grupos ativos) e paused (grupos pausados)
  const waitingFromGroups: JobPro[] = [];
  const pausedFromGroups: JobPro[] = [];

  for (const { isPaused, jobs } of groupJobsResults) {
    if (isPaused) {
      pausedFromGroups.push(...jobs);
    } else {
      waitingFromGroups.push(...jobs);
    }
  }

  // Separar jobs delayed usando filter (mais limpo, mesma performance)
  const delayedFromPausedGroups = allDelayed.filter(job => {
    const groupId = job.opts.group?.id;
    return groupId && pausedGroupIds.has(groupId);
  });

  const delayedFromActiveGroups = allDelayed.filter(job => {
    const groupId = job.opts.group?.id;
    return !groupId || !pausedGroupIds.has(groupId);
  });

  // Combinar jobs paused: de grupos pausados + delayed de grupos pausados
  const allPausedJobs = [...pausedFromGroups, ...delayedFromPausedGroups];

  // Combinar waiting: global + de grupos ativos
  const allWaitingJobs = [...globalWaiting, ...waitingFromGroups];

  return {
    waiting: allWaitingJobs.map(formatJob),
    active: active.map(formatJob),
    delayed: delayedFromActiveGroups.map(formatJob),
    completed: completed.map(formatJob),
    failed: failed.map(formatJob),
    paused: allPausedJobs.map(formatJob),
  };
}

function formatJob(job: JobPro) {
  // Calcular delay restante (se o job ainda esta em delayed)
  let delayRemaining: number | undefined;
  if (job.opts.delay && job.timestamp) {
    const processAt = job.timestamp + job.opts.delay;
    delayRemaining = Math.max(0, processAt - Date.now());
  }

  return {
    id: job.id,
    name: job.name,
    data: job.data,
    group: job.opts.group?.id,
    delay: job.opts.delay,
    delayRemaining: delayRemaining,
    timestamp: job.timestamp,
    processedOn: job.processedOn,
    finishedOn: job.finishedOn,
    returnvalue: job.returnvalue,
    failedReason: job.failedReason,
  };
}

export async function getGroupStatus(groupId: string) {
  const status = await queue.getGroupStatus(groupId);
  return { groupId, status };
}

export async function getAllGroups() {
  const groups = await queue.getGroups();

  // --- Teste de Performance: checkIfGroupHasDelayed ---
  const startCheck = performance.now();

  // Testando para um grupo específico
  const hasDelayed = await checkIfGroupHasDelayed('group-a');

  const endCheck = performance.now();
  console.log(`[Performance] checkIfGroupHasDelayed('group-a') respondeu "${hasDelayed}" em: ${(endCheck - startCheck).toFixed(3)}ms`);
  // ----------------------------------------------------

  const startTimeCount = performance.now();
  const delayedCountByGroup = await getDelayedCountByGroup();
  const endTimeCount = performance.now();

  console.log(`[Performance] getDelayedCountByGroup (Todos os grupos) levou: ${(endTimeCount - startTimeCount).toFixed(3)}ms`);

  const groupsWithDetails = await Promise.all(
      groups.map(async (group) => {
        const waitingJobsCount = await queue.getGroupJobsCount(group.id);
        const delayedJobsCount = delayedCountByGroup.get(group.id) || 0;
        return {
          id: group.id,
          status: group.status,
          jobsCount: waitingJobsCount,
          delayedJobsCount: delayedJobsCount,
          totalJobsCount: waitingJobsCount + delayedJobsCount,
        };
      })
  );
  return groupsWithDetails;
}

export async function getGroupsCountByStatus() {
  return await queue.getGroupsCountByStatus();
}

export async function clearAllJobs() {
  await queue.obliterate({ force: true });
  addEvent('queue-cleared', {});
}
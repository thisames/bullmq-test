import { JobPro, GroupStatus } from '@taskforcesh/bullmq-pro';
import { getActive, keyPrefix } from './connection';
import { addEvent } from './events';

// =====================================================================
// Scripts Lua — o diferencial: rodam direto no Redis, então funcionam
// contra qualquer fila BullMQ e cobrem o ponto cego de "delayed por grupo".
// =====================================================================

// Conta jobs delayed agrupados por grupo -> [gid, count, gid, count, ...]
const countDelayedGroupedScript = `
    local delayedKey = KEYS[1]
    local prefix = ARGV[1]
    local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
    local groups = {}
    for i, jobId in ipairs(jobIds) do
      local gid = redis.call('HGET', prefix .. jobId, 'gid')
      if gid then
        groups[gid] = (groups[gid] or 0) + 1
      end
    end
    local result = {}
    for gid, count in pairs(groups) do
      table.insert(result, gid)
      table.insert(result, count)
    end
    return result
`;

// IDs de jobs delayed de um grupo específico
const getDelayedIdsByGroupScript = `
    local delayedKey = KEYS[1]
    local prefix = ARGV[1]
    local groupId = ARGV[2]
    local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
    local result = {}
    for i, jobId in ipairs(jobIds) do
      if redis.call('HGET', prefix .. jobId, 'gid') == groupId then
        table.insert(result, jobId)
      end
    end
    return result
`;

// Deleta jobs delayed de um grupo (remove do ZSET delayed e apaga o hash do job)
const deleteDelayedByGroupScript = `
    local delayedKey = KEYS[1]
    local prefix = ARGV[1]
    local groupId = ARGV[2]
    local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
    local deleted = 0
    for i, jobId in ipairs(jobIds) do
      local jobKey = prefix .. jobId
      if redis.call('HGET', jobKey, 'gid') == groupId then
        redis.call('ZREM', delayedKey, jobId)
        redis.call('DEL', jobKey)
        deleted = deleted + 1
      end
    end
    return deleted
`;

// Existe algum delayed no grupo? (early-return, mais leve)
const existsDelayedInGroupScript = `
    local delayedKey = KEYS[1]
    local prefix = ARGV[1]
    local targetGroupId = ARGV[2]
    local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
    for i, jobId in ipairs(jobIds) do
      if redis.call('HGET', prefix .. jobId, 'gid') == targetGroupId then
        return 1
      end
    end
    return 0
`;

export async function getDelayedCountByGroup(): Promise<Map<string, number>> {
  const { redis } = getActive();
  const prefix = keyPrefix();
  const result = (await redis.eval(countDelayedGroupedScript, 1, `${prefix}delayed`, prefix)) as string[];
  const map = new Map<string, number>();
  for (let i = 0; i < result.length; i += 2) {
    map.set(result[i], parseInt(result[i + 1], 10));
  }
  return map;
}

export async function checkIfGroupHasDelayed(groupId: string): Promise<boolean> {
  const { redis } = getActive();
  const prefix = keyPrefix();
  const result = await redis.eval(existsDelayedInGroupScript, 1, `${prefix}delayed`, prefix, groupId);
  return result === 1;
}

export async function getDelayedIdsByGroup(groupId: string): Promise<string[]> {
  const { redis } = getActive();
  const prefix = keyPrefix();
  return (await redis.eval(getDelayedIdsByGroupScript, 1, `${prefix}delayed`, prefix, groupId)) as string[];
}

export async function deleteDelayedByGroup(groupId: string): Promise<number> {
  const { redis } = getActive();
  const prefix = keyPrefix();
  return (await redis.eval(deleteDelayedByGroupScript, 1, `${prefix}delayed`, prefix, groupId)) as number;
}

// =====================================================================
// Operações de fila/grupos (degradam quando a fila não é BullMQ Pro)
// =====================================================================

function formatJob(job: JobPro) {
  let delayRemaining: number | undefined;
  if (job.opts.delay && job.timestamp) {
    delayRemaining = Math.max(0, job.timestamp + job.opts.delay - Date.now());
  }
  return {
    id: job.id,
    name: job.name,
    data: job.data,
    group: job.opts.group?.id,
    delay: job.opts.delay,
    delayRemaining,
    timestamp: job.timestamp,
    processedOn: job.processedOn,
    finishedOn: job.finishedOn,
    returnvalue: job.returnvalue,
    failedReason: job.failedReason,
  };
}

export async function getAllJobs() {
  const { queue } = getActive();

  const [globalWaiting, active, allDelayed, completed, failed] = await Promise.all([
    queue.getWaiting(),
    queue.getActive(),
    queue.getDelayed(),
    queue.getCompleted(),
    queue.getFailed(),
  ]);

  const waitingFromGroups: JobPro[] = [];
  const pausedFromGroups: JobPro[] = [];
  let pausedGroupIds = new Set<string>();

  // Grupos são recurso do BullMQ Pro — pode não existir na fila-alvo.
  try {
    const allGroups = await queue.getGroups();
    const paused = await queue.getGroupsByStatus(GroupStatus.Paused);
    pausedGroupIds = new Set(paused.map((g) => g.id));

    const results = await Promise.all(
      allGroups.map(async (g) => ({
        isPaused: pausedGroupIds.has(g.id),
        jobs: await queue.getGroupJobs(g.id),
      }))
    );
    for (const { isPaused, jobs } of results) {
      (isPaused ? pausedFromGroups : waitingFromGroups).push(...jobs);
    }
  } catch {
    /* fila sem grupos / não-Pro: segue só com os estados básicos */
  }

  const delayedFromPaused = allDelayed.filter((j) => {
    const g = j.opts.group?.id;
    return g && pausedGroupIds.has(g);
  });
  const delayedActive = allDelayed.filter((j) => {
    const g = j.opts.group?.id;
    return !g || !pausedGroupIds.has(g);
  });

  return {
    waiting: [...globalWaiting, ...waitingFromGroups].map(formatJob),
    active: active.map(formatJob),
    delayed: delayedActive.map(formatJob),
    completed: completed.map(formatJob),
    failed: failed.map(formatJob),
    paused: [...pausedFromGroups, ...delayedFromPaused].map(formatJob),
  };
}

export async function getAllGroups() {
  const { queue } = getActive();
  let groups;
  try {
    groups = await queue.getGroups();
  } catch {
    return []; // fila não-Pro / sem grupos
  }
  const delayedByGroup = await getDelayedCountByGroup();
  return Promise.all(
    groups.map(async (group) => {
      const jobsCount = await queue.getGroupJobsCount(group.id);
      const delayedJobsCount = delayedByGroup.get(group.id) || 0;
      return {
        id: group.id,
        status: group.status,
        jobsCount,
        delayedJobsCount,
        totalJobsCount: jobsCount + delayedJobsCount,
      };
    })
  );
}

export async function getGroupsCountByStatus() {
  const { queue } = getActive();
  try {
    return await queue.getGroupsCountByStatus();
  } catch {
    return {};
  }
}

export async function getGroupStatus(groupId: string) {
  const { queue } = getActive();
  const status = await queue.getGroupStatus(groupId);
  return { groupId, status };
}

export async function pauseGroup(groupId: string) {
  const { queue } = getActive();
  await queue.pauseGroup(groupId);
  addEvent('group-paused', { groupId });
}

export async function resumeGroup(groupId: string) {
  const { queue } = getActive();
  await queue.resumeGroup(groupId);
  addEvent('group-resumed', { groupId });
}

// Deleta TODOS os jobs de um grupo: waiting/prioritized (nativo) + delayed (Lua)
export async function deleteGroupFull(groupId: string): Promise<{ waitingDeleted: boolean; delayedDeleted: number }> {
  const { queue } = getActive();
  try {
    await queue.deleteGroup(groupId);
  } catch {
    /* não-Pro: cai só no delete de delayed via Lua */
  }
  const delayedDeleted = await deleteDelayedByGroup(groupId);
  addEvent('group-deleted', { groupId, delayedDeleted });
  return { waitingDeleted: true, delayedDeleted };
}

export async function addJob(
  name: string,
  data: unknown,
  options: { delay?: number; groupId?: string } = {}
) {
  const { queue } = getActive();
  const jobOptions: Record<string, unknown> = {};
  if (options.delay) jobOptions.delay = options.delay;
  if (options.groupId) jobOptions.group = { id: options.groupId };
  const job = await queue.add(name, data as object, jobOptions);
  addEvent('added', { jobId: job.id, name, group: options.groupId, delay: options.delay });
  return job;
}

export async function clearAllJobs() {
  const { queue } = getActive();
  await queue.obliterate({ force: true });
  addEvent('queue-cleared', {});
}

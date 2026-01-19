import Redis from 'ioredis';

const redis = new Redis({ host: 'localhost', port: 6379 });

// Script Lua para buscar jobs delayed por grupo
// Isso roda direto no Redis, muito mais eficiente
const getDelayedByGroupScript = `
local delayedKey = KEYS[1]
local prefix = ARGV[1]
local groupId = ARGV[2]
local start = tonumber(ARGV[3]) or 0
local stop = tonumber(ARGV[4]) or -1

-- Pegar todos os job IDs do sorted set delayed
local jobIds = redis.call('ZRANGE', delayedKey, start, stop)

local result = {}

for i, jobId in ipairs(jobIds) do
  local jobKey = prefix .. jobId
  local gid = redis.call('HGET', jobKey, 'gid')
  
  -- Se o grupo bate, adiciona ao resultado
  if gid == groupId then
    -- Pegar todos os dados do job
    local jobData = redis.call('HGETALL', jobKey)
    table.insert(result, jobId)
    table.insert(result, jobData)
  end
end

return result
`;

// Script para contar jobs delayed por grupo (mais leve, so conta)
const countDelayedByGroupScript = `
local delayedKey = KEYS[1]
local prefix = ARGV[1]
local groupId = ARGV[2]

local jobIds = redis.call('ZRANGE', delayedKey, 0, -1)
local count = 0

for i, jobId in ipairs(jobIds) do
  local jobKey = prefix .. jobId
  local gid = redis.call('HGET', jobKey, 'gid')
  
  if gid == groupId then
    count = count + 1
  end
end

return count
`;

// Script para pegar contagem de delayed agrupado por grupo
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

-- Converter para array de pares [groupId, count]
local result = {}
for gid, count in pairs(groups) do
  table.insert(result, gid)
  table.insert(result, count)
end

return result
`;

async function testLuaScripts() {
  const queueName = 'test-queue';
  const prefix = `bull:${queueName}:`;
  const delayedKey = `bull:${queueName}:delayed`;

  console.log('=== Testando scripts Lua ===\n');

  // Teste 1: Contar delayed por grupo
  console.log('1. Contagem de delayed por grupo (group-a):');
  const count = await redis.eval(
    countDelayedByGroupScript,
    1,
    delayedKey,
    prefix,
    'group-a'
  );
  console.log(`   Resultado: ${count} jobs\n`);

  // Teste 2: Contagem agrupada
  console.log('2. Contagem de delayed agrupada por todos os grupos:');
  const grouped = await redis.eval(
    countDelayedGroupedScript,
    1,
    delayedKey,
    prefix
  ) as string[];

  // Converter array plano para objeto
  const groupedObj: Record<string, number> = {};
  for (let i = 0; i < grouped.length; i += 2) {
    groupedObj[grouped[i]] = parseInt(grouped[i + 1]);
  }
  console.log('   Resultado:', groupedObj, '\n');

  // Teste 3: Buscar jobs de um grupo (primeiros 5)
  console.log('3. Jobs delayed do group-a (primeiros 5):');
  const jobs = await redis.eval(
    getDelayedByGroupScript,
    1,
    delayedKey,
    prefix,
    'group-a',
    '0',
    '4'
  ) as any[];

  // Parsear resultado
  for (let i = 0; i < jobs.length; i += 2) {
    const jobId = jobs[i];
    const jobDataArray = jobs[i + 1];
    // Converter array de pares para objeto
    const jobData: Record<string, string> = {};
    for (let j = 0; j < jobDataArray.length; j += 2) {
      jobData[jobDataArray[j]] = jobDataArray[j + 1];
    }
    console.log(`   Job ${jobId}:`, { name: jobData.name, gid: jobData.gid });
  }

  // Comparar performance
  console.log('\n=== Comparacao de performance ===\n');

  console.log('Metodo 1: getDelayed() + filter no JS');
  const start1 = Date.now();
  const allDelayed = await redis.zrange(delayedKey, 0, -1);
  let jsCount = 0;
  for (const jobId of allDelayed) {
    const gid = await redis.hget(`${prefix}${jobId}`, 'gid');
    if (gid === 'group-a') jsCount++;
  }
  const time1 = Date.now() - start1;
  console.log(`   Tempo: ${time1}ms, Count: ${jsCount}\n`);

  console.log('Metodo 2: Script Lua (tudo no Redis)');
  const start2 = Date.now();
  const luaCount = await redis.eval(
    countDelayedByGroupScript,
    1,
    delayedKey,
    prefix,
    'group-a'
  );
  const time2 = Date.now() - start2;
  console.log(`   Tempo: ${time2}ms, Count: ${luaCount}\n`);

  console.log(`Lua eh ${Math.round(time1 / time2)}x mais rapido!`);

  await redis.quit();
}

testLuaScripts().catch(console.error);

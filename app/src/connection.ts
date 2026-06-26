import IORedis, { RedisOptions } from 'ioredis';
import { QueuePro } from '@taskforcesh/bullmq-pro';

// Conexão ativa: o cliente conecta dinamicamente a UM Redis/fila por vez.
export interface ConnectionInfo {
  redisUrl: string;
  prefix: string;
  queueName: string;
}

export interface ActiveConnection extends ConnectionInfo {
  queue: QueuePro;
  redis: IORedis; // client cru, usado pelos scripts Lua e pela descoberta
}

let active: ActiveConnection | null = null;

export function parseRedisOptions(url: string): RedisOptions {
  let u: URL;
  try {
    u = new URL(url);
  } catch {
    throw new Error('Redis URL inválida. Use redis://host:porta ou rediss://user:senha@host:porta');
  }
  if (u.protocol !== 'redis:' && u.protocol !== 'rediss:') {
    throw new Error(`Protocolo inválido: "${u.protocol}". Use redis:// ou rediss://`);
  }
  return {
    host: u.hostname,
    port: u.port ? Number(u.port) : 6379,
    username: u.username ? decodeURIComponent(u.username) : undefined,
    password: u.password ? decodeURIComponent(u.password) : undefined,
    db: u.pathname && u.pathname.length > 1 ? Number(u.pathname.slice(1)) : 0,
    tls: u.protocol === 'rediss:' ? {} : undefined,
    // Obrigatório para o BullMQ não derrubar comandos bloqueantes
    maxRetriesPerRequest: null,
  };
}

// Esconde a senha ao devolver/logar a URL
export function maskUrl(url: string): string {
  try {
    const u = new URL(url);
    if (u.password) u.password = '***';
    return u.toString();
  } catch {
    return url;
  }
}

async function makeRedis(opts: RedisOptions, persistent = false): Promise<IORedis> {
  const redis = new IORedis({
    ...opts,
    lazyConnect: true,
    connectTimeout: 8000,
    // Descoberta/validação: falha rápido. Conexão persistente: reconecta sozinha.
    ...(persistent ? {} : { retryStrategy: () => null }),
  });
  // Sem listener, um erro de socket viraria unhandledRejection. O erro real
  // é capturado no connect()/ping() abaixo.
  redis.on('error', () => {});
  await redis.connect();
  await redis.ping();
  return redis;
}

// Descobre filas BullMQ varrendo chaves `<prefix>:*:meta` (stateless, não altera a conexão ativa).
export async function discoverQueues(url: string, prefix = 'bull'): Promise<string[]> {
  const redis = await makeRedis(parseRedisOptions(url));
  try {
    const names = new Set<string>();
    const match = `${prefix}:*:meta`;
    const suffix = ':meta';
    let cursor = '0';
    do {
      const [next, keys] = await redis.scan(cursor, 'MATCH', match, 'COUNT', 200);
      cursor = next;
      for (const key of keys) {
        // key === `${prefix}:<nome>:meta`
        const name = key.slice(prefix.length + 1, key.length - suffix.length);
        if (name) names.add(name);
      }
    } while (cursor !== '0');
    return Array.from(names).sort();
  } finally {
    redis.disconnect();
  }
}

export async function connect(info: ConnectionInfo): Promise<ActiveConnection> {
  await disconnect();
  const opts = parseRedisOptions(info.redisUrl);
  // Valida a conexão antes de subir o QueuePro (mensagem de erro mais clara)
  const redis = await makeRedis(opts, true);
  let queue: QueuePro;
  try {
    queue = new QueuePro(info.queueName, { connection: { ...opts }, prefix: info.prefix });
    await queue.waitUntilReady();
  } catch (err) {
    redis.disconnect();
    throw err;
  }
  active = { ...info, queue, redis };
  return active;
}

export async function disconnect(): Promise<void> {
  if (!active) return;
  const a = active;
  active = null;
  try { await a.queue.close(); } catch { /* noop */ }
  try { a.redis.disconnect(); } catch { /* noop */ }
}

export function getActive(): ActiveConnection {
  if (!active) {
    const err = new Error('Nenhuma conexão ativa. Conecte-se a um Redis primeiro.') as Error & { statusCode?: number };
    err.statusCode = 409;
    throw err;
  }
  return active;
}

export function getStatus() {
  if (!active) return { connected: false as const };
  return {
    connected: true as const,
    redisUrl: maskUrl(active.redisUrl),
    prefix: active.prefix,
    queueName: active.queueName,
  };
}

// Prefixo de chave da fila ativa, ex.: "bull:minha-fila:"
export function keyPrefix(): string {
  const a = getActive();
  return `${a.prefix}:${a.queueName}:`;
}

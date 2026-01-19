import Redis from 'ioredis';

const redis = new Redis({ host: 'localhost', port: 6379 });

async function inspectRedis() {
  // Ver a estrutura do delayed
  const delayedType = await redis.type('bull:test-queue:delayed');
  console.log('Tipo do delayed:', delayedType);

  // Pegar alguns jobs delayed
  const delayedJobs = await redis.zrange('bull:test-queue:delayed', 0, 5);
  console.log('Jobs delayed (IDs):', delayedJobs);

  // Ver estrutura de um job
  if (delayedJobs.length > 0) {
    const jobData = await redis.hgetall(`bull:test-queue:${delayedJobs[0]}`);
    console.log('Estrutura do job:', JSON.stringify(jobData, null, 2));

    // Ver se tem campo de grupo
    if (jobData.opts) {
      const opts = JSON.parse(jobData.opts);
      console.log('Opts do job:', opts);
    }
  }

  await redis.quit();
}

inspectRedis().catch(console.error);

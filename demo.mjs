// 跨进程验证：1 个 lead 投 10 封到共享队列，3 个独立 node 进程抢单。
// 期望：10 封全部被处理、无重复、无遗漏。
import { initBus, post, drain, claim } from './bus.mjs';
import { spawn } from 'node:child_process';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(tmpdir(), `agent-bus-demo-${randomUUID().slice(0, 8)}`);
await initBus(root);

// ① lead 投 10 封到共享队列
const posted = [];
for (let i = 0; i < 10; i += 1) {
  posted.push((await post(root, 'lead', 'pool', { goal: `job-${i}` })).taskId);
}
console.log(`lead 投递 ${posted.length} 封 → pool`);

// ② 起 3 个独立进程抢单
const workers = ['W1', 'W2', 'W3'].map((id) => new Promise((resolve, reject) => {
  const p = spawn(process.execPath, [join(here, 'worker.mjs'), root, id, 'pool'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let out = '';
  let err = '';
  p.stdout.on('data', (d) => { out += d; });
  p.stderr.on('data', (d) => { err += d; });
  p.on('close', (code) => {
    if (code !== 0) return reject(new Error(`${id} exit ${code}: ${err}`));
    try {
      resolve(JSON.parse(out.trim().split('\n').pop()));
    } catch (e) {
      reject(new Error(`${id} 输出不可解析: ${out}`));
    }
  });
}));

const stats = await Promise.all(workers);
console.log('worker 结果:', stats.map((s) => `${s.agent}=${s.handled}`).join(' '));

// ③ 校验：总数对得上、无重复
const handledAll = stats.flatMap((s) => s.taskIds);
const totalHandled = handledAll.length;
const uniqueHandled = new Set(handledAll).size;

// ④ lead 收结果
const results = await drain(root, 'lead');
const claimedResults = [];
while (true) {
  const r = await claim(root, 'lead');
  if (!r) break;
  claimedResults.push(r);
}

const problems = [];
if (totalHandled !== 10) problems.push(`处理总数 ${totalHandled} ≠ 10`);
if (uniqueHandled !== totalHandled) problems.push(`重复处理：unique ${uniqueHandled} < total ${totalHandled}`);
if (results.length !== 10) problems.push(`lead 收到 ${results.length} 封结果 ≠ 10`);
if (claimedResults.length !== 10) problems.push(`lead 领到 ${claimedResults.length} 封 ≠ 10`);
const missing = posted.filter((id) => !handledAll.includes(id));
if (missing.length) problems.push(`遗漏 ${missing.length} 封`);

if (problems.length) {
  console.error('DEMO FAILED:', problems.join('；'));
  process.exit(1);
}
console.log('DEMO OK —— 10 封任务被 3 个独立进程无重复无遗漏地处理完');
process.exit(0);

import { initBus, post, claim } from './bus.mjs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';

const N = 8;
const hist = new Map();
for (let i = 0; i < N; i += 1) {
  const root = join(tmpdir(), `stress-${randomUUID().slice(0, 8)}`);
  await initBus(root);
  await post(root, 'X', 'Y', { goal: `g${i}` });
  const racers = await Promise.all([claim(root, 'Y'), claim(root, 'Y'), claim(root, 'Y')]);
  const winners = racers.filter(Boolean).length;
  hist.set(winners, (hist.get(winners) ?? 0) + 1);
  if (winners !== 1) console.log(`第 ${i} 轮异常：赢家 ${winners} 个`);
}
console.log('赢家数分布:', Object.fromEntries([...hist.entries()].sort()));

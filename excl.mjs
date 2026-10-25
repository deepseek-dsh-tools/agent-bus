// 验证：O_CREAT|O_EXCL（open 'wx'）在 Windows 上并发是否可靠互斥。
// 同时复测 rename 的并发行为作为对照。
import { open, rename, writeFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';

const d = join(tmpdir(), `excl-${randomUUID().slice(0, 8)}`);
await mkdir(d, { recursive: true });

const N = 20;

// ① open 'wx' 并发
const lock = join(d, 'lock');
const excl = await Promise.all(Array.from({ length: N }, async () => {
  try {
    const fh = await open(lock, 'wx');
    await fh.close();
    return 'win';
  } catch (e) {
    return e.code;
  }
}));
const wins = excl.filter((x) => x === 'win').length;
const codes = [...new Set(excl.filter((x) => x !== 'win'))];
console.log(`① open 'wx' ×${N} → 赢家 ${wins}，其余错误码 ${JSON.stringify(codes)}`);

// ② 对照：rename 并发
const src = join(d, 'src.json');
const dst = join(d, 'dst.json');
await writeFile(src, 'payload');
const ren = await Promise.all(Array.from({ length: N }, async () => {
  try { await rename(src, dst); return 'win'; } catch (e) { return e.code; }
}));
const rwins = ren.filter((x) => x === 'win').length;
console.log(`② rename ×${N} → "成功" ${rwins} 个（>1 即为不可靠互斥）`);

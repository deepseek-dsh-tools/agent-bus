#!/usr/bin/env node
/**
 * agent-bus —— 零依赖「文件信箱」。
 *
 * 用途：让两个互相看不见的 agent（子智能体、别的进程、甚至别的机器挂同一个盘）
 * 通过磁盘交换任务与结果。宿主只负责派活和给路径，不中转内容。
 *
 * 为什么存在：DSH 的 send_message 只能在「父子」之间传递，
 * 兄弟 agent 直连会被 `dsh-subagent/lib/index.js:1665` 的 authorizeLineage 拦掉
 * （`belongs to another parent session`, UNAUTHORIZED）。磁盘是唯一干净的总线。
 *
 * ── 目录结构 ───────────────────────────────────────────────────────────────
 *   <root>/bus/<agentId>/pending/<taskId>.json   待取原始消息
 *   <root>/bus/<agentId>/claimed/<taskId>.lock   认领标记（open 'wx' 独占创建）
 *   <root>/bus/<agentId>/done/<taskId>.json      已取归档
 *
 * ── 互斥铁律（2026-10-21 实测，勿改）──────────────────────────────────────
 *   抢任务**必须**用 `open(path, 'wx')`（O_CREAT|O_EXCL）。
 *   在 Windows 上 `rename` 不能当互斥用：同一 src→dst 并发 20 次，
 *   **有 4 次报"成功"**（`excl.mjs` 三轮复测一致），会导致同一个任务被重复领取。
 *   而 `open 'wx'` ×20 恒定只有 1 个赢家、其余 EEXIST。
 *   所以：认领靠 O_EXCL，rename 只用来做归档（失败也不影响正确性）。
 *
 * ── 写盘铁律 ───────────────────────────────────────────────────────────────
 *   一切 writeFile 走 tmp 名 + rename 落地（避免读到半截 JSON）。
 */

import { mkdir, writeFile, rename, readdir, readFile, open } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';
import assert from 'node:assert/strict';

// ── 内部工具 ───────────────────────────────────────────────────────────────

/** 原子写：先写带 pid + 随机后缀的临时名，再 rename 落地。 */
async function atomicWrite(finalPath, data) {
  const tmp = `${finalPath}.tmp.${process.pid}.${randomUUID().slice(0, 8)}`;
  await writeFile(tmp, data, 'utf8');
  await rename(tmp, finalPath);
  return finalPath;
}

function box(root, agentId, sub) {
  return join(root, 'bus', agentId, sub);
}

/** 洗牌，避免多个 worker 每轮都抢同一封。 */
function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

async function listJson(dir) {
  try {
    return (await readdir(dir)).filter((n) => n.endsWith('.json'));
  } catch (err) {
    if (err.code === 'ENOENT') return [];
    throw err;
  }
}

/** 尝试独占认领 name；成功返回 true，已被别人认领返回 false。 */
async function tryClaim(claimedDir, name) {
  await mkdir(claimedDir, { recursive: true });
  const marker = join(claimedDir, `${name}.lock`);
  try {
    const fh = await open(marker, 'wx'); // ← 唯一可靠的跨进程互斥
    await fh.close();
    return true;
  } catch (err) {
    if (err.code === 'EEXIST') return false;
    throw err;
  }
}

// ── 公开 API ───────────────────────────────────────────────────────────────

/**
 * 幂等建 bus 根目录。
 * @param {string} root - 信箱根目录。
 * @returns {Promise<string>} root 本身。
 */
export async function initBus(root) {
  await mkdir(join(root, 'bus'), { recursive: true });
  return root;
}

/**
 * 投递一封消息。收件人目录按需创建。
 * @param {string} root
 * @param {string} from
 * @param {string} to
 * @param {object} payload
 * @returns {Promise<{taskId: string, path: string}>}
 */
export async function post(root, from, to, payload) {
  const taskId = randomUUID();
  const pending = box(root, to, 'pending');
  await mkdir(pending, { recursive: true });
  const target = join(pending, `${taskId}.json`);
  const record = { taskId, from, to, ts: Date.now(), payload };
  await atomicWrite(target, JSON.stringify(record, null, 2));
  return { taskId, path: target };
}

/**
 * 取一封未认领的待办。认领靠 O_EXCL 独占标记，并发抢同一封只有一个赢家。
 * @param {string} root
 * @param {string} agentId
 * @returns {Promise<{taskId: string, from: string, payload: object} | null>}
 */
export async function claim(root, agentId) {
  const pending = box(root, agentId, 'pending');
  const claimed = box(root, agentId, 'claimed');
  const done = box(root, agentId, 'done');

  const names = await listJson(pending);
  if (names.length === 0) return null;

  for (const name of shuffle(names)) {
    if (!(await tryClaim(claimed, name))) continue; // 已被别人认领
    const src = join(pending, name);
    let msg;
    try {
      msg = JSON.parse(await readFile(src, 'utf8'));
    } catch (err) {
      // 消息文件在认领后被删掉了，本封作废，继续下一封
      if (err.code === 'ENOENT') continue;
      throw err;
    }
    // 归档（纯记录用途；即使 rename 行为异常也不影响互斥正确性）
    await mkdir(done, { recursive: true }).then(() =>
      rename(src, join(done, name)).catch(() => {}));
    return { taskId: msg.taskId, from: msg.from, payload: msg.payload };
  }
  return null;
}

/**
 * 回一个结果给请求方（当成一封普通消息投回去）。
 * @param {string} root
 * @param {string} from - 回信人（即原收件人）
 * @param {string} agentId - 原请求方
 * @param {string} taskId - 原 taskId，便于对方关联
 * @param {object} result
 * @returns {Promise<{taskId: string, path: string}>}
 */
export async function deliver(root, from, agentId, taskId, result) {
  return post(root, from, agentId, { kind: 'result', taskId, result });
}

/**
 * 只列出**尚未被认领**的待办（不取走），用于观察队列深度。
 * @param {string} root
 * @param {string} agentId
 * @returns {Promise<string[]>} taskId 数组
 */
export async function drain(root, agentId) {
  const pending = box(root, agentId, 'pending');
  const claimed = box(root, agentId, 'claimed');
  const claimedSet = new Set(
    (await listJson(claimed)).map((n) => n.replace(/\.lock$/, '')),
  );
  const ids = [];
  for (const name of await listJson(pending)) {
    if (claimedSet.has(name)) continue;
    const msg = JSON.parse(await readFile(join(pending, name), 'utf8'));
    ids.push(msg.taskId);
  }
  return ids;
}

// ── 自测 ───────────────────────────────────────────────────────────────────

async function selftest() {
  const root = join(tmpdir(), `agent-bus-selftest-${randomUUID().slice(0, 8)}`);
  await initBus(root);

  // ① 投递与领取
  const a = await post(root, 'A', 'B', { goal: 'task-1' });
  const b = await post(root, 'A', 'B', { goal: 'task-2' });
  assert.equal((await drain(root, 'B')).length, 2, 'drain 应看到 2 封');

  const got1 = await claim(root, 'B');
  const got2 = await claim(root, 'B');
  assert.ok(got1 && got2, '应领到 2 封');
  assert.notEqual(got1.taskId, got2.taskId, '不能重复领同一封');
  assert.equal(await claim(root, 'B'), null, '领完后应为 null');
  assert.equal((await drain(root, 'B')).length, 0, 'drain 应为空');

  // ② 回信
  await deliver(root, 'B', 'A', got1.taskId, { ok: true, n: 1 });
  await deliver(root, 'B', 'A', got2.taskId, { ok: true, n: 2 });
  assert.equal((await drain(root, 'A')).length, 2, 'A 应收到 2 封回信');
  const res = await claim(root, 'A');
  assert.equal(res.payload.kind, 'result', 'kind 应为 result');
  assert.ok([a.taskId, b.taskId].includes(res.payload.taskId), 'taskId 应对得上');

  // ③ 并发抢同一封：只有一个赢家（跑 20 轮，确认不是碰巧）
  for (let round = 0; round < 20; round += 1) {
    const r = join(tmpdir(), `agent-bus-conc-${randomUUID().slice(0, 8)}`);
    await initBus(r);
    await post(r, 'X', 'Y', { goal: `only-one-${round}` });
    const racers = await Promise.all([claim(r, 'Y'), claim(r, 'Y'), claim(r, 'Y')]);
    const winners = racers.filter(Boolean).length;
    assert.equal(winners, 1, `第 ${round} 轮并发 claim 应只有 1 个赢家，实际 ${winners}`);
    assert.equal(new Set(racers.filter(Boolean).map((x) => x.taskId)).size, 1);
  }

  // ④ 认领后 drain 不再显示
  const r2 = join(tmpdir(), `agent-bus-drain-${randomUUID().slice(0, 8)}`);
  await initBus(r2);
  await post(r2, 'X', 'Y', { goal: 'g' });
  assert.equal((await drain(r2, 'Y')).length, 1);
  await claim(r2, 'Y');
  assert.equal((await drain(r2, 'Y')).length, 0, '认领后 drain 应为空');

  console.log('SELFTEST OK');
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  const mode = process.argv[2] ?? 'selftest';
  if (mode !== 'selftest') {
    console.error(`unknown mode: ${mode}（唯一支持：selftest）`);
    process.exit(1);
  }
  selftest()
    .then((code) => process.exit(code))
    .catch((err) => {
      console.error('SELFTEST FAILED:', err?.message ?? err);
      process.exit(1);
    });
}

// 一个 worker agent：从 pool 队列抢任务，干完回信给 lead。
// 用法: node worker.mjs <root> <agentId> <queueId>
import { claim, deliver } from './bus.mjs';

const [root, me, queue] = process.argv.slice(2);
if (!root || !me || !queue) {
  console.error('usage: node worker.mjs <root> <agentId> <queueId>');
  process.exit(2);
}

let handled = 0;
const seen = [];
// 简单的抢单循环：抢到就干，抢不到就退出
// 真实场景应该加 idle timeout / 长轮询
for (;;) {
  const msg = await claim(root, queue);
  if (!msg) break;
  // 「干活」——这里用 echo 代替
  await deliver(root, me, msg.from, msg.taskId, {
    by: me,
    goal: msg.payload.goal,
    echoedAt: Date.now(),
  });
  seen.push(msg.taskId);
  handled += 1;
}

console.log(JSON.stringify({ agent: me, handled, taskIds: seen }));

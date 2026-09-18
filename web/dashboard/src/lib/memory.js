import { getField } from './format.js';

export function instanceMemoryUsed(instance, engine) {
  return engine === 'kvrocks'
    ? getField(instance, 'memory_rss', 'MemoryRSS', null)
    : getField(instance, 'memory_used', 'MemoryUsed', null);
}

export function memoryMetricTitle(engine) {
  return engine === 'kvrocks' ? '进程驻留内存（RSS） / 容器内存限额' : 'Redis 分配内存（used_memory） / 容器内存限额';
}

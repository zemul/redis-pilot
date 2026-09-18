import test from 'node:test';
import assert from 'node:assert/strict';
import { instanceMemoryUsed, memoryMetricTitle } from '../src/lib/memory.js';

test('Redis allocated bytes and Kvrocks RSS remain distinct', () => {
 const sample = { memory_used: 100, memory_rss: 300 };
 assert.equal(instanceMemoryUsed(sample, 'redis'), 100);
 assert.equal(instanceMemoryUsed(sample, 'kvrocks'), 300);
 assert.equal(instanceMemoryUsed({ memory_used: 100 }, 'kvrocks'), null);
 assert.equal(instanceMemoryUsed(undefined, 'kvrocks'), null);
 assert.equal(instanceMemoryUsed({memory_rss: 0}, 'kvrocks'), 0);
 assert.match(memoryMetricTitle('kvrocks'), /RSS/);
});

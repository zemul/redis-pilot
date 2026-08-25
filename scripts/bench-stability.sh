#!/bin/bash
# 稳定性测试 - 持续写读验证可用性
# 用法: ./bench-stability.sh [host] [port]
# Ctrl+C 退出后输出统计

HOST=${1:-10.70.100.67}
PORT=${2:-16379}
OK=0
FAIL=0
START=$(date +%s)

trap 'END=$(date +%s); DURATION=$((END-START)); echo ""; echo "=== 统计 ==="; echo "运行时长: ${DURATION}s"; echo "成功: $OK"; echo "失败: $FAIL"; echo "成功率: $(echo "scale=4; $OK/($OK+$FAIL)*100" | bc)%"; exit' INT

echo "开始稳定性测试: $HOST:$PORT"
echo "Ctrl+C 退出"
echo ""

while true; do
  TS=$(date +%s%N)
  # 写入
  if ! redis-cli -h $HOST -p $PORT SET "bench:$TS" "hello" EX 300 > /dev/null 2>&1; then
    FAIL=$((FAIL + 1))
    echo "[$(date '+%H:%M:%S')] ❌ SET失败 ok=$OK fail=$FAIL"
    sleep 1
    continue
  fi
  # 读取
  if ! redis-cli -h $HOST -p $PORT GET "bench:$TS" > /dev/null 2>&1; then
    FAIL=$((FAIL + 1))
    echo "[$(date '+%H:%M:%S')] ❌ GET失败 ok=$OK fail=$FAIL"
    sleep 1
    continue
  fi
  OK=$((OK + 1))
  # 每1000次打个日志
  if [ $((OK % 1000)) -eq 0 ]; then
    echo "[$(date '+%H:%M:%S')] ✅ ok=$OK fail=$FAIL"
  fi
  sleep 0.1
done

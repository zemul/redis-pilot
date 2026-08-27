// 统一的 API 请求模块：手写 fetch，同源无 base 前缀。
// request 依赖两处运行时上下文——当前登录用户名（写入 X-Operator）和 401 登出回调——
// 因此以工厂函数注入，返回签名稳定的 request(path, options)。
import { authHeaders, clearStoredAuth } from '../Auth.jsx';

// createRequest 绑定运行时上下文，返回可直接调用的 request 函数。
//   operator: 当前登录用户名，用于审计的 X-Operator 头
//   onUnauthorized: 收到 401 时的回调（通常清空本地登录态并跳登录页）
export function createRequest({ operator, onUnauthorized } = {}) {
  return async function request(path, options = {}) {
    const headers = authHeaders();
    const response = await fetch(path, {
      ...options,
      headers: {
        ...headers,
        'X-Operator': operator || 'dashboard',
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
        ...(options.headers || {})
      }
    });
    if (response.status === 401) {
      clearStoredAuth();
      if (onUnauthorized) onUnauthorized();
      throw new Error('登录已过期，请重新登录');
    }
    const body = await response.json().catch(() => null);
    if (!response.ok) throw new Error(body?.error || `请求失败：${response.status} ${path}`);
    if (body?.error) throw new Error(body.error);
    return body?.data;
  };
}

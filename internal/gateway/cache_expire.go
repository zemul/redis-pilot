package gateway

// Kvrocks 2.15 only accepts the two-argument EXPIRE family. Implement the
// Redis conditions inside the same owner-fenced cold script, never as a
// read/check/write across connections. Native expiration applies the result.
const cacheConditionalExpireScript = `
local cmd=ARGV[2]
if (cmd=='EXPIRE' or cmd=='PEXPIRE' or cmd=='EXPIREAT' or cmd=='PEXPIREAT') and #ARGV>4 then
 local nx,xx,gt,lt=false,false,false,false
 for i=5,#ARGV do
  local flag=string.upper(ARGV[i])
  if flag=='NX' then nx=true elseif flag=='XX' then xx=true
  elseif flag=='GT' then gt=true elseif flag=='LT' then lt=true
  else return redis.error_reply('ERR unsupported expiration option') end
 end
 if nx and (xx or gt or lt) or gt and lt then return redis.error_reply('ERR incompatible expiration options') end
 local value=ARGV[4]
 local ttl=tonumber(value)
 local digits=string.gsub(value,'^%-','')
 if not ttl or not string.match(value,'^%-?%d+$') or (#digits>1 and string.sub(digits,1,1)=='0') or value=='-0' then
  return redis.error_reply('ERR value is not an integer or out of range')
 end
 -- Lua numbers cannot compare adjacent integers outside this range exactly.
 -- Refuse those conditional inputs before mutation rather than mis-evaluate.
 if math.abs(ttl)>9007199254740991 then return redis.error_reply('ERR conditional expiration exceeds exact millisecond range') end
 if cmd=='EXPIRE' or cmd=='EXPIREAT' then ttl=ttl*1000 end
 if math.abs(ttl)>9007199254740991 then return redis.error_reply('ERR conditional expiration exceeds exact millisecond range') end
 if cmd=='EXPIRE' or cmd=='PEXPIRE' then
  local now=redis.call('TIME')
  ttl=ttl+tonumber(now[1])*1000+math.floor(tonumber(now[2])/1000)
 end
 if math.abs(ttl)>9007199254740991 then return redis.error_reply('ERR conditional expiration exceeds exact millisecond range') end
 local previous=redis.call('PEXPIRETIME',KEYS[2])
 if previous==-2 then return 0 end
 if previous>9007199254740991 then return redis.error_reply('ERR existing expiration exceeds exact millisecond range') end
 if nx and previous~=-1 or xx and previous==-1 then return 0 end
 if gt and (previous==-1 or ttl<=previous) or lt and previous~=-1 and ttl>=previous then return 0 end
 a={cmd,KEYS[2],ARGV[4]}
end
`

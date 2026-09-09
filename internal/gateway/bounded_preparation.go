package gateway

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Fixed-width hexadecimal IDs are compared as strings: never Lua doubles.
// A single SET both records the preparation and retires its predecessor.
// An exact retry writes nothing, including after the business value changes.
const boundedPreparationScript = `
redis.replicate_commands()
if #KEYS ~= 2 or #ARGV ~= 2 then return redis.error_reply('ERR prepare arguments') end
local ok, wanted = pcall(cjson.decode, string.sub(ARGV[2], 9))
if not ok or type(wanted) ~= 'table' or wanted.version ~= 2
   or type(wanted.operation_id) ~= 'string' or #wanted.operation_id ~= 32
   or string.find(wanted.operation_id, '[^0-9a-f]')
   or type(wanted.previous_operation_id) ~= 'string' or #wanted.previous_operation_id ~= 32
   or string.find(wanted.previous_operation_id, '[^0-9a-f]')
   or wanted.operation_id <= wanted.previous_operation_id
   or string.sub(ARGV[2], 1, 8) ~= 'prepare:' then
  return redis.error_reply('ERR prepare sequence')
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  if wanted.previous_operation_id ~= string.rep('0', 32) then
    return redis.error_reply('ERR prepare predecessor missing')
  end
else
  if redis.call('TYPE', KEYS[2]).ok ~= 'string' or redis.call('PTTL', KEYS[2]) ~= -1 then
    return redis.error_reply('ERR prepare marker conflict')
  end
  local raw = redis.call('GET', KEYS[2])
  if raw == ARGV[2] then return 2 end
  local parsed, previous = pcall(cjson.decode, string.sub(raw, 9))
  if not parsed or type(previous) ~= 'table' or previous.version ~= 2
     or previous.operation_id ~= wanted.previous_operation_id
     or previous.task_id ~= wanted.task_id or previous.history ~= wanted.history
     or string.match(raw, '"epoch":(%d+)') ~= string.match(ARGV[2], '"epoch":(%d+)')
     or string.sub(raw, 1, 8) ~= 'prepare:' then
    return redis.error_reply('ERR prepare predecessor conflict')
  end
end
if redis.call('TYPE', KEYS[1]).ok ~= 'string' or redis.call('PTTL', KEYS[1]) ~= -1
   or redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return redis.error_reply('ERR prepare key changed')
end
redis.call('SET', KEYS[2], ARGV[2])
return 1
`

func preparationScript(i evictionIntent) string {
	if i.Version == 2 {
		return boundedPreparationScript
	}
	return hotPreparationScript
}

// The existing binding lock serializes producers. An unfinished predecessor
// cannot be replaced, so its cold confirmation never depends on overwritten
// source evidence. The operation ID is part of the caller's retry identity.
func (h *hotPreparation) configureBoundedIntent(i *evictionIntent) error {
	i.Version, i.PreviousOperationID = 2, strings.Repeat("0", 32)
	path := filepath.Join(h.options.Confirmation.OperationDir, "confirmation-operation.log")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if !i.valid() {
			return errEvictionOperation
		}
		return nil
	} else if err != nil {
		return err
	}
	op, err := openEvictionOperation(path, h.options.Confirmation.Binding.identity(), nil, nil)
	if err != nil {
		return err
	}
	defer op.close()
	previous, err := op.snapshot()
	if err != nil || !previous.Finished || previous.Intent.Version != 2 {
		return errEvictionOperation
	}
	i.PreviousOperationID = previous.Intent.OperationID
	if !i.valid() {
		return errEvictionOperation
	}
	return nil
}

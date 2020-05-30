local metrics = {"auth.meta_load", "all.meta_load", "req_rate", "queue_len", "cpu_load_avg"}

-- Metric for balancing is the workload; also dumps metrics
local function mds_load()
  for rank, mds in pairs(mds) do
    local s = "MDS"..rank..": < "
    for _, metric in ipairs(metrics) do
      s = s..metric.."="..mds[metric].." "
    end
    mds.load = mds["all.meta_load"]
    BAL_LOG(5, s.."> load="..mds.load)
  end
end

-- Shed load when you have load and your neighbor doesn't
local function when()
  local t = math.floor(( #mds - whoami + 1) / 2) + whoami
  if t > #mds then t = whoami end
  --while t ~= whoami and mds[t].load < .01 do t = t - 1 end
  while t ~= whoami and mds[t].load > .01 do t = t - 1 end
  --print("When: t=" .. t)
  my_load = mds[whoami].load
  his_load = mds[t].load
  if my_load > .01 and his_load < .01 then
    BAL_LOG(5, "when: migrating! my_load="..my_load.." hisload="..his_load)
  	return true, t
  end
  BAL_LOG(5, "when: not migrating! my_load="..my_load.." hisload="..his_load)
  return false, t
end

-- Shed half your load to your neighbor
-- neighbor=whoami+2 because Lua tables are indexed starting at 1
local function where(targets, t)
  targets[t] = mds[whoami]["load"]/2
  return targets
end

local targets = {}
for rank in pairs(mds) do
  targets[rank] = 0
end

mds_load()
local flag, t = when()
if flag then
  where(targets, t)
end

return targets

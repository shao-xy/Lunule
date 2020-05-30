#!/usr/bin/env lua

mds = {}

local function one_mds(authmeta, allmeta, reqrate, qlen, cpu)
	single_mds = {req_rate = reqrate, queue_len = qlen, cpu_load_avg = cpu}
	single_mds["auth.meta_load"] = authmeta
	single_mds["all.meta_load"] = allmeta
	return single_mds
end

local function gen_mds_1()
	mds = {}
	mds[1] = one_mds(50, 1000, 100, 50, 50)
	mds[2] = one_mds(50, 0, 100, 50, 50)
	mds[3] = one_mds(50, 1000, 100, 50, 50)
	mds[4] = one_mds(50, 100, 100, 50, 50)
	mds[5] = one_mds(50, 0, 100, 50, 50)
	return mds
end

local function gen_mds_2()
	mds = {}
	mds[1] = one_mds(50, 1000, 100, 50, 50)
	mds[2] = one_mds(50, 1000, 100, 50, 50)
	mds[3] = one_mds(50, 0, 100, 50, 50)
	mds[4] = one_mds(50, 0, 100, 50, 50)
	mds[5] = one_mds(50, 0, 100, 50, 50)
	return mds
end

local function gen_mds(config)
	local lut = {gen_mds_1, gen_mds_2}
	return lut[config]()
end

function BAL_LOG(level, msg)
	print("[" .. level .. "] " .. msg)
end

function gen_state_func()
	state = 0
	function RDState()
		return state
	end
	function WRState(n)
		state = n
	end
	return RDState, WRState
end

function parse_args()
	if #arg ~= 3 then
		print(string.format("Usage: lua %s <mds-config> <strategy> <whoami>", arg[0]))
		os.exit(-1)
	end
	local ts = arg[2]
	if string.sub(ts, string.len(ts) - 3, string.len(ts)) == ".lua" then
		ts = string.sub(ts, 1, string.len(ts) - 4)
	end
	return tonumber(arg[1]), ts, tonumber(arg[3])
end

RDState, WRState = gen_state_func()

mds_config, strategy, whoami = parse_args()
mds = gen_mds(mds_config)
print("Calculated targets for mds rank " .. whoami .. ":")
targets = require(strategy)
for rank, target in pairs(targets) do
	print(string.format("%3d => %3d", rank, target))
end

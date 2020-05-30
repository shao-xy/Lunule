#include <lua.hpp>
#include <string>
#include <sstream>
#include <fstream>
#include <map>

#include "common/debug.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

#define dout_subsys ceph_subsys_
#define dout_context g_ceph_context

#define mantle_dout dout
#define mantle_dendl dendl

static int dout_wrapper(lua_State *L)
{
	int level = luaL_checkinteger(L, 1);
	lua_concat(L, lua_gettop(L)-1);
	dout(level) << lua_tostring(L, 2) << dendl;
	return 0;
}

int main(int argc, const char ** argv)
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_ANY,
                   CODE_ENVIRONMENT_UTILITY,
                   0);

	// Read script from file here
	std::ifstream ifs("/home/ceph/test_mantle.lua");
	std::stringstream ss;
	ss << ifs.rdbuf();
	std::string s = ss.str();
	//dout(0) << "Script content: " << s << dendl;
	//boost::string_view script_s = s;
	std::vector<std::map<std::string, double>> metrics;
	std::map<int, double> my_targets;

	int whoami = 0;

	lua_State *L = luaL_newstate();
	if (!L) {
		dout(0) << "Could not create Lua VM." << dendl;
		return -1;
	}

	/* balancer policies can use basic Lua functions */
	//luaopen_base(L);
	//luaopen_coroutine(L);
	//luaopen_string(L);
	//luaopen_math(L);
	//luaopen_table(L);
	//luaopen_utf8(L);
	luaL_openlibs(L);

	/* setup debugging */
	lua_register(L, "BAL_LOG", dout_wrapper);

	lua_settop(L, 0); /* clear the stack */

	/* load the balancer */
	//if (luaL_loadstring(L, script_s.data())) {
	if (luaL_loadstring(L, s.c_str())) {
		mantle_dout(0) << "WARNING: mantle could not load balancer: "
						<< lua_tostring(L, -1) << mantle_dendl;
		return -EINVAL;
	}

	/* tell the balancer which mds is making the decision */
	lua_pushinteger(L, (lua_Integer)whoami);
	lua_setglobal(L, "whoami");

	/* global mds metrics to hold all dictionaries */
	lua_newtable(L);

	/* push name of mds (i) and its metrics onto Lua stack */
	for (size_t i=0; i < metrics.size(); i++) {
		lua_newtable(L);

		/* push values into this mds's table; setfield assigns key/pops val */
		for (const auto &it : metrics[i]) {
			lua_pushnumber(L, it.second);
			lua_setfield(L, -2, it.first.c_str());
		}

		/* in global mds table at stack[-3], set k=stack[-1] to v=stack[-2] */
		lua_seti(L, -2, i);
	}

	/* set the name of the global mds table */
	lua_setglobal(L, "mds");

	assert(lua_gettop(L) == 1);
	if (lua_pcall(L, 0, 1, 0) != LUA_OK) {
		mantle_dout(0) << "WARNING: mantle could not execute script: "
						<< lua_tostring(L, -1) << mantle_dendl;
		return -EINVAL;
	}

	/* parse response by iterating over Lua stack */
	if (lua_istable(L, -1) == 0) {
		mantle_dout(0) << "WARNING: mantle script returned a malformed response" << mantle_dendl;
		return -EINVAL;
	}

	/* fill in return value */
	for (lua_pushnil(L); lua_next(L, -2); lua_pop(L, 1)) {
		if (!lua_isinteger(L, -2) || !lua_isnumber(L, -1)) {
			mantle_dout(0) << "WARNING: mantle script returned a malformed response" << mantle_dendl;
			return -EINVAL;
		}
		int rank = lua_tointeger(L, -2);
		my_targets[rank] = lua_tonumber(L, -1);
	}
	return 0;
}

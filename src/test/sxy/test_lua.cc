#include <lua.hpp>
#include <string>
#include <sstream>
#include <fstream>

#include <iostream>
using std::cout;
using std::endl;

static int myadd(lua_State * L)
{
	double left = luaL_checknumber(L, 1);
	double right = luaL_checknumber(L, 2);
	lua_pushnumber(L, left + 2 * right);
	//return 1;
	//lua_pushnumber(L, luaL_checknumber(L, 2));
	
	double buffer[10] = {0.0};
	int total = 0;

	cout << "lua_gettop: " << lua_gettop(L) << endl;
	for (int i = 1; i <= lua_gettop(L) + 1; i++) {
		cout << i << '\t';
		if (!lua_isnumber(L, i)) {
			cout << "Not number" << endl;
			break;
		}
		buffer[i] = luaL_checknumber(L, i);
		cout << buffer[i] << endl;
		total += 1;
	}

	for (int i = 0; i < total; i++) {
		//lua_pushnumber(L, buffer[i]);
	}

	//return total;
	return 1;
}

static int myminus(lua_State * L)
{
	double left = luaL_checknumber(L, 1);
	double right = luaL_checknumber(L, 2);
	lua_pushnumber(L, left - right);
	return 1;
}

int main(int argc, char ** argv)
{
	// Read script from file here
	std::ifstream ifs("/home/ceph/test_mantle.lua");
	std::stringstream ss;
	ss << ifs.rdbuf();
	std::string s = ss.str();

	int error;
	//lua_State * L = lua_open();
	lua_State * L = luaL_newstate();
	//luaopen_base(L);
	//luaopen_table(L);
	//luaopen_io(L);
	//luaopen_string(L);
	//luaopen_math(L);
	luaL_openlibs(L);

	lua_register(L, "myadd", myadd);
	lua_register(L, "myminus", myminus);

	error = luaL_loadstring(L, s.c_str()) || lua_pcall(L, 0, 0, 0);
	if (error) {
		fprintf(stderr, "%s", lua_tostring(L, -1));
		lua_pop(L, 1);
	}

	lua_close(L);
	return 0;
}

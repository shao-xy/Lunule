#include <lua.hpp>
#include <string>
#include <sstream>
#include <fstream>

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

	error = luaL_loadstring(L, s.c_str()) || lua_pcall(L, 0, 0, 0);
	if (error) {
		fprintf(stderr, "%s", lua_tostring(L, -1));
		lua_pop(L, 1);
	}

	lua_close(L);
	return 0;
}

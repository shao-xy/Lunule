####################################################################
# The following source files are included in libcommon_files target
# 	DispatchQueuePatch.cc
# 	util.cc
####################################################################
add_library(adsl STATIC
	mig_co_req.cc
)
target_link_libraries(adsl common mds)

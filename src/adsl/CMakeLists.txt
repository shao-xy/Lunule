add_library(adsl STATIC
	util.cc
	mig_co_req.cc
)
target_link_libraries(adsl common mds)

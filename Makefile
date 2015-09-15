PROJECT=sblob
include erlang.mk
DEPS = lager

ERLFLAGS= -pa $(CURDIR)/.eunit -pa $(CURDIR)/ebin -pa $(CURDIR)/deps/*/ebin

# =============================================================================
# Verify that the programs we need to run are installed on this system
# =============================================================================
ERL = $(shell which erl)

ifeq ($(ERL),)
$(error "Erlang not available on this system")
endif

REBAR=./rebar

ifeq ($(REBAR),)
$(error "Rebar not available on this system")
endif

clean-eunit: clean
	rm -rf .eunit log

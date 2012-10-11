APPLICATION := app_cache

ERL := erl
EPATH := -pa ebin -pz deps/*/ebin
TEST_EPATH := -pa .eunit -pz deps/*/ebin 
PLT = .app_cache_plt

.PHONY: all doc clean test

all: compile

compile:
	@rebar compile

deps:
	@rebar get-deps

doc:
	@rebar skip_deps=true doc

clean:
	@rebar skip_deps=true clean

depclean:
	@rebar clean

distclean:
	@rebar delete-deps

build-plt:
	@dialyzer --build_plt --apps kernel stdlib sasl crypto ssl inets tools xmerl runtime_tools compiler syntax_tools mnesia

dialyze: compile
	@dialyzer -r ebin -r deps/*/ebin -Wno_undefined_callbacks

test: compile
	@rebar skip_deps=true ct verbose=1

console:
	$(ERL) -sname $(APPLICATION) $(EPATH) -config app

test-console: test
	$(ERL) -sname $(APPLICATION)_test $(TEST_EPATH) -config app


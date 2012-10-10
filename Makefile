APPLICATION := app_cache

ERL := erl
EPATH := -pa ebin -pz deps/*/ebin
TEST_EPATH := -pa .eunit -pz deps/*/ebin 

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

dialyze: compile
	@dialyzer -r src -r deps

test: compile
	@rebar skip_deps=true ct verbose=1

console:
	$(ERL) -sname $(APPLICATION) $(EPATH) -config app

test-console: test
	$(ERL) -sname $(APPLICATION)_test $(TEST_EPATH) -config app


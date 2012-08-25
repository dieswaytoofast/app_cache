app_cache
=========
Toolset that can be used to

1. Externalize the CRUD activities associated with mnesia
2. Provide a "write cache"" that will automatically persist data to secondary storage
3. Provide a "read cache" that will automatically pull data in from external sources
4. Provide a "transform cache" that will automatically transform data before each read and/or write

Details
=======
_app_cache_ simplifies the process of setting up and accessing your mnesia tables.  Of course, there is a bit of _win some lose some_ here, i.e., for absolutely performance and controllability, you're better off using mnesia directly. That said, we tend to use this for most of our own needs, dropping into mnesia only when necessary.

_app_cache_ stores information about your tables in the form of metadata, in a table called ``` app_metatable``` (big shocker in this name, eh?).  The metadata is stored in a record that looks like so

```erlang
-record(app_metatable, {
          table                                             :: table(),
          version = 1                                       :: table_version(),
          time_to_live = infinity                           :: time_to_live(),    %% in seconds
          type = set                                        :: table_type(),
          fields = []                                       :: [table_key()],
          secondary_index_fields = []                       :: [table_key()],
          read_transform_function                           :: function_identifier(),
          write_transform_function                          :: function_identifier(),
          refresh_function = #refresh_data{}                :: #refresh_data{},
          persist_function = #persist_data{}                :: #persist_data{},
          last_update = 0                                   :: non_neg_integer(),
          reason                                            :: any(),
          extras                                            :: any()
         }).
```

The important fields here are

Field | Description | Default
----- | ----------- | --------
table | Name of the table | required / no default
time_to_live | Cache expiration in integer seconds, or 'infinity' | infinity
type | The type of the table (set / ordered_set / bag) | set
fields | All the fields in the table | required / no default
secondary_index_fields | Any additional fields that need indexes on them | []
read_transform_function | Function used to transform the data _after_ the read, but _before_ you get at it | undefined
write_transform_function | Function used to transform the data _after_ you say write, but _before_ it gets written | undefined
refresh_function | A function to automaticaly "refresh" the data | #refresh_data{}
persist_function | A function to automatically "persist" the data to secondary storage | #persist_data{}

**NOTES**

1. **If** there is a field named ``` timestamp```, **then**, on each write, this field will get automagically updated with the current time in gregorian seconds.  Note that you can override this behavior by setting a value yourself - the auto-timestamping only happens if the value is ```timestamp```.
2. **If** the ```time_to_live``` is an integer, **then** records in the table will automatically get expired ```time_to_live``` seconds after the last time that record was updated. (**Not** _accessed_. _Updated_!)
3. ```function_identifier()``` is defined as either an anoymous function
```erlang
{function, fun foo() -> something_here() end}
```
or as a module_and_function 
```erlang
{module_and_function, {module_name, function_name}
```
Note that if you use an anonymous function, _it will *not* work across nodes!!!_

4. ```refresh_data{}``` is a record of the form
```erlang
-record(refresh_data, {
          before_each_read = false                          :: boolean(),
          after_each_read = false                           :: boolean(),
          refresh_interval = ?INFINITY                      :: time_to_live(),   % Seconds
          function_identifier                               :: function_identifier()
         }).
```
It defines the rules behind the automatic refreshing of the data in the cache from external sources, and is defined further below
5. ```persist_data{}``` is a record of the form
```erlang
-record(persist_data, {
          synchronous = false                               :: boolean(),
          function_identifier                               :: function_identifier()
         }).
```
It defines the rules behind the automatic persisting of data from the cache to secondary storage, and is defined further below

**RECORDS**

. ```#refresh_data{}```

Field | Description | Default
----- | ----------- | --------
function_identifier() | same as (3) above | undefined
refresh_interval | how frequently the function gets called | infinity (i.e., never)
before_each_read | _true_: The function gets synchronously called before each read, ensuring that the data in the cache is current. _false_: no function call per read | false
after_each_read | _true_: The function gets called asynchronously after each read. _false_: no function call per read | false

. ```#persist_data{}```

Field | Description | Default
----- | ----------- | --------
function_identifier() | same as (3) above | undefined
synchronous | _true_: This is a _write_through_ cache, i.e., each cache write is successful if, and only if, the persistance is successful. _false_: The write to persistence is asynchronous (but is triggered with each write) | false

**CAVEATS**

1. If ```#refresh_data.after_each_read =:= true```, the refresh_function is invoked asynchronously.  Multiple reads on the same key may (and probably **will**) not get the new data immediately.
2. For refreshing, you need to prime the pump. The automagic refreshing of a record only start _after_ the first read on that record (writing has no effect)
3. Deleting data which is associated with a refresher can be slow(er) - it needs to sycnchronize the delete with the removal of any stray refreshers.
3. ```#persist_function.function_identifier``` is considered to have failed if it throws an exception. 
	a. If an exception is thrown, and ```synchronous =:= true``` then the cache write will successfully rollback.  
	b. If ```synchronous =:= false```, there will be **no** rollback

Installation
============
Add this as a rebar dependency to your project.

1. The _dynarec_ parse_transform is **necessary**. You will find it in **priv/dynarec.erl**.  _You must include this file as part of your source._
2. _Run **app_cache:setup(Nodes)** at least once before starting your application_!!  This will do some basic house-keeping associated with setting up disc schemae for mnesia.
	a. **Nodes** is a list of all the nodes that you will be running mnesia on.  
	b. If you are running only one node, you can call  **app_cache:setup()** or **app_cache:setup([node()])**

Setup
=====
Set up a record of the form described in **DETAILS** above and call **app_cache:cache_init/1** with it as an argument. e.g

```erlang
app_cache:cache_init(#app_metatable{
                			table = foo_table_1,
                				version = 1, 
                				time_to_live = 60,
                				type = ordered_set,
                				fields = [key, timestamp, value, name],
                				secondary_index_fields = [name]}).
```

This will set up a table **foo_table_1**, with a time_to_live of **60**, of the type **ordered_set**, with the fields **[key, timestamp, value, name]**, and an additional index on the field **name**.

As an alternate, you can use the **helper* file **priv/app_cache_table_info.erl**.  Include it in your source, and use it as so:

```erlang
my_setup() ->
    app_cache:cache_init(get_metadata()).

%% Get the table definitions for app_cache
get_metadata() ->
    [get_table_info(Name) || Name <- [foo_table_1, foo_table_2]].

get_table_info(foo_table_1) ->
    app_cache_table_info:table_info(foo_table_1, ordered_set, 60);
get_table_info(foo_table_2) ->
    app_cache_table_info:table_info(foo_table_2, bag).
```
Credits
=======
This started as a variant of the mnesia accessors at [mlapi](https://github.com/jcomellas/mlapi) by [jcomellas](https://github.com/jcomellas/)

Much additional munging performed as part of [Spawnfest 2012](http://spawnfest.com/)
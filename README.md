app_cache
=========
Toolset that can be used to

1. Externalize the CRUD activities associated with mnesia
2. Provide cached and un-cached sequences
2. Provide a "write cache"" that will automatically persist data to secondary storage
3. Provide a "read cache" that will automatically pull data in from external sources
4. Provide a "transform cache" that will automatically transform data before each read and/or write

**WARNING**
**The ```timestamp``` field is treated specially. Read on…**

If your record does not contain a field called ```timestamp``` you can happily and safely ignore this paragraph.  If, however, it **does** contain a ```timestamp``` field, then you need to be aware that ```app_cache``` does fun things with this field.  In particular

1. If you write the record, but leave the timestamp as undefined - ```#my_record.timestamp =:= undefined``` - then _app_cache_ will automagically set it to the current time in gregorian seconds, i.e. ```calendar:datetime_to_gregorian_seconds(calendar:universal_time())```
2. It will automatically add an index to the timestamp field, allowing you to get data based on the index using ```get_data_from_index/3```

The point?
Primarily for use when you need timestamped data, but don't want to actually have to deal with the management of the timestamp data.  In particular

1. You want to search data by both the primary key (say, _user_id_), and by _timestamp_ (_Give me all of Joe's data_, vs, _Give me all data after this morning_)
2. You're tracking events with the same data but different timestamps (_Every time the doorbell rings, write an event_ followed up with a _How many times did the doorbell ring today_ query)



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

Mnesia Usage
=====
Create
------

Creating tables consists of calling ```app_cache:cache_init/1``` with a list of ```#app_metatable{}``` records as the argument up a record of the form described in **DETAILS** above and call **app_cache:cache_init/1** with it as an argument. e.g

```erlang
app_cache:cache_init([]#app_metatable{
                			table = foo_table_1,
                				version = 1, 
                				time_to_live = 60,
                				type = ordered_set,
                				fields = [key, timestamp, value, name],
                				secondary_index_fields = [name]}]).
```

This will set up a table ```foo_table_1```, with a time_to_live of ```60```, of the type ```ordered_set```, with the fields ```[key, timestamp, value, name]```, and an additional index on the field ```name```.

As an alternate, you can use the **helper** file ```priv/app_cache_table_info.erl```.  Include it in your source, and use it as so:

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

Write
-----
Assuming that the table actually exists, you can write to the table w/ the following functions (look at [the docs](http://dieswaytoofast.github.com/app_cache/) for more details)
Note that ```TransactionType``` is either **safe** or **dirty**.

1. **safe** will run the queries in 'transactional' mode - i.e., it'll either run through to completion, or fail entirely (with the exception of bag deletes documented elsewhere)
2. **dirty** will use mnesia's _dirty_ functions, which will be much (!) faster, but as you can imagine, can leave things in an inconsistent state on failure

Function | Parameters | Description
----- | ----------- | --------
set_data | Record | Write the record _Record_ to the table.<br>Note that the tablename is ```element(1, Record)```
set_data_overwriting_timestamp | Record | Set data, but ignore the _timestamp_ field, i.e., if there is an existing record which is identical in all parameters except for _timestamp_, then overwrite that record

**EXAMPLES**

```erlang
(app_cache@pecorino)70> app_cache:set_data(#test_table_2{
										key = foo1, 
										value = bar1}).
ok
(app_cache@pecorino)71> app_cache:set_data(#test_table_2{
										key = foo1, 
										value = bar1}).
ok
```
```erlang
(app_cache@pecorino)72> app_cache:get_data(test_table_2, foo1).
[#test_table_2{key = foo1,timestamp = 63513323628,
               value = bar1,name = undefined},
 #test_table_2{key = foo1,timestamp = 63513323636,
               value = bar1,name = undefined}]     
```
```erlang
(app_cache@pecorino)73> app_cache:set_data(#test_table_2{
										key = foo2, 
										value = bar2}).
ok
(app_cache@pecorino)74> app_cache:set_data_overwriting_timestamp(#test_table_2{
										key = foo2, 
										value = bar2}).
ok
(app_cache@pecorino)75> app_cache:get_data(test_table_2, foo2).                                           
[#test_table_2{key = foo2,timestamp = 63513323683,
               value = bar2,name = undefined}]
```


Read
----
Assuming that the table actually exists, you can read from the tables w/ the following functions (look at [the docs](http://dieswaytoofast.github.com/app_cache/) for more details)
Note that ```TransactionType``` is either **safe** or **dirty**.

1. **safe** will run the queries in 'transactional' mode - i.e., it'll either run through to completion, or fail entirely (with the exception of bag deletes documented elsewhere)
2. **dirty** will use mnesia's _dirty_ functions, which will be much (!) faster, but as you can imagine, can leave things in an inconsistent state on failure

Function | Parameters | Description
----- | ----------- | --------
get_data | Table, Key | Get all the records from the Table with the key Key
get_data_from_index | Table, Value, IndexField | Get all the records from the Table where  Value matches the value in field IndexField <p>e.g. get_data_from_index(test_table_1, "some thing here", value) where 'value' is an indexed field in test_table_1</p>
get_data_by_last_key | Table | Get the last Record (in erlang term order) in Table
get_data_by_first_key | Table | Get the first Record (in erlang term order) in Table
get_last_n_entries | Table, N | Get the last N entries (in erlang term order) in Table
get_first_n_entries | Table, N | Get the first N entries (in erlang term order) in Table
get_after | Table, After | Get all records in table with keys greater than or equal to After
get_records | Table, RecordN | Get any items in the table that (exactly) match Record.  <p>This is of particular use for ```bag```s with <i>timestamp</i> fields. If you pass in a record with ```timestamp =:= undefined```, you will get back all the records that match regardless of the timestamp
get_all_data | Table |Get all the data in Table

**EXAMPLES**

```erlang
(app_cache@pecorino)25> app_cache:set_data(#test_table_1{
										key = foo1, 
										value = bar1}).
ok      
(app_cache@pecorino)26> app_cache:set_data(#test_table_1{
										key = foo2, 
										value = bar2}).
ok
(app_cache@pecorino)27> app_cache:set_data(#test_table_1{
										key = foo3, 
										value = bar3}).
ok
```
```erlang
(app_cache@pecorino)30>app_cache:get_data(test_table_1, foo1).
[#test_table_1{key = foo1,timestamp = 63513306669,
               value = bar1,name = undefined}]
```
```erlang
(app_cache@pecorino)31> app_cache:get_data_by_last_key(test_table_1).
[#test_table_1{key = foo3,timestamp = 63513310206,
               value = bar3,name = undefined}]
```
```erlang
(app_cache@pecorino)55> app_cache:get_last_n_entries(test_table_1, 2).
[#test_table_1{key = foo3,timestamp = 63513310206,
               value = bar3,name = undefined},
 #test_table_1{key = foo2,timestamp = 63513310202,
               value = bar2,name = undefined}]
```
```erlang
(app_cache@pecorino)56> app_cache:get_first_n_entries(test_table_1, 2).
[#test_table_1{key = foo1,timestamp = 63513306669,
               value = bar1,name = undefined},
 #test_table_1{key = foo2,timestamp = 63513310202,
               value = bar2,name = undefined}]               
```
```erlang
(app_cache@pecorino)58> app_cache:get_after(test_table_1, foo2).       
[#test_table_1{key = foo2,timestamp = 63513310202,
               value = bar2,name = undefined},
 #test_table_1{key = foo3,timestamp = 63513310206,
               value = bar3,name = undefined}]
```
```erlang
(app_cache@pecorino)65> app_cache:get_records( #test_table_1{
										key = foo2, 
										value = bar2}).             
[#test_table_1{key = foo2,timestamp = 63513310202,
               value = bar2,name = undefined}]
```
```erlang
(app_cache@pecorino)62> app_cache:get_all_data(test_table_1).                       
[#test_table_1{key = foo1,timestamp = 63513306669,
               value = bar1,name = undefined},
 #test_table_1{key = foo2,timestamp = 63513310202,
               value = bar2,name = undefined},
 #test_table_1{key = foo3,timestamp = 63513310206,
               value = bar3,name = undefined}]
```
           
**NOTE**

1. In general, any read request above that involves _erlang term order_ will be performant when used on ```ordered_sets```, and probably not when used on ```sets``` or ```bags```

Delete
------
Assuming that the table actually exists, you can delete records from the table w/ the following functions (look at [the docs](http://dieswaytoofast.github.com/app_cache/) for more details)
Note that ```TransactionType``` is either **safe** or **dirty**.

1. **safe** will run the queries in 'transactional' mode - i.e., it'll either run through to completion, or fail entirely (with the exception of bag deletes documented elsewhere)
2. **dirty** will use mnesia's _dirty_ functions, which will be much (!) faster, but as you can imagine, can leave things in an inconsistent state on failure

Function | Parameters | Description
-------- | ---------- | --------
remove_data | Table, Key | Remove (all) the record(s) with key Key in Table
remove_all_data | Table | Remove _all_ the data in Table
remove_record | Record | Remove the record Record from the table specified as ```element(1, Record)```.  Note that _all_ the fields need to match
remove_record_ignoring_timestamp | Record | Remove the record Record ignoring any existing timestamp field. <p>The difference between this and ```remove_record``` is that if the record contains a ```timestamp``` field, it is ignored.</p> <p>This is of use w/ ```bags``` where you might have the same record w/ multiple timestamps, and you want them all gone</p>
 

**EXAMPLES**

```erlang
(app_cache@pecorino)70> app_cache:set_data(#test_table_2{
										key = foo1, 
										value = bar1}).
ok
(app_cache@pecorino)71> app_cache:set_data(#test_table_2{
										key = foo1, 
										value = bar1}).
ok
```
```erlang
(app_cache@pecorino)84> app_cache:get_data(test_table_2, foo1).                                                                    
[#test_table_2{key = foo1,timestamp = 63513331997,
               value = bar1,name = undefined},
 #test_table_2{key = foo1,timestamp = 63513331998,
               value = bar1,name = undefined}]
(app_cache@pecorino)85> app_cache:remove_record(#test_table_2{
										key = foo1, 
										timestamp = 63513331997, 
										value = bar1,
										name = undefined}).
ok
(app_cache@pecorino)86> app_cache:get_data(test_table_2, foo1).                                                                    
[#test_table_2{key = foo1,timestamp = 63513331998,
               value = bar1,name = undefined}]
```
```erlang
(app_cache@pecorino)86> app_cache:get_data(test_table_2, foo1).                                                                    
[#test_table_2{key = foo1,timestamp = 63513331998,
               value = bar1,name = undefined}]
(app_cache@pecorino)87> app_cache:remove_data(test_table_2, foo1).
ok
(app_cache@pecorino)88> app_cache:get_data(test_table_2, foo1).   
[]
```



Credits
=======
This started as a variant of the mnesia accessors at [mlapi](https://github.com/jcomellas/mlapi) by [jcomellas](https://github.com/jcomellas/)

Much additional munging performed as part of [Spawnfest 2012](http://spawnfest.com/)
%%%-------------------------------------------------------------------
%%% @author regupathy.b
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Oct 2015 1:00 PM
%%%-------------------------------------------------------------------
-module(ddb_tables_util).
-author("regupathy.b").

%% API
%% -export([]).
-compile([export_all]).

-include("json_keys.hrl").
-include("ddb_tables.hrl").
-include("ddb_actions.hrl").
-include("ddb_parameters.hrl").

%% create_table("TableName", [AttributeList], {Hash}|{Hash,Range})
create_table(TableName, AttributeList, Keys) ->
  Attributes = form_attributes(AttributeList,[]),
  KeySchema = form_key_schema(Keys),
  [
    {?TABLE_NAME, TableName},
    {?ATTRIBUTE_DEFINITIONS, Attributes},
    {?KEY_SCHEMA, KeySchema},
    {?PROVISIONED_THROUGHPUT, [{?WRITE_CAPACTY_UNITS, 1}, {?READ_CAPACITY_UNITS,1}]}
  ].


%% create_table_with_index("TableName", [AttributeList], {Hash}|{Hash,Range},
%% [{global,[{IndexName,Hash1,Range1}...]}, {local, [{IndexName,Hash1,Range1}...]}])
create_table_with_index(TableName, AttributeList, KeyList, IndexList) ->
  Attributes = form_attributes(AttributeList,[]),
  KeySchema = form_key_schema(KeyList),
  case form_indexes(IndexList) of
    {{Global, GlobalData}, {Local, LocalData}} ->
      [
        {?TABLE_NAME, TableName},
        {?ATTRIBUTE_DEFINITIONS, Attributes},
        {?KEY_SCHEMA, KeySchema},
        {Global, GlobalData},
        {Local, LocalData},
        {?PROVISIONED_THROUGHPUT, [{?WRITE_CAPACTY_UNITS, 1}, {?READ_CAPACITY_UNITS,1}]}
      ];
    {Index, Data} ->
      [
        {?TABLE_NAME, TableName},
        {?ATTRIBUTE_DEFINITIONS, Attributes},
        {?KEY_SCHEMA, KeySchema},
        {Index, Data},
        {?PROVISIONED_THROUGHPUT, [{?WRITE_CAPACTY_UNITS, 1}, {?READ_CAPACITY_UNITS,1}]}
      ]
  end.


form_attributes([], Acc) -> lists:reverse(Acc);
form_attributes([H|T], Acc) ->
  Attribute = [{?ATTRIBUTE_NAME, H}, {?ATTRIBUTE_TYPE, "S"}],
  form_attributes(T, [Attribute|Acc]).

form_key_schema(KeyList) ->
  case KeyList of
    {HashKey} ->
      [[{?ATTRIBUTE_NAME, HashKey},{?KEY_TYPE, ?HASH}]];
    {Hash,Range} ->
      [
        [{?ATTRIBUTE_NAME, Hash},{?KEY_TYPE, ?HASH}],
        [{?ATTRIBUTE_NAME, Range},{?KEY_TYPE, ?RANGE}]
      ]
  end.

form_global_index_key_schema([], Acc) -> lists:reverse(Acc);
form_global_index_key_schema([H|T], Acc) ->
  List = case H of
           {Index, HashKey} ->
             [
               {?INDEX_NAME, Index},
               {?KEY_SCHEMA, [[{?ATTRIBUTE_NAME, HashKey},{?KEY_TYPE, ?HASH}]]},
               {?PROJECTION, [{?PROJECTION_TYPE, "ALL"}]},
               {?PROVISIONED_THROUGHPUT, [{?WRITE_CAPACTY_UNITS,1},{?READ_CAPACITY_UNITS,1}]}
             ];
           {Index,Hash,Range} ->
             [
               {?INDEX_NAME, Index},
               {?KEY_SCHEMA, [
                 [{?ATTRIBUTE_NAME, Hash},{?KEY_TYPE, ?HASH}],
                 [{?ATTRIBUTE_NAME, Range},{?KEY_TYPE, ?RANGE}]
               ]},
               {?PROJECTION, [{?PROJECTION_TYPE, "ALL"}]},
               {?PROVISIONED_THROUGHPUT, [{?WRITE_CAPACTY_UNITS,1},{?READ_CAPACITY_UNITS,1}]}
             ]
         end,
  form_global_index_key_schema(T, [List|Acc]).

form_local_index_key_schema(_HashKey, [], Acc) -> lists:reverse(Acc);
form_local_index_key_schema(HashKey, [H|T], Acc) ->
  {Index, RangeKey} = H,
  List = [
    {?INDEX_NAME, Index},
    {?KEY_SCHEMA, [
      [{?ATTRIBUTE_NAME, HashKey},{?KEY_TYPE, ?HASH}],
      [{?ATTRIBUTE_NAME, RangeKey},{?KEY_TYPE, ?RANGE}]
    ]},
    {?PROJECTION, [{?PROJECTION_TYPE, "ALL"}]}
  ],
  form_local_index_key_schema(HashKey, T, [List|Acc]).


%% [{global,[{IndexName,Hash1,Range1}...]}, {local,Hash, [{IndexName,Range1}...]}]
form_indexes(IndexList) ->
  case IndexList of
    [{global, KeyList1}, {local, HashKey, RangeList}] ->
      GlobalKeyData = form_global_index_key_schema(KeyList1, []),
      LocalKeyData = form_local_index_key_schema(HashKey, RangeList, []),
      {{?GLOBAL_SECONDARY_INDEXES, GlobalKeyData}, {?LOCAL_SECONDARY_INDEXES, LocalKeyData}};
    [{global, KeyList}] ->
      GlobalKeyData = form_global_index_key_schema(KeyList, []),
      {?GLOBAL_SECONDARY_INDEXES, GlobalKeyData};
    [{local, HashKey, RangeList}] ->
      LocalKeyData = form_local_index_key_schema(HashKey, RangeList, []),
      {?LOCAL_SECONDARY_INDEXES, LocalKeyData}
  end.
%%   io:format("Index : ~n~p~n~n",[Index]),
%%   Index.

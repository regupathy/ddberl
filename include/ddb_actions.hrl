%%%-------------------------------------------------------------------
%%% @author natesh
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Aug 2015 4:08 PM
%%%-------------------------------------------------------------------
%% -author("natesh").

-define(BATCH_GET_ITEM, <<"BatchGetItem">>).
-define(BATCH_WRITE_ITEM, <<"BatchWriteItem">>).
-define(CREATE_TABLE, <<"CreateTable">>).
-define(DELETE_ITEM, <<"DeleteItem">>).
-define(DELETE_TABLE, <<"DeleteTable">>).
-define(DESCRIBE_TABLE, <<"DescribeTable">>).
-define(GET_ITEM, <<"GetItem">>).
-define(LIST_TABLES, <<"ListTables">>).
-define(PUT_ITEM, <<"PutItem">>).
-define(QUERY, <<"Query">>).
-define(SCAN, <<"Scan">>).
-define(UPDATE_ITEM, <<"UpdateItem">>).
-define(UPDATE_TABLE, <<"UpdateTable">>).

-record(operation,{lt,gt,eq,le,ge}).

operation(#operation.lt) -> <<" < ">>;
operation(#operation.gt) -> <<" > ">>;
operation(#operation.eq) -> <<" = ">>;
operation(#operation.le) -> <<" =< ">>;
operation(#operation.ge) -> <<" >= ">>.


read_actions() ->
  [?BATCH_GET_ITEM,
    ?GET_ITEM,
    ?QUERY,
    ?SCAN
  ].

write_actions() ->
  [?BATCH_WRITE_ITEM,
  ?DELETE_ITEM,
  ?PUT_ITEM,
  ?UPDATE_ITEM].




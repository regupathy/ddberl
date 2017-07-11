%%%-------------------------------------------------------------------
%%% @author natesh
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Aug 2015 3:26 PM
%%%-------------------------------------------------------------------
-module(ddb_tables).
-author("natesh").

-include("json_keys.hrl").
-include("ddb_tables.hrl").
-include("ddb_actions.hrl").
-include("ddb_parameters.hrl").

%% API
%% -export([]).
-compile([export_all]).

create_application_based_tables(AppId) ->
  ok = create_message_table(AppId),
  ok = create_channel_table(AppId),
  ok = create_message_queue_table(AppId),
  ok = create_channel_subscription_table(AppId),
  ok = create_session_table(AppId),
  ok = create_account_logout_time_table(AppId),
  ok = create_account_channel_table(AppId).

create_message_table(AppId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?MESSAGE_TABLE},
%%     {"AttributeDefinitions", [
%%       [{"AttributeName", ?MESSAGE_ID}, {"AttributeType", "S"}]
%% %%       [{"AttributeName", ?MESSAGE_TYPE}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?MESSAGE}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?CREATED_TIME}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?FROM_ID}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?TO_TYPE}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?TO_ID}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?INTERNAL_FILE_PATH}, {"AttributeType", "S"}]
%%     ]},
%%     {"KeySchema", [[{"AttributeName", ?MESSAGE_ID},{"KeyType", "HASH"}]]},
%%     {"ProvisionedThroughput", [{"WriteCapacityUnits", 1}, {"ReadCapacityUnits", 1}]}
%%   ],
  TableName = ddb_util:merge_table_name(AppId,?MESSAGE_TABLE),
  Data = ddb_tables_util:create_table(TableName,[?MESSAGE_ID], {?MESSAGE_ID}),
  create_table(Data).


create_channel_table(AppId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?CHANNEL_TABLE},
%%     {"AttributeDefinitions", [
%%       [{"AttributeName", ?CHANNEL_ID}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?CHANNEL_NAME}, {"AttributeType", "S"}]
%% %%       [{"AttributeName", ?CREATED_BY}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?CREATED_TIME}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?ACCOUNT_LIST}, {"AttributeType", "SS"}],
%% %%       [{"AttributeName", ?ADMIN_LIST}, {"AttributeType", "SS"}]
%%     ]},
%%     {"KeySchema", [[{"AttributeName", ?CHANNEL_ID},{"KeyType", "HASH"}]]},
%%     {"GlobalSecondaryIndexes", [
%%       [{"IndexName", "ChannelName-index"},
%%         {"KeySchema", [
%%           [{"AttributeName", "ChannelName"},{"KeyType", "HASH"}]
%%         ]},
%%         {"Projection", [{"ProjectionType", "ALL"}]},
%%         {"ProvisionedThroughput", [{"WriteCapacityUnits", 1}, {"ReadCapacityUnits", 1}]}
%%       ]
%%     ]},
%%     {"ProvisionedThroughput", [{"WriteCapacityUnits", 1}, {"ReadCapacityUnits", 1}]}
%%   ],
  TableName = ddb_util:merge_table_name(AppId,?CHANNEL_TABLE),
  ChannelNameIndex = ddb_util:name_index([?CHANNEL_NAME]),
  Data = ddb_tables_util:create_table_with_index(TableName,
    [?CHANNEL_ID,?CHANNEL_NAME],
    {?CHANNEL_ID},
    [{global,[{ChannelNameIndex,?CHANNEL_NAME}]}]),
    create_table(Data).

create_message_queue_table(AppId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?MESSAGE_QUEUE_TABLE},
%%     {"AttributeDefinitions", [
%%       [{"AttributeName", ?ACCOUNT_ID}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?MESSAGE_ID}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?MESSAGE_STATUS}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?MESSAGE_TIME}, {"AttributeType", "S"}]
%%     ]},
%%     {"KeySchema", [
%%       [{"AttributeName", "AccountId"},{"KeyType", "HASH"}],
%%       [{"AttributeName", "MessageId"},{"KeyType", "RANGE"}]
%%     ]},
%%     {"LocalSecondaryIndexes", [
%% %%       [{"IndexName", "MessageStatus-index"},
%% %%         {"KeySchema", [
%% %%           [{"AttributeName", "AccountId"},{"KeyType", "HASH"}],
%% %%           [{"AttributeName", "MessageStatus"},{"KeyType", "RANGE"}]
%% %%         ]},
%% %%         {"Projection", [{"ProjectionType", "ALL"}]}
%% %%       ],
%%       [{"IndexName", "MessageTime-index"},
%%         {"KeySchema", [
%%           [{"AttributeName", "AccountId"},{"KeyType", "HASH"}],
%%           [{"AttributeName", "MessageTime"},{"KeyType", "RANGE"}]
%%         ]},
%%         {"Projection", [{"ProjectionType", "ALL"}]}
%%       ]
%%     ]},
%%     {"ProvisionedThroughput", [{"WriteCapacityUnits", 1}, {"ReadCapacityUnits", 1}]}
%%   ],
  TableName = ddb_util:merge_table_name(AppId,?MESSAGE_QUEUE_TABLE),
  MsgTimeIndex = ddb_util:name_index([?MESSAGE_TIME]),
  Data = ddb_tables_util:create_table_with_index(TableName,
    [?ACCOUNT_ID,?MESSAGE_ID,?MESSAGE_TIME],
    {?ACCOUNT_ID,?MESSAGE_ID},
    [{local,?ACCOUNT_ID, [{MsgTimeIndex, ?MESSAGE_TIME}]}]),
  create_table(Data).


create_channel_subscription_table(AppId) ->
%%   Data = [{"TableName", AppId ++ ?CHANNEL_SUBSCRIPTION_TABLE},
%%     {"AttributeDefinitions", [
%%       [{"AttributeName", ?CHANNEL_ID}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?ACCOUNT_ID}, {"AttributeType", "S"}]
%% %%       [{"AttributeName", ?SUBSCRIPTION_TYPE}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?CHANNEL_TIME}, {"AttributeType", "S"}]
%%     ]},
%%     {"KeySchema", [
%%       [{"AttributeName", "ChannelId"},{"KeyType", "HASH"}],
%%       [{"AttributeName", "AccountId"},{"KeyType", "RANGE"}]
%%     ]},
%% %%     {"LocalSecondaryIndexes", [
%% %%       [{"IndexName", "ChannelTime-index"},
%% %%         {"KeySchema", [
%% %%           [{"AttributeName", "ChannelId"},{"KeyType", "HASH"}],
%% %%           [{"AttributeName", "ChannelTime"},{"KeyType", "RANGE"}]
%% %%         ]},
%% %%         {"Projection", [{"ProjectionType", "ALL"}]}
%% %%       ],
%% %%       [{"IndexName", "SubscriptionType-index"},
%% %%         {"KeySchema", [
%% %%           [{"AttributeName", "ChannelId"},{"KeyType", "HASH"}],
%% %%           [{"AttributeName", "SubscriptionType"},{"KeyType", "RANGE"}]
%% %%         ]},
%% %%         {"Projection", [{"ProjectionType", "ALL"}]}
%% %%       ]
%% %%     ]},
%%     {"ProvisionedThroughput", [{"WriteCapacityUnits", 1}, {"ReadCapacityUnits", 1}]}
%%   ],
  TableName = ddb_util:merge_table_name(AppId,?CHANNEL_SUBSCRIPTION_TABLE),
  Data = ddb_tables_util:create_table(TableName,[?CHANNEL_ID,?ACCOUNT_ID], {?CHANNEL_ID,?ACCOUNT_ID}),
  create_table(Data).

create_session_table(AppId) ->
%%   Data = [{"TableName", AppId ++ ?SESSION_TABLE},
%%     {"AttributeDefinitions", [
%%       [{"AttributeName", ?SESSION_ID}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?RANGE_KEY}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?ACCOUNT_ID}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?USER_AGENT}, {"AttributeType", "S"}],
%% %%       [{"AttributeName", ?LOCATION}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?CREATED_TIME}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?LAST_REQ_TIME}, {"AttributeType", "S"}],
%%       [{"AttributeName", ?CLOSE_TIME}, {"AttributeType", "S"}]
%%   ]},
%%     {"KeySchema", [
%%       [{"AttributeName", "SessionId"},{"KeyType", "HASH"}],
%%       [{"AttributeName", "RangeKey"},{"KeyType", "RANGE"}]
%%     ]},
%%     {"LocalSecondaryIndexes", [
%%       [{"IndexName", "AccountId-index"},
%%         {"KeySchema", [
%%           [{"AttributeName", "SessionId"},{"KeyType", "HASH"}],
%%           [{"AttributeName", "AccountId"},{"KeyType", "RANGE"}]
%%         ]},
%%         {"Projection", [{"ProjectionType", "ALL"}]}
%%       ],
%%       [{"IndexName", "UserAgent-index"},
%%         {"KeySchema", [
%%           [{"AttributeName", "SessionId"},{"KeyType", "HASH"}],
%%           [{"AttributeName", "UserAgent"},{"KeyType", "RANGE"}]
%%         ]},
%%         {"Projection", [{"ProjectionType", "ALL"}]}
%%       ],
%%       [{"IndexName", "CreatedTime-index"},
%%         {"KeySchema", [
%%           [{"AttributeName", "SessionId"},{"KeyType", "HASH"}],
%%           [{"AttributeName", "CreatedTime"},{"KeyType", "RANGE"}]
%%         ]},
%%         {"Projection", [{"ProjectionType", "ALL"}]}
%%       ],
%%       [{"IndexName", "LastReqTime-index"},
%%         {"KeySchema", [
%%           [{"AttributeName", "SessionId"},{"KeyType", "HASH"}],
%%           [{"AttributeName", "LastReqTime"},{"KeyType", "RANGE"}]
%%         ]},
%%         {"Projection", [{"ProjectionType", "ALL"}]}
%%       ],
%%       [{"IndexName", "CloseTime-index"},
%%         {"KeySchema", [
%%           [{"AttributeName", "SessionId"},{"KeyType", "HASH"}],
%%           [{"AttributeName", "CloseTime"},{"KeyType", "RANGE"}]
%%         ]},
%%         {"Projection", [{"ProjectionType", "ALL"}]}
%%       ]
%%     ]},
%%     {"ProvisionedThroughput", [{"WriteCapacityUnits", 1}, {"ReadCapacityUnits", 1}]}
%%   ],
  TableName = ddb_util:merge_table_name(AppId,?SESSION_TABLE),
  Data = ddb_tables_util:create_table(TableName, [?SESSION_ID], {?SESSION_ID}),
  create_table(Data).


create_account_logout_time_table(AppId) ->
%%   Data = [{"TableName", AppId ++ ?ACCOUNT_LOGOUT_TIME_TABLE},
%%     {"AttributeDefinitions", [
%%       [{"AttributeName", ?ACCOUNT_ID}, {"AttributeType", "S"}],
%%         [{"AttributeName", ?UA_ID}, {"AttributeType", "S"}]
%%       ]},
%%     {"KeySchema", [
%%       [{"AttributeName", ?ACCOUNT_ID},{"KeyType", "HASH"}],
%%       [{"AttributeName", ?UA_ID},{"KeyType", "RANGE"}]
%%     ]},
%%     {"ProvisionedThroughput", [{"WriteCapacityUnits", 1}, {"ReadCapacityUnits", 1}]}
%%   ],
  TableName = ddb_util:merge_table_name(AppId,?ACCOUNT_LOGOUT_TIME_TABLE),
  Data = ddb_tables_util:create_table(TableName,[?ACCOUNT_ID,?UA_ID], {?ACCOUNT_ID,?UA_ID}),
  create_table(Data).

create_account_channel_table(AppId) ->
%%   Data = [{"TableName", AppId ++ ?ACCOUNT_CHANNEL_TABLE},
%%     {"AttributeDefinitions", [
%%       [{"AttributeName", ?ACCOUNT_ID}, {"AttributeType", "S"}]
%% %%         [{"AttributeName", ?CHANNEL_LIST}, {"AttributeType", "SS"}]
%%       ]},
%%     {"KeySchema", [[{"AttributeName", ?ACCOUNT_ID},{"KeyType", "HASH"}]]},
%%     {"ProvisionedThroughput", [{"WriteCapacityUnits", 1}, {"ReadCapacityUnits", 1}]}
%%   ],
  TableName = ddb_util:merge_table_name(AppId,?ACCOUNT_CHANNEL_TABLE),
  Data = ddb_tables_util:create_table(TableName, [?ACCOUNT_ID], {?ACCOUNT_ID}),
  create_table(Data).


delete_application_based_tables(AppId) ->
  ok = delete_table(AppId,?MESSAGE_TABLE),
  ok = delete_table(AppId,?CHANNEL_TABLE),
  ok = delete_table(AppId,?MESSAGE_QUEUE_TABLE),
  ok = delete_table(AppId,?CHANNEL_SUBSCRIPTION_TABLE),
  ok = delete_table(AppId,?SESSION_TABLE),
  ok = delete_table(AppId,?ACCOUNT_LOGOUT_TIME_TABLE),
  ok = delete_table(AppId,?ACCOUNT_CHANNEL_TABLE).



delete_table(AppId,Name) ->
  try
    Data = [{?TABLE_NAME, ddb_util:merge_table_name(AppId,Name)}],
    {ok, JsonData} = erljson_decoder:scan(Data),
    Response = erl_ddb:send_data(?DELETE_TABLE, JsonData),
    ddb_response:validate_response(Response)
  catch
    _:_ -> erlang:display(erlang:get_stacktrace())
  end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

create_table(Data) ->
  JsonData = jsonGenerator:create_json(Data),
  Response = erl_ddb:send_data(?CREATE_TABLE, JsonData),
  ddb_response:validate_response(Response).
%%%-------------------------------------------------------------------
%%% @author natesh
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Jul 2015 4:12 PM
%%%-------------------------------------------------------------------
-module(ddb_req_handler).
-author("natesh").

-include("json_keys.hrl").
-include("ddb_tables.hrl").
-include("ddb_actions.hrl").
-include("ddb_status.hrl").

%% API
%% -export([create_req/2]).
-compile([export_all]).


%% *** APPLICATION_TABLE ***
create_application(AppId,AppName,AppKey,AccessLimit,SecretKey) ->
%%   Data = [{?TABLE_NAME, ?APPLICATION_TABLE},
%%     {"Item",
%%       [{?APP_ID, [{"S", AppId}]},
%%         {?RANGE_KEY, [{"S", ?TRUE}]},
%%         {?APPLICATION_NAME, [{"S", AppName}]},
%%         {?APP_KEY, [{"S", AppKey}]},
%%         {?ACCESS_LIMIT, [{"S", AccessLimit}]},
%%         {?SECRET_KEY, [{"S", SecretKey}]},
%%         {?APPLICATION_STATUS, [{"S", ?ACTIVE}]}
%%       ]}],
  {TableName,KeyValues} = ddb_parameters:create_application(AppId,AppName,AppKey,AccessLimit,SecretKey),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).
%%   JsonData = jsonGenerator:create_json(Data),
%%   Response = erl_ddb:send_data(?PUT_ITEM, JsonData),
%%   ddb_response:validate_response(Response).

update_application_status(AppId, Status) ->
%%   Data = [{?TABLE_NAME, ?APPLICATION_TABLE},
%%     {"Key", [{?APP_ID, [{"S", AppId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "set ApplicationStatus = :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", Status}]}]
%%     }
%%   ],
  {TableName,Key, UpdateAttrib} = ddb_parameters:update_application_status(AppId,Status),
  Data = ddb_actions:update_set_item(TableName,Key, UpdateAttrib),
  erl_ddb:hit_ddb(Data).
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).

delete_application(AppId) ->
%%   Data = [{?TABLE_NAME, ?APPLICATION_TABLE},
%%     {"Key", [{?APP_ID, [{"S", AppId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]}
%%   ],
  {TableName,Keys} = ddb_parameters:delete_application(AppId),
  Data = ddb_actions:delete_item(TableName,Keys),
  erl_ddb:hit_ddb(Data).
%%   JsonData = jsonGenerator:create_json(Data),
%%   Response = erl_ddb:send_data(?DELETE_ITEM, JsonData),
%%   ddb_response:validate_response(Response).

get_active_applications() ->
%%   Data = [{?TABLE_NAME, ?APPLICATION_TABLE},
%%     {"FilterExpression", "ApplicationStatus = :val1"},
%%     {"ExpressionAttributeValues",
%%       [
%%         {":val1", [{"S", ?ACTIVE}]}
%%       ]
%%     },
%%     {"ProjectionExpression", "AppId, ApplicationName, AccessLimit, AppKey, SecretKey"}
%%   ],
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_active_applications(),
  Data = ddb_actions:scan(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).

get_applictaion_details(AppId) ->
%%   TableName = ?APPLICATION_TABLE,
%%   Keys = [{?APP_ID, AppId},{?RANGE_KEY,?TRUE}],
%%   ProjectionExpression = "AppId, ApplicationName, AccessLimit, AppKey, SecretKey, ApplicationStatus",
%%   ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_applictaion_details(AppId),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_all_application_ids() ->
%%   Data = [{?TABLE_NAME, ?APPLICATION_TABLE},
%%     {"ProjectionExpression", "AppId"}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, ProjectionExpression} = ddb_parameters:get_all_application_ids(),
  Data = ddb_actions:scan(TableName, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

%% *** ACCOUNT_TABLE ***
add_account(AccountId,AccountType,AccessId,AccountName,Password,AccountTime,UAIdList) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"Item",
%%       [{?ACCOUNT_ID, [{"S", AccountId}]},
%%         {?RANGE_KEY, [{"S", ?TRUE}]},
%%         {?ACCOUNT_TYPE, [{"S", AccountType}]},
%%         {?ACCESS_ID, [{"S", AccessId}]},
%%         {?ACCOUNT_NAME, [{"S", AccountName}]},
%%         {?PASSWORD, [{"S", Password}]},
%%         {?CREATED_TIME, [{"S", AccountTime}]},
%%         {?LOGOUT_TIME, [{"S", AccountTime}]},
%%         {?UA_ID_LIST, [{"SS", UAIdList}]},
%%         {?ACCOUNT_STATUS, [{"S", ?ACTIVE}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  {TableName,KeyValues} = ddb_parameters:add_account(AccountId,AccountType,AccessId,AccountName,Password,AccountTime,UAIdList),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

account_authentication(AccessId,Password) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"IndexName","AccessId-Password-index"},
%%     {"KeyConditionExpression", "AccessId = :val1 AND Password = :val2"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", AccessId}]},
%%         {":val2", [{"S", Password}]}
%%       ]},
%%     {"ProjectionExpression", "AccountId, AccountName"}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?QUERY, JsonData),
  {TableName, IndexList, KeyCondition, ProjectionExpression} = ddb_parameters:account_authentication(AccessId,Password),
  Data = ddb_actions:query_with_index_projection(TableName, IndexList, KeyCondition, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

add_application_in_account(AccountId, AppIdList) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "add ApplicationList :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", AppIdList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_application_list_in_account(AccountId, AppIdList),
  Data = ddb_actions:update_add_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).

remove_application_from_account(AccountId, AppIdList) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "delete ApplicationList :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", AppIdList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_application_list_in_account(AccountId, AppIdList),
  Data = ddb_actions:update_delete_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).

update_user_agent_in_account(AccountId, UAIdList) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "add UAIdList :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", UAIdList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_user_agent_list_in_account(AccountId, UAIdList),
  Data = ddb_actions:update_add_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).

get_accounts_details(AccessId) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"IndexName","AccessId-Password-index"},
%%     {"KeyConditionExpression", "AccessId = :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", AccessId}]}]},
%%     {"ProjectionExpression", "AccountId, AccountName, AccountType, AccessId"}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?QUERY, JsonData).
  {TableName, IndexList, KeyCondition, ProjectionExpression} = ddb_parameters:get_accounts_details(AccessId),
  Data = ddb_actions:query_with_index_projection(TableName, IndexList, KeyCondition, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

add_buddy(AccountId, BuddyList) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"Key", [{"AccountId", [{"S", AccountId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "add BuddyList :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", BuddyList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_buddy_list(AccountId, BuddyList),
  Data = ddb_actions:update_add_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).

remove_buddy(AccountId, BuddyList) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "delete BuddyList :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", BuddyList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_buddy_list(AccountId, BuddyList),
  Data = ddb_actions:update_delete_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).

%% update_logout_time(AccountId, Time) ->
%% %%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%% %%     {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]},
%% %%       {?RANGE_KEY, [{"S", ?TRUE}]}
%% %%     ]},
%% %%     {"UpdateExpression", "set LogoutTime = :val1"},
%% %%     {"ExpressionAttributeValues",
%% %%       [{":val1", [{"S", Time}]}]
%% %%     }
%% %%   ],
%% %%   JsonData = jsonGenerator:create_json(Data),
%% %%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
%%   {TableName, Keys, UpdateExpression} = ddb_parameters:update_logout_time(AccountId, Time),
%%   Data = ddb_actions:update_set_item(TableName, Keys, UpdateExpression),
%%   erl_ddb:hit_ddb(Data).

remove_account(AccountId) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?DELETE_ITEM, JsonData).
  {TableName,Keys} = ddb_parameters:remove_account(AccountId),
  Data = ddb_actions:delete_item(TableName, Keys),
  erl_ddb:hit_ddb(Data).

%% *** Check for filter expression with "contains" ***
user_agent_availability(AccountId) ->
%%   TableName = ?ACCOUNT_TABLE,
%%   Keys = [{?ACCOUNT_ID, AccountId},{?RANGE_KEY,?TRUE}],
%%   ProjectionExpression = ?UA_ID_LIST,
%%   Response = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
%%   case ddb_response:get_response(Response) of
%%     {ok, AttributeList} ->
%%       Val = ddb_response:get_single_value(AttributeList),
%%       {ok, Val};
%%     data_not_found ->
%%       not_found;
%%     {error, Reason} ->
%%       {error, Reason}
%%   end.
  {TableName, Keys, ProjectionExpression} = ddb_parameters:user_agent_availability(AccountId),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_all_active_accounts() ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"FilterExpression", "AccountStatus = :val1"},
%%     {"ExpressionAttributeValues",
%%       [
%%         {":val1", [{"S", ?ACTIVE}]}
%%       ]
%%     },
%%     {"ProjectionExpression", "AccountId, AccountName, AccountType, AccessId, Password"}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_all_active_accounts(),
  Data = ddb_actions:scan(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).


%% *** SESSION_TABLE ***
add_session(AppId,SessionId,AccountId,Location,CreatedTime,UAId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?SESSION_TABLE},
%%     {"Item",
%%       [{?SESSION_ID, [{"S", SessionId}]},
%%         {?RANGE_KEY, [{"S", ?TRUE}]},
%%         {?ACCOUNT_ID, [{"S", AccountId}]},
%%         {?USER_AGENT, [{"S", UserAgent}]},
%%         {?USER_LOCATION, [{"S", Location}]},
%%         {?SESSION_STATUS, [{"S", ?ACTIVE}]},
%%         {?CREATED_TIME, [{"S", CreatedTime}]},
%%         {?UA_ID, [{"S", UAId}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  {TableName,KeyValues} = ddb_parameters:add_session(AppId,SessionId,AccountId,Location,CreatedTime,UAId),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

get_active_sessions(AppId, AccountId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?SESSION_TABLE},
%%     {"FilterExpression", "AccountId = :val1 AND SessionStatus = :val2"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", AccountId}]},
%%         {":val2", [{"S", "ACTIVE"}]}
%%       ]
%%     },
%%     {"ProjectionExpression", ?SESSION_ID}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_active_sessions(AppId,AccountId),
  Data = ddb_actions:scan(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_all_active_sessions(AppId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?SESSION_TABLE},
%%     {"FilterExpression", "SessionStatus = :val"},
%%     {"ExpressionAttributeValues",
%%       [
%%         {":val", [{"S", "ACTIVE"}]}
%%       ]
%%     },
%%     {"ProjectionExpression", ?SESSION_ID}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_all_active_sessions(AppId),
  Data = ddb_actions:scan(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

%% *** CHANNEL_TABLE ***
create_channel(AppId, ChannelId, ChannelName, CreatedBy, CreatedTime, UserList,AdminList) ->
%%   NewUserList = [CreatedBy|UserList],  %% Previously I have to prepend the CreatedBy value to the UserList. Removed
%%   AdminList = [CreatedBy],
%%   Data = [{?TABLE_NAME, AppId ++ ?CHANNEL_TABLE},
%%     {"Item",
%%       [{?CHANNEL_ID, [{"S", ChannelId}]},
%%         {?CHANNEL_NAME, [{"S", ChannelName}]},
%%         {?CREATED_BY, [{"S", CreatedBy}]},
%%         {?CREATED_TIME, [{"S", CreatedTime}]},
%%         {?CHANNEL_STATUS,[{"S",?ACTIVE}]},
%%         {?ACCOUNT_LIST, [{"SS", UserList}]},
%%         {?ADMIN_LIST, [{"SS", AdminList}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  {TableName,KeyValues} = ddb_parameters:create_channel(AppId,ChannelId,ChannelName,CreatedBy,CreatedTime,UserList,AdminList),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

add_to_account_channel_table(AppId, AccountIdList,ChannelIdList) ->
  lists:foreach(fun(AccountId) ->
    add_channel_list_to_account_channel_table(AppId,AccountId,ChannelIdList)
  end, AccountIdList).

add_channel_list_to_account_channel_table(AppId,AccountId,ChannelIdList) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?ACCOUNT_CHANNEL_TABLE},
%%     {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]}]},
%%     {"UpdateExpression", "add ChannelList :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", ChannelIdList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_channel_in_account_channel_table(AppId,AccountId,ChannelIdList),
  Data = ddb_actions:update_add_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).

add_to_channel_subscription_table(AppId,ChannelId,ChannelTime,AccountIdList) ->
%%   Req  = form_batch_chnl_sub_request(ChannelId, ChannelTime, AccountIdList, []),
%%   Data = [{"RequestItems", [
%%     {AppId ++ ?CHANNEL_SUBSCRIPTION_TABLE, Req}
%%   ]
%%   }],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?BATCH_WRITE_ITEM, JsonData).
  TableName = ddb_util:merge_table_name(AppId,?CHANNEL_SUBSCRIPTION_TABLE),
  Req = ddb_parameters:form_batch_chnl_sub_request(ChannelId, ChannelTime, AccountIdList, []),
  Data = ddb_actions:batch_write_request([{TableName, Req}]),
  erl_ddb:hit_ddb(Data).

%% form_batch_chnl_sub_request(_ChannelId, _ChannelTime,[], Acc) -> lists:reverse(Acc);
%% form_batch_chnl_sub_request(ChannelId, ChannelTime, [H|Rest], Acc) ->
%%   Val = [{"PutRequest", [
%%     {"Item", [
%%       {?CHANNEL_ID, [{"S", ChannelId}]},
%%       {?ACCOUNT_ID, [{"S", H}]},
%%       {?SUBSCRIPTION_TYPE, [{"S", ?SUBSCRIBED}]},
%%       {?CHANNEL_TIME, [{"S", ChannelTime}]}
%%     ]}
%%   ]}],
%%   form_batch_chnl_sub_request(ChannelId, ChannelTime, Rest, [Val|Acc]).

channel_authentication(AppId, ChannelName) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?CHANNEL_TABLE},
%%     {"IndexName","ChannelName-index"},
%%     {"KeyConditionExpression", "ChannelName = :val"},
%%     {"ExpressionAttributeValues",
%%       [
%%         {":val", [{"S", ChannelName}]}
%%       ]
%%     },
%%     {"ProjectionExpression", "ChannelId"}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?QUERY, JsonData).
  {TableName, IndexList, KeyCondition, ProjectionExpression} = ddb_parameters:channel_authentication(AppId, ChannelName),
  Data = ddb_actions:query_with_index_projection(TableName, IndexList, KeyCondition, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

%% remove_channels(AppId, [{ChannelId, [AcclountList]}])
remove_channels(AppId, ChannelIdAccountList) ->
  remove_channels_from_channel_table(AppId, ChannelIdAccountList),
  remove_channels_from_channel_subscription_table(AppId, ChannelIdAccountList),
  remove_channel_list_from_account_channel_table(AppId, ChannelIdAccountList).

remove_channels_from_channel_table(AppId, ChannelIdAccountList) ->
  TableName = ddb_util:merge_table_name(AppId,?CHANNEL_TABLE),
  Req = ddb_parameters:batch_remove_channel(ChannelIdAccountList, []),
  Data = ddb_actions:batch_write_request([{TableName, Req}]),
  erl_ddb:hit_ddb(Data).

remove_channels_from_channel_subscription_table(AppId, ChannelIdAccountList) ->
  TableName = ddb_util:merge_table_name(AppId,?CHANNEL_SUBSCRIPTION_TABLE),
  Req = ddb_parameters:batch_remove_channel_subscription(ChannelIdAccountList, []),
  Data = ddb_actions:batch_write_request([{TableName, Req}]),
  erl_ddb:hit_ddb(Data).

%% remove_channels_from_channel_table(_AppId, []) -> ok;
%% remove_channels_from_channel_table(AppId, [H|Rest]) ->
%%   {ChannelId, _AccountList} = H,
%%   Req =
%%   Data = [{"RequestItems", [
%%     {AppId ++ ?CHANNEL_TABLE, [
%%       [{"DeleteRequest", [
%%         {"Key", [
%%           {?CHANNEL_ID, [{"S", ChannelId}]}
%%         ]}
%%       ]}]
%%     ]},
%%     {AppId ++ ?CHANNEL_SUBSCRIPTION_TABLE, [[
%%       {"DeleteRequest", [
%%         {"Key", [
%%           {?CHANNEL_ID, [{"S", ChannelId}]},
%%           {?RANGE_KEY, [{"S", ?TRUE}]}
%%         ]}
%%       ]}
%%     ]]
%%     }
%%   ]
%%   }],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?BATCH_WRITE_ITEM, JsonData),
%%   remove_channels_from_channel_table(AppId, Rest).

remove_channel_list_from_account_channel_table(_AppId, []) -> ok;
remove_channel_list_from_account_channel_table(AppId, [H|Rest]) ->
  {ChannelId, AccountList} = H,
  remove_channel_list_from_account_channel_table(AppId, AccountList, [ChannelId]),
  remove_channel_list_from_account_channel_table(AppId, Rest).

remove_channel_list_from_account_channel_table(AppId, AccountList, ChannelList) ->
  lists:foreach(fun(AccountId) ->
%%     Data = [{?TABLE_NAME, AppId ++ ?ACCOUNT_CHANNEL_TABLE},
%%       {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]}]},
%%       {"UpdateExpression", "delete ChannelList :val1"},
%%       {"ExpressionAttributeValues",
%%         [{":val1", [{"SS", ChannelList}]}]
%%       }
%%     ],
%%     JsonData = jsonGenerator:create_json(Data),
%%     erl_ddb:send_data(?UPDATE_ITEM, JsonData)
    {TableName, Keys, UpdateExpression} = ddb_parameters:update_channel_in_account_channel_table(AppId,AccountId,ChannelList),
    Data = ddb_actions:update_delete_item(TableName, Keys, UpdateExpression),
    erl_ddb:hit_ddb(Data)
  end,AccountList).

add_admins_to_channel(AppId, ChannelId, AdminList) ->
  add_list_to_channel(?ADMIN_LIST, AppId, ChannelId, AdminList).

add_list_to_channel(ListType, AppId, ChannelId, MemberList) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?CHANNEL_TABLE},
%%     {"Key", [{"ChannelId", [{"S", ChannelId}]}]},
%%     {"UpdateExpression", "add " ++ binary_to_list(ListType) ++ " :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", MemberList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_list_in_channel_table(ListType, AppId, ChannelId, MemberList),
  Data = ddb_actions:update_add_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).


remove_admins_from_channel(AppId, ChannelId, AdminList) ->
  remove_list_from_channel(?ADMIN_LIST, AppId, ChannelId, AdminList).

get_admins_from_channel(AppId, ChannelId) ->
%%   TableName = AppId ++ ?CHANNEL_TABLE,
%%   Keys = [{?CHANNEL_ID, ChannelId}],
%%   ProjectionExpression = ?ADMIN_LIST,
%%   ddb_actions:get_item(TableName, Keys, ProjectionExpression).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_admins_from_channel(AppId, ChannelId),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

%% remove_channels_from_account(AccountId, ChannelList) ->
%%   Data = [{?TABLE_NAME, ?ACCOUNT_TABLE},
%%     {"Key", [{"AccountId", [{"S", AccountId}]}]},
%%     {"UpdateExpression", "delete ChannelList :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", ChannelList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).

remove_accounts_from_channel(AppId, ChannelId, UserList) ->
  remove_list_from_channel(?ACCOUNT_LIST, AppId, ChannelId, UserList).

remove_list_from_channel(ListType, AppId, ChannelId, MemberList) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?CHANNEL_TABLE},
%%     {"Key", [{?CHANNEL_ID, [{"S", ChannelId}]}]},
%%     {"UpdateExpression", "delete " ++ binary_to_list(ListType) ++ " :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", MemberList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_list_in_channel_table(ListType, AppId, ChannelId, MemberList),
  Data = ddb_actions:update_delete_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).

update_channel_subscription_type(AppId, AccountId, ChannelId, SubscriptionType) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?CHANNEL_SUBSCRIPTION_TABLE},
%%     {"Key", [{?CHANNEL_ID, [{"S", ChannelId}]},
%%       {?ACCOUNT_ID, [{"S", AccountId}]}
%%     ]},
%%     {"UpdateExpression", "set SubscriptionType = :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", SubscriptionType}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName,Key, UpdateAttrib} = ddb_parameters:update_channel_subscription_type(AppId, AccountId, ChannelId, SubscriptionType),
  Data = ddb_actions:update_set_item(TableName,Key, UpdateAttrib),
  erl_ddb:hit_ddb(Data).

get_all_account_channels(AppId) ->
%% Data = [{?TABLE_NAME, AppId ++ ?ACCOUNT_CHANNEL_TABLE}],
%%  JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, ProjectionExpression} = ddb_parameters:get_all_account_channels(AppId),
  Data = ddb_actions:scan(TableName, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_all_channel_ids(AppId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?CHANNEL_TABLE},
%%     {"ProjectionExpression", ?CHANNEL_ID}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, ProjectionExpression} = ddb_parameters:get_all_channel_ids(AppId),
  Data = ddb_actions:scan(TableName, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_active_channels(AppId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?CHANNEL_TABLE},
%%     {"FilterExpression", "ChannelStatus = :val1"},
%%     {"ExpressionAttributeValues",
%%       [
%%         {":val1", [{"S", "ACTIVE"}]}
%%       ]
%%     },
%%     {"ProjectionExpression", "ChannelId, ChannelName, AccountList, AdminList"}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_active_channels(AppId),
  Data = ddb_actions:scan(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

%% ***** MESSAGE *****
-spec save_direct_message(AppId::string(), MsgId::string(),FromId::string(),ToId::string(),MessageType::string(),
    Message::string(),Time::string()) -> ok | {error, Reason::any()}.
save_direct_message(AppId, MsgId,FromId,ToId,MessageType,Message,Time) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?MESSAGE_TABLE},
%%     {"Item",
%%       [{?MESSAGE_ID, [{"S", MsgId}]},
%%         {?FROM_ID, [{"S", FromId}]},
%%         {?TO_TYPE, [{"S", "Direct"}]},
%%         {?TO_ID, [{"S", ToId}]},
%%         {?MESSAGE, [{"S", Message}]},
%%         {?CREATED_TIME, [{"S", Time}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   io:format("Json Data ~p ",[JsonData]),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  ToType = ?DIRECT_MSG,
  {TableName,KeyValues} = ddb_parameters:save_message(AppId,MsgId,FromId,ToId,ToType,MessageType,Message,Time),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

save_msg_details_in_msg_queue(AppId, AccountId,MsgId,MsgStatus,MsgTime) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?MESSAGE_QUEUE_TABLE},
%%     {"Item",
%%       [{?ACCOUNT_ID, [{"S", AccountId}]},
%%       {?MESSAGE_ID, [{"S", MsgId}]},
%%         {?MESSAGE_STATUS, [{"S", MsgStatus}]},
%%         {?MESSAGE_TIME, [{"S", MsgTime}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  {TableName,KeyValues} = ddb_parameters:save_msg_details_in_msg_queue(AppId, AccountId,MsgId,MsgStatus,MsgTime),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

-spec save_channel_message(AppId::string(),MsgId::string(),FromId::string(),ToId::string(),MessageType::string(),
    Message::string(),Time::string()) -> ok | {error, Reason::any()}.
save_channel_message(AppId,MsgId,FromId,ToId,MessageType,Message,Time) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?MESSAGE_TABLE},
%%     {"Item",
%%       [{?MESSAGE_ID, [{"S", MsgId}]},
%%         {?FROM_ID, [{"S", FromId}]},
%%         {?TO_TYPE, [{"S", "Channel"}]},
%%         {?TO_ID, [{"S", ToId}]},
%%         {?MESSAGE, [{"S", Message}]},
%%         {?CREATED_TIME, [{"S", Time}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  ToType = ?CHANNEL_MSG,
  {TableName,KeyValues} = ddb_parameters:save_message(AppId,MsgId,FromId,ToId,ToType,MessageType,Message,Time),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

save_group_message(AppId,MsgId,FromId,ToIdList,MessageType,Message,MsgTime) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?MESSAGE_TABLE},
%%     {"Item",
%%       [{?MESSAGE_ID, [{"S", MsgId}]},
%%         {?FROM_ID, [{"S", FromId}]},
%%         {?TO_TYPE, [{"S", "Group"}]},
%% %%         {"ToId", [{"S", ""}]},
%%         {?MESSAGE, [{"S", Message}]},
%%         {?GROUP_LIST, [{"SS", ToIdList}]},
%%         {?CREATED_TIME, [{"S", MsgTime}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  ToType = ?GROUP_MSG,
  {TableName,KeyValues} = ddb_parameters:save_group_message(AppId,MsgId,FromId,ToType,ToIdList,MessageType,Message,MsgTime),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

save_msg_details_in_msg_queue_for_acc_list(AppId,MsgId,MsgTime,AccountIdList) ->
  TableName = ddb_util:merge_table_name(AppId,?MESSAGE_QUEUE_TABLE),
  Req = ddb_parameters:form_batch_msg_request(MsgId, MsgTime, AccountIdList, []),
  Data = ddb_actions:batch_write_request([{TableName, Req}]),
  erl_ddb:hit_ddb(Data).
%%   Data = [{"RequestItems", [
%%     {AppId ++ ?MESSAGE_QUEUE_TABLE, Req}
%%   ]
%%   }],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?BATCH_WRITE_ITEM, JsonData).

%% form_batch_msg_request(_MsgId, _MsgTime, [], Acc) -> lists:reverse(Acc);
%% form_batch_msg_request(MsgId, MsgTime, [H|Rest], Acc) ->
%%   Val = [{"PutRequest", [
%%     {"Item", [
%%       {?ACCOUNT_ID, [{"S", H}]},
%% %%       {?RANGE_KEY, [{"S", ?TRUE}]},
%%       {?MESSAGE_ID, [{"S", MsgId}]},
%%       {?MESSAGE_STATUS, [{"S", "Received"}]},
%%       {?MESSAGE_TIME, [{"S", MsgTime}]}
%%     ]}
%%   ]}],
%%   form_batch_msg_request(MsgId, MsgTime, Rest, [Val|Acc]).

-spec get_message_by_message_id(AppId::string(),MsgId::string()) ->
  {ok, {MsgId::string(), MsgType::string(), Message::string(), Time::string(),
    FromId::string(),ToType::string(),ToId::string()}} | {error, Reason::any()}.
get_message_by_message_id(AppId, MsgId) ->
%%   TableName = AppId ++ ?MESSAGE_TABLE,
%%   Keys = [{?MESSAGE_ID, MsgId}],
%%   ProjectionExpression = "MessageId, MessageType, Message, CreatedTime, FromId, ToType, ToId",
%%   ddb_actions:get_item(TableName, Keys, ProjectionExpression).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_message_by_message_id(AppId, MsgId),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_message_ids(AppId, AccountId, LogoutTime) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?MESSAGE_QUEUE_TABLE},
%%     {"IndexName","MessageTime-index"},
%%     {"KeyConditionExpression", "AccountId = :val1 AND MessageTime >= :val2"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", AccountId}]},
%%         {":val2", [{"S", LogoutTime}]}
%%       ]},
%%     {"ProjectionExpression", "MessageId"}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?QUERY, JsonData).
  {TableName, IndexList, KeyCondition, ProjectionExpression} = ddb_parameters:get_message_ids(AppId, AccountId, LogoutTime),
  Data = ddb_actions:query_with_index_projection(TableName, IndexList, KeyCondition, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_message_by_message_ids(_AppId, []) -> [];
get_message_by_message_ids(AppId, MsgIdList) ->
%%   Req = form_batch_get_msg_req(MsgIdList, []),
%%   Data = [{"RequestItems", [
%%     {AppId ++ ?MESSAGE_TABLE, [
%%       {"Keys", Req},
%%       {"ProjectionExpression", "MessageId, MessageType, Message, CreatedTime, FromId, ToType, ToId"}
%%     ]}
%%   ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?BATCH_GET_ITEM, JsonData).
  {TableName, Keys, Projection} = ddb_parameters:get_message_by_message_ids(AppId, MsgIdList),
  Data = ddb_actions:batch_get_request([{TableName, Keys, Projection}]),
  [[{_,Response}]] = erl_ddb:batch_hit_ddb(Data),
  Response.
%%   ddb_actions:order_batch_result(Keys,Response).
%%   [{_,Response}] = erl_ddb:batch_hit_ddb(Data),
%%   ddb_actions:order_batch_result(Keys,Response).

%% form_batch_get_msg_req([], Acc) -> lists:reverse(Acc);
%% form_batch_get_msg_req([MessageId|Rest], Acc) ->
%%   Req = [{"MessageId", [{"S", MessageId}]}],
%%   form_batch_get_msg_req(Rest, [Req|Acc]).

%% get_message_only_by_message_id(AppId, MsgId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?MESSAGE_TABLE},
%%     {"KeyConditionExpression", "MessageId = :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", MsgId}]}
%%       ]},
%%     {"ProjectionExpression", "MessageId, Message"}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?QUERY, JsonData).

update_message_sent_time(AppId, MsgId,Time) ->
  {TableName,KeySet,UpdateAttrib} = ddb_parameters:update_message_sent_time(AppId, MsgId,Time),
  Data = ddb_actions:update_set_item(TableName,KeySet, UpdateAttrib),
  erl_ddb:hit_ddb(Data).

get_message_delivered_time(AppId, Time) ->
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_message_delivered_time(AppId, Time),
  Data = ddb_actions:scan(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_message_delivered_time_by_message_id(AppId, MsgId) ->
  {TableName, Keys, Projection} = ddb_parameters:get_message_delivered_time_by_message_id(AppId, MsgId),
  Data = ddb_actions:get_item(TableName, Keys, Projection),
  case erl_ddb:hit_ddb(Data) of
    [{CreatedTime, SentTime}] -> ddb_util:time_diff(CreatedTime,SentTime);
    _Any -> "unread_message"
  end.

%% -spec get_message_by_time(AccountId::string(),Time::string()) ->
%%   {ok, MessageRecords::list()} | {error, Reason::any()}.
%% get_message_by_time(_AccountId,_Time) -> ok.
%%
%% -spec get_message_by_last_message_id(AccountId::string(), MsgId::string()) ->
%%   {ok, MessageRecords::list()} | {error, Reason::any()}.
%% get_message_by_last_message_id(_AccountId, _MsgId) -> ok.

%% *** LAST_ID ***
-spec add_last_id(AppId::string(), MsgId::string(), SessionId::string(),
    ChannelId::string()) -> ok | {error, Reason::any()}.
add_last_id(AppId,ChannelId,MessageId,SessionId) ->
%%   Data = [{?TABLE_NAME, ?LAST_ID_TABLE},
%%     {"Item",
%%       [{"AppId", [{"S", AppId}]},
%%         {"ChannelId", [{"S", ChannelId}]},
%%         {"MessageId", [{"S", MessageId}]},
%%         {"SessionId", [{"S", SessionId}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  {TableName,KeyValues} = ddb_parameters:add_last_id(AppId,ChannelId,MessageId,SessionId),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

update_last_id(Key, AppId, Id) ->
%%   Data = [{?TABLE_NAME, ?LAST_ID_TABLE},
%%     {"Key", [{?APP_ID, [{"S", AppId}]}]},
%%     {"UpdateExpression", "set " ++ binary_to_list(Key) ++ "= :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", Id}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName,KeySet,UpdateAttrib} = ddb_parameters:update_last_id(Key, AppId, Id),
  Data = ddb_actions:update_set_item(TableName,KeySet, UpdateAttrib),
  erl_ddb:hit_ddb(Data).

get_last_id(Key, AppId) ->
%%   TableName = ?LAST_ID_TABLE,
%%   Keys = [{?APP_ID, AppId}],
%%   ProjectionExpression = Key,
%%   ddb_actions:get_item(TableName, Keys, ProjectionExpression).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_last_id(Key, AppId),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_all_last_ids(AppId) ->
%%   TableName = ?LAST_ID_TABLE,
%%   Keys = [{?APP_ID, AppId}],
%%   ProjectionExpression = all,
%%   ddb_actions:get_item(TableName, Keys, ProjectionExpression).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_all_last_ids(AppId),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

get_all_last_ids() ->
%%   Data = [{?TABLE_NAME, ?LAST_ID_TABLE}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, ProjectionExpression} = ddb_parameters:get_all_last_ids(),
  Data = ddb_actions:scan(TableName, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

delete_last_id(AppId) ->
%%   Data = [{?TABLE_NAME, ?LAST_ID_TABLE},
%%     {"Key", [{?APP_ID, [{"S", AppId}]}
%%     ]}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   Response = erl_ddb:send_data(?DELETE_ITEM, JsonData),
%%   ddb_response:validate_response(Response).
  {TableName,Keys} = ddb_parameters:delete_last_id(AppId),
  Data = ddb_actions:delete_item(TableName, Keys),
  erl_ddb:hit_ddb(Data).


%% ***** KEY_PAIR Table *****
add_key_value(Key,Value) ->
%%   Data = [{?TABLE_NAME, ?KEY_PAIR_TABLE},
%%     {"Item",
%%       [{"TrKey", [{"S", Key}]},
%%         {"TrValue", [{"S", Value}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  {TableName,KeyValues} = ddb_parameters:add_key_value(Key,Value),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

get_key_value(Id) ->
%%   TableName = ?KEY_PAIR_TABLE,
%%   Keys = [{?TR_KEY, Id}],
%%   ProjectionExpression = ?TR_VALUE,
%%   ddb_actions:get_item(TableName, Keys, ProjectionExpression).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_key_value(Id),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

-spec get_all_key_values() ->
  {ok, [{AppId::string(),AccountId::string()}]} | {error, Reason::any()}.
get_all_key_values() ->
%%   Data = [{?TABLE_NAME, ?KEY_PAIR_TABLE}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, ProjectionExpression} = ddb_parameters:get_all_key_values(),
  Data = ddb_actions:scan(TableName, ProjectionExpression),
  erl_ddb:hit_ddb(Data).

update_logout_and_sessions_to_inactive(_AppId, []) -> ok;
update_logout_and_sessions_to_inactive(AppId, [H|Rest]) ->
  {AccountId, SsnDeviceList} = H,
  update_logout_and_sessions_to_inactive(AppId,AccountId,SsnDeviceList),
  update_logout_and_sessions_to_inactive(AppId, Rest).

update_logout_and_sessions_to_inactive(_AppId,_AccountId, []) -> ok;
update_logout_and_sessions_to_inactive(AppId,AccountId, [H|Rest]) ->
  {SessionId,UAId, CloseTime} = H,
  update_logout_for_session(AppId, SessionId,CloseTime),
  update_application_account_logout_time(AppId,AccountId, UAId, CloseTime),
  update_logout_and_sessions_to_inactive(AppId,AccountId, Rest).

update_logout_for_session(AppId, SessionId, CloseTime) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?SESSION_TABLE},
%%     {"Key", [
%%       {?SESSION_ID, [{"S", SessionId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "set SessionStatus = :val1, CloseTime = :val2"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", "INACTIVE"}]},
%%         {":val2", [{"S", CloseTime}]}
%%       ]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName,Key,UpdateAttrib} = ddb_parameters:update_logout_for_session(AppId, SessionId, CloseTime),
  Data = ddb_actions:update_set_item(TableName,Key, UpdateAttrib),
  erl_ddb:hit_ddb(Data).

update_application_account_logout_time(AppId, AccountId, UAId, LogoutTime) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?ACCOUNT_LOGOUT_TIME_TABLE},
%%     {"Key", [{?ACCOUNT_ID, [{"S", AccountId}]},
%%       {?UA_ID, [{"S", UAId}]}
%%     ]},
%%     {"UpdateExpression", "set LogoutTime = :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", LogoutTime}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName,Key,UpdateAttrib} = ddb_parameters:update_application_account_logout_time(AppId, AccountId, UAId, LogoutTime),
  Data = ddb_actions:update_set_item(TableName,Key, UpdateAttrib),
  erl_ddb:hit_ddb(Data).

%% update_logout_for_session(_AppId, []) -> ok;
%% update_logout_for_session(AppId, [H|Rest]) ->
%%   {SessionId,_UAId, CloseTime} = H,
%%   Data = [{?TABLE_NAME, AppId ++ ?SESSION_TABLE},
%%     {"Key", [
%%       {"SessionId", [{"S", SessionId}]},
%%       {"RangeKey", [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "set SessionStatus = :val1, CloseTime = :val2"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", "INACTIVE"}]},
%%         {":val2", [{"S", CloseTime}]}
%%       ]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData),
%%   update_logout_for_session(AppId, Rest).

add_account_logout_time(AppId, AccountId,UAId,AccountTime) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?ACCOUNT_LOGOUT_TIME_TABLE},
%%     {"Item",
%%       [
%%         {?ACCOUNT_ID, [{"S", AccountId}]},
%%         {?UA_ID, [{"S", UAId}]},
%%         {?LOGOUT_TIME, [{"S", AccountTime}]}
%%       ]}],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?PUT_ITEM, JsonData).
  {TableName,KeyValues} = ddb_parameters:add_account_logout_time(AppId, AccountId,UAId,AccountTime),
  Data = ddb_actions:put_item(TableName,KeyValues),
  erl_ddb:hit_ddb(Data).

update_user_agent(UAId,AppId,AccountId) ->
  AppAccountList = ddb_util:join_app_account(AppId,AccountId),
%%   Data = [{?TABLE_NAME, ?USER_AGENT_TABLE},
%%     {"Key", [{?UA_ID, [{"S", UAId}]}]},
%%     {"UpdateExpression", "add AppAccountList :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"SS", AppAccountList}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName, Keys, UpdateExpression} = ddb_parameters:update_user_agent(UAId,AppAccountList),
  Data = ddb_actions:update_add_item(TableName, Keys, UpdateExpression),
  erl_ddb:hit_ddb(Data).

get_application_account_logout_time(AppId, AccountId) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?ACCOUNT_LOGOUT_TIME_TABLE},
%%     {"FilterExpression", "AccountId = :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", AccountId}]}
%% %%         {":val2", [{"S", UAId}]}
%%       ]
%%     },
%%     {"ProjectionExpression", ?LOGOUT_TIME}
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?SCAN, JsonData).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_application_account_logout_time(AppId, AccountId),
  Data = ddb_actions:scan(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).


get_application_account_logout_time(AppId, AccountId, UAId) ->
%%   TableName = AppId ++ ?ACCOUNT_LOGOUT_TIME_TABLE,
%%   Keys = [{?ACCOUNT_ID, AccountId},{?UA_ID,UAId}],
%%   ProjectionExpression = ?LOGOUT_TIME,
%%   ddb_actions:get_item(TableName, Keys, ProjectionExpression).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_application_account_logout_time(AppId, AccountId, UAId),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).


update_last_req_time(AppId, SessionId, Time) ->
%%   Data = [{?TABLE_NAME, AppId ++ ?SESSION_TABLE},
%%     {"Key", [
%%       {?SESSION_ID, [{"S", SessionId}]},
%%       {?RANGE_KEY, [{"S", ?TRUE}]}
%%     ]},
%%     {"UpdateExpression", "set LastReqTime = :val1"},
%%     {"ExpressionAttributeValues",
%%       [{":val1", [{"S", Time}]}]
%%     }
%%   ],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?UPDATE_ITEM, JsonData).
  {TableName,Key,UpdateAttrib} = ddb_parameters:update_last_req_time(AppId, SessionId, Time),
  Data = ddb_actions:update_set_item(TableName,Key, UpdateAttrib),
  erl_ddb:hit_ddb(Data).

get_account_channels(AppId, AccountId) ->
%%   TableName = AppId ++ ?ACCOUNT_CHANNEL_TABLE,
%%   Keys = [{?ACCOUNT_ID, AccountId}],
%%   ProjectionExpression = ?CHANNEL_LIST,
%%   ddb_actions:get_item(TableName, Keys, ProjectionExpression).
  {TableName, Keys, ProjectionExpression} = ddb_parameters:get_account_channels(AppId, AccountId),
  Data = ddb_actions:get_item(TableName, Keys, ProjectionExpression),
  erl_ddb:hit_ddb(Data).
%%
delete_all_channels(AppId) ->
  ddb_tables:delete_table(AppId,?CHANNEL_TABLE),
  ddb_tables:delete_table(AppId,?CHANNEL_SUBSCRIPTION_TABLE),
  ddb_tables:delete_table(AppId,?ACCOUNT_CHANNEL_TABLE),
  ddb_tables:create_channel_table(AppId),
  ddb_tables:create_channel_subscription_table(AppId),
  ddb_tables:create_account_channel_table(AppId).


save_inital_auto_values(AppId,AccountId) ->
%%   Data = [{"RequestItems", [
%%     {?KEY_PAIR_TABLE, [
%%       [{"PutRequest", [
%%         {"Item", [
%%           {"TrKey", [{"S", ?APP_ID}]},
%%           {"TrValue", [{"S", AppId}]}
%%         ]}
%%       ]}],
%%       [{"PutRequest", [
%%         {"Item", [
%%           {"TrKey", [{"S", ?ACCOUNT_ID}]},
%%           {"TrValue", [{"S", AccountId}]}
%%         ]}
%%       ]}]
%%     ]}
%%   ]
%%   }],
%%   JsonData = jsonGenerator:create_json(Data),
%%   erl_ddb:send_data(?BATCH_WRITE_ITEM, JsonData).
  Req = ddb_parameters:save_inital_auto_values(AppId,AccountId),
  Data = ddb_actions:batch_write_request([{?KEY_PAIR_TABLE, Req}]),
  erl_ddb:hit_ddb(Data).


%% save_account_auto_values(AccountId,AccessId) ->
%%   Data = [{"RequestItems", [
%%     {?KEY_PAIR_TABLE, [
%%       [{"PutRequest", [
%%         {"Item", [
%%           {"TrKey", [{"S", ?ACCOUNT_ID}]},
%%           {"TrValue", [{"S", AccountId}]}
%%         ]}
%%       ]}],
%%       [{"PutRequest", [
%%         {"Item", [
%%           {"TrKey", [{"S", ?ACCESS_ID}]},
%%           {"TrValue", [{"S", AccessId}]}
%%         ]}
%%       ]}]
%%     ]}
%%   ]
%%   }],
%%   JsonData = jsonGenerator:create_json(Data),
%%   Response = erl_ddb:send_data(?BATCH_WRITE_ITEM, JsonData),
%%   ddb_response:validate_response(Response).

%% ====================================================================
%% Internal functions
%% ====================================================================

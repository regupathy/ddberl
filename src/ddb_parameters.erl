%%%-------------------------------------------------------------------
%%% @author regupathy.b
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Sep 2015 12:55 PM
%%%-------------------------------------------------------------------
-module(ddb_parameters).
-author("regupathy.b").

-include("json_keys.hrl").
-include("ddb_tables.hrl").
-include("ddb_status.hrl").


%% API
%% -export([]).
-compile(export_all).
-include("ddb_actions.hrl").
create_application(AppId,AppName,AppKey,AccessLimit,SecretKey) ->
  {?APPLICATION_TABLE,
    [{?APP_ID,AppId},
%%     {?RANGE_KEY,?TRUE},
    {?APP_NAME,AppName},
    {?APP_KEY,AppKey},
    {?ACCESS_LIMIT,AccessLimit},
    {?SECRET_KEY,SecretKey},
    {?APP_STATUS,?ACTIVE}]}.

update_application_status(AppId,Status) ->
  {?APPLICATION_TABLE,
    [{?APP_ID,AppId}],
    [{?APP_STATUS,Status}]}.

delete_application(AppId) ->
  {?APPLICATION_TABLE,
    [{?APP_ID,AppId}]}.

get_active_applications() ->
  {?APPLICATION_TABLE,
    [{?APP_STATUS,operation(#operation.eq),?ACTIVE}],
    [?APP_ID, ?APP_NAME, ?ACCESS_LIMIT, ?APP_KEY, ?SECRET_KEY]}.

get_applictaion_details(AppId) ->
  {?APPLICATION_TABLE,
    [{?APP_ID, AppId}],
    [?APP_ID, ?APP_NAME, ?ACCESS_LIMIT, ?APP_KEY,?SECRET_KEY,?APP_STATUS]}.

get_all_application_ids() ->
  {?APPLICATION_TABLE,[?APP_ID]}.

add_account(AccountId,AccountType,AccessId,AccountName,Password,AccountTime,UAIdList) ->
  {?ACCOUNT_TABLE,
    [{?ACCOUNT_ID,AccountId},
%%       {?RANGE_KEY,?TRUE},
      {?ACCOUNT_TYPE,AccountType},
      {?ACCESS_ID,AccessId},
      {?ACCOUNT_NAME,AccountName},
      {?PASSWORD,Password},
      {?CREATED_TIME,AccountTime},
%%       {?LOGOUT_TIME,AccountTime},
      {?UA_ID_LIST, <<"SS">>, UAIdList},
      {?ACCOUNT_STATUS,?ACTIVE}]}.

account_authentication(AccessId,Password) ->
  {?ACCOUNT_TABLE,
    [?ACCESS_ID,?PASSWORD],
    [{?ACCESS_ID,operation(#operation.eq),AccessId},{?PASSWORD,operation(#operation.eq),Password}],
    [?ACCOUNT_ID,?ACCOUNT_NAME]}.

update_application_list_in_account(AccountId, AppIdList) ->
  {?ACCOUNT_TABLE,
    [{?ACCOUNT_ID,AccountId}],
    [{?APP_LIST,<<"SS">>,AppIdList}]}.

update_user_agent_list_in_account(AccountId, UAIdList) ->
  {?ACCOUNT_TABLE,
    [{?ACCOUNT_ID,AccountId}],
    [{?UA_ID_LIST,<<"SS">>,UAIdList}]}.

%% remove_application_from_account(AccountId, AppIdList) ->
%%   {?ACCOUNT_TABLE,
%%     [{?ACCOUNT_ID,AccountId},{?RANGE_KEY,?TRUE}],
%%     [{?APPLICATION_LIST,<<"SS">>,AppIdList}]}.

get_accounts_details(AccessId) ->
  {?ACCOUNT_TABLE,
    [?ACCESS_ID,?PASSWORD],
    [{?ACCESS_ID,operation(#operation.eq),AccessId}],
    [?ACCOUNT_ID, ?ACCOUNT_NAME, ?ACCOUNT_TYPE, ?ACCESS_ID]}.

update_buddy_list(AccountId, BuddyList) ->
  {?ACCOUNT_TABLE,
    [{?ACCOUNT_ID,AccountId}],
    [{?BUDDY_LIST,<<"SS">>,BuddyList}]
  }.

%% update_logout_time(AccountId, Time) ->
%%   {?ACCOUNT_TABLE,
%%     [{?ACCOUNT_ID,AccountId}],
%%     [{?LOGOUT_TIME, Time}]
%%   }.

remove_account(AccountId) ->
  {?ACCOUNT_TABLE,
    [{?ACCOUNT_ID, AccountId}]
  }.

user_agent_availability(AccountId) ->
  {?ACCOUNT_TABLE,
    [{?ACCOUNT_ID, AccountId}],
    [?UA_ID_LIST]
  }.

get_all_active_accounts() ->
  {?ACCOUNT_TABLE,
    [{?ACCOUNT_STATUS,operation(#operation.eq),?ACTIVE}],
    [?ACCOUNT_ID, ?ACCOUNT_NAME, ?ACCOUNT_TYPE, ?ACCESS_ID, ?PASSWORD]}.

add_session(AppId, SessionId, AccountId, Location, CreatedTime, UAId) ->
  {ddb_util:merge_table_name(AppId,?SESSION_TABLE),
    [
      {?SESSION_ID,SessionId},
      {?ACCOUNT_ID, AccountId},
      {?USER_LOCATION, Location},
      {?CREATED_TIME, CreatedTime},
      {?UA_ID, UAId},
      {?SESSION_STATUS,?ACTIVE}
    ]
  }.

get_active_sessions(AppId,AccountId) ->
  {ddb_util:merge_table_name(AppId,?SESSION_TABLE),
    [{?ACCOUNT_ID,operation(#operation.eq),AccountId},{?SESSION_STATUS,operation(#operation.eq),?ACTIVE}],
    [?SESSION_ID]}.

get_all_active_sessions(AppId) ->
  {ddb_util:merge_table_name(AppId,?SESSION_TABLE),
    [{?SESSION_STATUS,operation(#operation.eq),?ACTIVE}],
    [?SESSION_ID]}.

create_channel(AppId,ChannelId, ChannelName, CreatedBy, CreatedTime, UserList,AdminList) ->
  {ddb_util:merge_table_name(AppId,?CHANNEL_TABLE),
    [
      {?CHANNEL_ID,ChannelId},
      {?CHANNEL_NAME,ChannelName},
      {?CREATED_BY,CreatedBy},
      {?CHANNEL_STATUS, ?ACTIVE},
      {?CREATED_TIME,CreatedTime},
      {?ACCOUNT_LIST,<<"SS">>, UserList},
      {?ADMIN_LIST,<<"SS">>,AdminList}
    ]
  }.

update_channel_in_account_channel_table(AppId,AccountId,ChannelIdList) ->
  {ddb_util:merge_table_name(AppId,?ACCOUNT_CHANNEL_TABLE),
    [{?ACCOUNT_ID, AccountId}],
    [{?CHANNEL_LIST, <<"SS">>,ChannelIdList}]
  }.

form_batch_chnl_sub_request(_ChannelId, _ChannelTime,[], Acc) -> lists:reverse(Acc);
form_batch_chnl_sub_request(ChannelId, ChannelTime, [AccountId|Rest], Acc) ->
  Item = [
    {?CHANNEL_ID, ChannelId},
    {?ACCOUNT_ID, AccountId},
    {?SUBSCRIPTION_TYPE, ?SUBSCRIBED},
    {?CHANNEL_TIME, ChannelTime}
  ],
  Req = ddb_actions:put_request(Item),
  form_batch_chnl_sub_request(ChannelId, ChannelTime, Rest, [Req|Acc]).

channel_authentication(AppId, ChannelName) ->
  {ddb_util:merge_table_name(AppId,?CHANNEL_TABLE),
    [?CHANNEL_NAME],
    [{?CHANNEL_NAME,operation(#operation.eq),ChannelName}],
    [?CHANNEL_ID]}.

batch_remove_channel([], Acc) -> Acc;
batch_remove_channel([H|Rest], Acc) ->
  {ChannelId, _AccountList} = H,
  Item = [{?CHANNEL_ID, ChannelId}],
  Req = ddb_actions:delete_request(Item),
  batch_remove_channel(Rest, [Req|Acc]).

batch_remove_channel_subscription([], Acc) -> lists:append(Acc);
batch_remove_channel_subscription([H|Rest], Acc) ->
  {ChannelId, AccountList} = H,
  Req = batch_remove_channel_subscription(ChannelId, AccountList, []),
  batch_remove_channel_subscription(Rest, [Req|Acc]).

batch_remove_channel_subscription(_ChannelId, [], Acc) -> Acc;
batch_remove_channel_subscription(ChannelId, [AccountId|Rest], Acc) ->
  Item = [{?CHANNEL_ID, ChannelId},{?ACCOUNT_ID,AccountId}],
  Req = ddb_actions:delete_request(Item),
  batch_remove_channel_subscription(ChannelId, Rest, [Req|Acc]).

update_list_in_channel_table(ListType, AppId, ChannelId, MemberList) ->
  {ddb_util:merge_table_name(AppId,?CHANNEL_TABLE),
    [{?CHANNEL_ID, ChannelId}],
    [{ListType, <<"SS">>, MemberList}]
  }.

get_admins_from_channel(AppId, ChannelId) ->
  {ddb_util:merge_table_name(AppId,?CHANNEL_TABLE),
    [{?CHANNEL_ID,ChannelId}],
    [?ADMIN_LIST]
  }.

update_channel_subscription_type(AppId, AccountId, ChannelId, SubscriptionType) ->
  {ddb_util:merge_table_name(AppId,?CHANNEL_SUBSCRIPTION_TABLE),
    [{?CHANNEL_ID, ChannelId},{?ACCOUNT_ID,AccountId}],
    [{?SUBSCRIPTION_TYPE,SubscriptionType}]
  }.

get_all_account_channels(AppId) ->
  {ddb_util:merge_table_name(AppId,?ACCOUNT_CHANNEL_TABLE),
    [?ACCOUNT_ID,?CHANNEL_LIST]
  }.

get_all_channel_ids(AppId) -> {ddb_util:merge_table_name(AppId,?CHANNEL_TABLE),[?CHANNEL_ID]}.

get_active_channels(AppId) ->
  {ddb_util:merge_table_name(AppId,?CHANNEL_TABLE),
    [{?CHANNEL_STATUS,operation(#operation.eq),?ACTIVE}],
    [?CHANNEL_ID,?CHANNEL_NAME,?ACCOUNT_LIST,?ADMIN_LIST]
  }.

save_message(AppId, MsgId,FromId,ToId,ToType,MessageType,Message,Time) ->
  {ddb_util:merge_table_name(AppId,?MESSAGE_TABLE),
    [
      {?MESSAGE_ID,MsgId},
      {?FROM_ID,FromId},
      {?TO_ID,ToId},
      {?TO_TYPE,ToType},
      {?MESSAGE_TYPE,MessageType},
      {?MESSAGE,Message},
      {?CREATED_TIME,Time}
    ]
  }.

save_msg_details_in_msg_queue(AppId, AccountId,MsgId,MsgStatus,MsgTime) ->
  {ddb_util:merge_table_name(AppId,?MESSAGE_QUEUE_TABLE),
    [
      {?ACCOUNT_ID, AccountId},
      {?MESSAGE_ID, MsgId},
      {?MESSAGE_STATUS,MsgStatus},
      {?MESSAGE_TIME,MsgTime}
    ]
  }.

save_group_message(AppId,MsgId,FromId,ToType,ToIdList,MessageType,Message,MsgTime) ->
  {ddb_util:merge_table_name(AppId,?MESSAGE_TABLE),
    [
      {?MESSAGE_ID,MsgId},
      {?FROM_ID,FromId},
      {?TO_TYPE,ToType},
      {?MESSAGE_TYPE,MessageType},
      {?MESSAGE,Message},
      {?GROUP_LIST, <<"SS">>,ToIdList},
      {?CREATED_TIME,MsgTime}
    ]
  }.

form_batch_msg_request(_MsgId, _MsgTime, [], Acc) -> lists:reverse(Acc);
form_batch_msg_request(MsgId, MsgTime, [AccountId|Rest], Acc) ->
  Item = [
    {?ACCOUNT_ID, AccountId},
    {?MESSAGE_ID, MsgId},
    {?MESSAGE_STATUS, ?RECEIVED},
    {?MESSAGE_TIME, MsgTime}
  ],
  Req = ddb_actions:put_request(Item),
  form_batch_msg_request(MsgId, MsgTime, Rest, [Req|Acc]).

get_message_by_message_id(AppId, MsgId) ->
  {ddb_util:merge_table_name(AppId,?MESSAGE_TABLE),
    [{?MESSAGE_ID, MsgId}],
    [?MESSAGE_ID,?MESSAGE_TYPE,?MESSAGE,?CREATED_TIME,?FROM_ID,?TO_TYPE,?TO_ID]
  }.

get_message_ids(AppId, AccountId, LogoutTime) ->
  {ddb_util:merge_table_name(AppId,?MESSAGE_QUEUE_TABLE),
    [?MESSAGE_TIME],
    [{?ACCOUNT_ID,operation(#operation.eq),AccountId},{?MESSAGE_TIME,operation(#operation.ge),LogoutTime}],
    [?MESSAGE_ID]
  }.

get_message_by_message_ids(AppId, MsgIdList) ->
  Keys = form_batch_get_msg_req(MsgIdList, []),
  {ddb_util:merge_table_name(AppId,?MESSAGE_TABLE),
    Keys,
    [?MESSAGE_ID,?MESSAGE_TYPE,?MESSAGE,?CREATED_TIME,?FROM_ID,?TO_TYPE,?TO_ID]
  }.

form_batch_get_msg_req([], Acc) -> lists:reverse(Acc);
form_batch_get_msg_req([MessageId|Rest], Acc) ->
  Req = [{?MESSAGE_ID,MessageId}],
  form_batch_get_msg_req(Rest, [Req|Acc]).

update_message_sent_time(AppId, MsgId,Time) ->
  {ddb_util:merge_table_name(AppId,?MESSAGE_TABLE),
    [{?MESSAGE_ID, MsgId}],
    [{?MESSAGE_SENT_TIME, Time}]
  }.

get_message_delivered_time(AppId,Time) ->
  {ddb_util:merge_table_name(AppId,?MESSAGE_TABLE),
    [{?CREATED_TIME,operation(#operation.ge),Time}],
    [?MESSAGE_ID,?FROM_ID,?TO_ID,?CREATED_TIME,?MESSAGE_SENT_TIME]}.

get_message_delivered_time_by_message_id(AppId, MsgId) ->
  {ddb_util:merge_table_name(AppId,?MESSAGE_TABLE),
    [{?MESSAGE_ID, MsgId}],
    [?CREATED_TIME,?MESSAGE_SENT_TIME]
  }.

add_last_id(AppId,ChannelId,MessageId,SessionId) ->
  {?LAST_ID_TABLE,
    [
      {?APP_ID,AppId},
      {?CHANNEL_ID,ChannelId},
      {?MESSAGE_ID,MessageId},
      {?SESSION_ID,SessionId}
    ]
  }.

update_last_id(Key, AppId, Id) ->
  {?LAST_ID_TABLE,
    [{?APP_ID,AppId}],
    [{Key, Id}]
  }.

get_last_id(Key, AppId) ->
  {?LAST_ID_TABLE,
    [{?APP_ID,AppId}],
    [Key]
  }.

get_all_last_ids(AppId) ->
  {?LAST_ID_TABLE,
    [{?APP_ID,AppId}],
    [?APP_ID, ?MESSAGE_ID, ?SESSION_ID, ?CHANNEL_ID]
  }.

get_all_last_ids() ->
  {?LAST_ID_TABLE,
    [?APP_ID, ?MESSAGE_ID, ?SESSION_ID, ?CHANNEL_ID]
  }.

delete_last_id(AppId) ->
  {?LAST_ID_TABLE,
    [{?APP_ID,AppId}]
  }.

add_key_value(Key,Value) ->
  {?KEY_PAIR_TABLE,
    [{?TR_KEY,Key},{?TR_VALUE,Value}]
  }.

get_key_value(Id) ->
  {?KEY_PAIR_TABLE,
    [{?TR_KEY,Id}],
    [?TR_VALUE]
  }.

get_all_key_values() ->
  {?KEY_PAIR_TABLE,
    [?TR_KEY,?TR_VALUE]
  }.

update_logout_for_session(AppId, SessionId, CloseTime) ->
  {ddb_util:merge_table_name(AppId,?SESSION_TABLE),
    [{?SESSION_ID,SessionId}],
    [{?SESSION_STATUS,?INACTIVE},{?CLOSE_TIME,CloseTime}]
  }.

update_application_account_logout_time(AppId, AccountId, UAId, LogoutTime) ->
  {ddb_util:merge_table_name(AppId,?ACCOUNT_LOGOUT_TIME_TABLE),
    [{?ACCOUNT_ID,AccountId},{?UA_ID,UAId}],
    [{?LOGOUT_TIME,LogoutTime}]
  }.

add_account_logout_time(AppId, AccountId,UAId,LogoutTime) ->
  {ddb_util:merge_table_name(AppId,?ACCOUNT_LOGOUT_TIME_TABLE),
    [
      {?ACCOUNT_ID,AccountId},
      {?UA_ID,UAId},
      {?LOGOUT_TIME, LogoutTime}
    ]
  }.

update_user_agent(UAId,AppAccountList) ->
  {?USER_AGENT_TABLE,
    [{?UA_ID,UAId}],
    [{?APP_ACCOUNT_LIST,<<"SS">>,AppAccountList}]
  }.

get_application_account_logout_time(AppId, AccountId) ->
  {ddb_util:merge_table_name(AppId,?ACCOUNT_LOGOUT_TIME_TABLE),
    [{?ACCOUNT_ID,operation(#operation.eq),AccountId}],
    [?LOGOUT_TIME]
  }.

get_application_account_logout_time(AppId, AccountId, UAId) ->
  {ddb_util:merge_table_name(AppId,?ACCOUNT_LOGOUT_TIME_TABLE),
    [{?ACCOUNT_ID,AccountId},{?UA_ID,UAId}],
    [?LOGOUT_TIME]
  }.

update_last_req_time(AppId, SessionId, Time) ->
  {ddb_util:merge_table_name(AppId,?SESSION_TABLE),
    [{?SESSION_ID,SessionId}],
    [{?LAST_REQ_TIME,Time}]
  }.

get_account_channels(AppId, AccountId) ->
  {ddb_util:merge_table_name(AppId,?ACCOUNT_CHANNEL_TABLE),
    [{?ACCOUNT_ID,AccountId}],
    [?CHANNEL_LIST]
  }.

save_inital_auto_values(AppId,AccountId) ->
  Item1 = [{?TR_KEY, ?APP_ID},{?TR_VALUE, AppId}],
  Item2 = [{?TR_KEY, ?ACCOUNT_ID},{?TR_VALUE, AccountId}],
  Req1 = ddb_actions:put_request(Item1),
  Req2 = ddb_actions:put_request(Item2),
  [Req1,Req2].

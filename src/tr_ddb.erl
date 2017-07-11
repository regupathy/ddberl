%%%-------------------------------------------------------------------
%%% @author regupathy.b
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Aug 2015 12:14 PM
%%%-------------------------------------------------------------------
-module(tr_ddb).
-author("regupathy.b").

%% API
%% -compile([export_all]).
-export([start/0]).

-export([create_application/6, pause_application/1, resume_application/1, delete_application/1,
  get_active_applications/0, get_application_details/1,get_all_application_ids/0]).

-export([add_account/7, account_authentication/2, verify_logout_time_to_add_user_agent/4, add_application_in_account/2,
  remove_application_from_account/2, update_user_agent_in_account/2,get_accounts_details/1,
  add_buddy/2, remove_buddy/2, remove_account/1, user_agent_availability/2, get_all_active_accounts/0]).

-export([add_session/6, get_active_sessions/2, get_all_active_sessions/1, update_last_req_time/3]).

-export([create_channel/6, channel_authentication/2, remove_channels/2, subscribe_channel/3, unsubscribe_channel/3,
  add_admins_to_channel/3, remove_admins_from_channel/3, get_admins_from_channel/2, add_channel_members/3,
  remove_channel_members/3, get_all_channel_ids/1, get_active_channels/1, get_all_account_channels/1]).

-export([save_direct_message/7, save_channel_message/8, save_group_message/7, get_message_by_message_id/2,
  get_message_by_account_id/3, get_message_by_time/3, update_message_sent_time/3, get_message_delivered_time/2,
  get_message_delivered_time_by_message_id/2]).

-export([add_last_id/4,get_last_message_id/1, get_last_session_id/1, get_last_channel_id/1, get_all_last_ids/1,
  get_all_last_ids/0]).

-export([save_inital_auto_values/2, add_key_value_AppId/1, add_key_value_AccountId/1, add_key_value_AccessKey/1,
  get_key_value_AppId/0, get_key_value_AccountId/0, get_key_value_AccessKey/0, get_all_key_values/0]).

-export([update_logout_and_sessions_to_inactive/2,add_account_logout_time/4,update_application_account_logout_time/4]).

-export([get_account_channels/2, delete_all_channels/1]).

-include("json_keys.hrl").
-include("ddb_tables.hrl").
-include("ddb_actions.hrl").
-include("ddb_range.hrl").
-include("ddb_status.hrl").

start() ->
  io:format("Application tr_ddb started successfully~n").

%% *** APPLICATION_TABLE ***
-spec create_application(AppId::string(),AppName::string(),AppKey::string(),AccessLimit::string(),
    SecretKey::string(),LastIds::tuple()) -> ok | {error, Reason::any()}.
create_application(AppId,AppName,AppKey,AccessLimit,SecretKey,LastIds) ->
%%   lager:info("DB: Create application with Appname: ~p, AppId: ~p",[AppName,AppId]),
  {ChannelId,MessageId,SessionId} = LastIds,
  ddb_tables:create_application_based_tables(ddb_util:check_and_convert_to_binary(AppId)),
  ddb_req_handler:create_application(AppId,AppName,AppKey,AccessLimit,SecretKey),
  add_key_value(?APP_ID,AppId),
  add_last_id(AppId,ChannelId,MessageId,SessionId).

-spec pause_application(AppId::string()) -> ok | {error, Reason::any()}.
pause_application(AppId) ->
%%   lager:info("DB: Pause application for AppId: ~p",[AppId]),
  update_application_status(AppId, ?PAUSE).

-spec resume_application(AppId::string()) -> ok | {error, Reason::any()}.
resume_application(AppId) ->
%%   lager:info("DB: Resume application for AppId: ~p",[AppId]),
  update_application_status(AppId, ?ACTIVE).

update_application_status(AppId, Status) ->
  ddb_req_handler:update_application_status(AppId, Status).

-spec delete_application(AppId::string()) -> ok | {error, Reason::any()}.
delete_application(AppId) ->
%%   lager:info("DB: Remove application for AppId ~p",[AppId]),
  ddb_req_handler:delete_application(AppId),
  ddb_req_handler:delete_last_id(AppId),
%%   remove_application_from_account(AccountId, AppId), %% Discussed: Need AccountId
  ddb_tables:delete_application_based_tables(ddb_util:check_and_convert_to_binary(AppId)).

-spec get_active_applications() ->
  {ok, [{AppID::string(),AppName::string(),AccessLimit::string(),Appkey::string(),SecretKey::string()}]} |
  not_found |
  {error, Reason::any()}.
get_active_applications() ->
  Response = ddb_req_handler:get_active_applications(),
  ddb_response:make_list_response(Response).

-spec get_application_details(AppId::string()) ->
  {AppID::string(),AppName::string(),AccessLimit::string(),Appkey::string(),SecretKey::string()} |
  not_found |
  {error, Reason::any()}.
get_application_details(AppId) ->
  Response = ddb_req_handler:get_applictaion_details(AppId),
  ddb_response:make_single_response(Response).

-spec get_all_application_ids() -> {ok, [AppIdList::list()]} | not_found | {error, Reason::any()}.
get_all_application_ids() ->
  Response = ddb_req_handler:get_all_application_ids(),
  IdList = ddb_util:check_and_convert_to_list(Response),
  ddb_response:make_list_response(IdList).


%% *** ACCOUNT_TABLE ***
-spec add_account(AppId::string(),AccountId::string(),AccountType::string(),AccessID::string(),AccountName::string(),
    Password::string(),UAId::string()) -> ok | {error, Reason::any()}.
add_account(AppId,AccountId,AccountType,AccessID,AccountName,Password,UAId) ->
%%   lager:info("DB: Add Account in AppId ~p with AccountName: ~p AccountId: ~p",[AppId,AccountName,AccountId]),
  AccountTime = ddb_util:get_time_stamp(),
  ddb_req_handler:add_account(AccountId,AccountType,AccessID,AccountName,Password,AccountTime,[UAId]),
  add_account_logout_time(AppId,AccountId,UAId,AccountTime),
  update_user_agent(UAId,AppId,AccountId),
  [StrAccountId] = ddb_util:strip_ids([AccountId]),
  add_key_value_AccountId(StrAccountId).

-spec account_authentication(AccessId::string(),Password::string()) ->
  {AccountId::string(),AccountName::string()} |
  not_found |
  {error, Reason::any()}.
account_authentication(AccessId,Password) ->
  Response = ddb_req_handler:account_authentication(AccessId,Password),
  ddb_response:make_single_response(Response).

-spec verify_logout_time_to_add_user_agent(AppId::string(),AccountId::string(), UAId::string(),
    AccountSsnStatus::boolean()) -> ok | {error, Reason::any()}.
%% verify_logout_time_to_add_user_agent("101_1001","102_1002", "device02", true)
verify_logout_time_to_add_user_agent(AppId,AccountId, UAId, AccountSsnStatus) ->
  case user_agent_availability(AccountId,UAId) of
    true ->
      {ok, OldTimeStamp} = get_user_agent_logout_time(AppId, AccountId, UAId),
      OldDate = calendar:gregorian_seconds_to_datetime(binary_to_integer(OldTimeStamp)),
      CurrentDate = ddb_util:get_date_time(),
      {Days, _Time} = calendar:time_difference(OldDate, CurrentDate),
      case Days > ?OFFLINE_MESSAGE_RANGE of
        true ->
          LogoutTime = ddb_util:get_time_stamp(),
          add_account_logout_time(AppId, AccountId,UAId,LogoutTime);
        false ->
          ok
      end;
    false ->
      update_user_agent(UAId,AppId,AccountId),
      update_user_agent_in_account(AccountId, [UAId]),
      case AccountSsnStatus of
        true ->
          LogoutTime = ddb_util:get_time_stamp(),
          add_account_logout_time(AppId, AccountId,UAId,LogoutTime);
        false ->
          {ok, LogoutTime} = get_last_logout_time(AppId, AccountId),
          add_account_logout_time(AppId, AccountId,UAId,LogoutTime)
      end
  end.

-spec add_application_in_account(AccountId::string(), AppId::string()) -> ok | {error, Reason::any()}.
add_application_in_account(AccountId, AppId) ->
  ddb_req_handler:add_application_in_account(AccountId, [AppId]).

-spec remove_application_from_account(AccountId::string(), AppId::string()) -> ok | {error, Reason::any()}.
remove_application_from_account(AccountId, AppId) ->
  ddb_req_handler:remove_application_from_account(AccountId, [AppId]).

-spec update_user_agent_in_account(AccountId::string(), UAIdList::list()) -> ok | {error, Reason::any()}.
update_user_agent_in_account(AccountId, UAIdList) ->
  ddb_req_handler:update_user_agent_in_account(AccountId, UAIdList).

-spec get_accounts_details(AccessId::string()) ->
  {AccountID::string(),AccountName::string(),AccountType::string(),AccessId::string()} |
  not_found.
get_accounts_details(AccessId) ->
  Response = ddb_req_handler:get_accounts_details(AccessId),
  ddb_response:make_single_response(Response).

%% Either single or List Ex: add_buddy("TR001",["TR002"]) |
%% add_buddy("TR001",["TR002","TR003","TR004","TR005"])
-spec add_buddy(AccountId::string(), BuddyList::list()) -> ok | {error, Reason::any()}.
add_buddy(AccountId, BuddyList) ->
  ddb_req_handler:add_buddy(AccountId, BuddyList).

-spec remove_buddy(AccountId::string(), BuddyList::list()) -> ok | {error, Reason::any()}.
remove_buddy(AccountId, BuddyList) ->
  ddb_req_handler:remove_buddy(AccountId, BuddyList).

%% -spec update_logout_time(AccountId::string(), Time::string()) -> ok | {error, Reason::any()}.
%% update_logout_time(AccountId, Time) ->
%%   ddb_req_handler:update_logout_time(AccountId, Time).

-spec remove_account(AccountId::string()) -> ok | {error, Reason::any()}.
remove_account(AccountId) ->
  ddb_req_handler:remove_account(AccountId).

-spec user_agent_availability(AccountId::string(),UAId::string()) -> Val::boolean().
user_agent_availability(AccountId,UAId) ->
  case ddb_req_handler:user_agent_availability(AccountId) of
    [] -> false;
    [{UAIdList}]  ->lists:member(UAId,UAIdList)
  end.

-spec get_all_active_accounts() -> {AccountId::string(), AccountName::string(), AccountType::string(),
  AccessID::string(), Password::string()} | not_found | {error, Reason::any()}.
get_all_active_accounts() ->
  Response = ddb_req_handler:get_all_active_accounts(),
  ddb_response:make_list_response(Response).


%% *** SESSION_TABLE ***
-spec add_session(AppId::string(), SessionId::string(), AccountId::string(),
    Location::string(), CreatedTime::string(), UAId::string()) -> ok | {error, Reason::any()}.
add_session(AppId,SessionId,AccountId,Location,CreatedTime,UAId) ->
%%   lager:info("DB: New Session in AppId ~p with AccountId: ~p SessionId: ~p",[AppId,AccountId,SessionId]),
  ddb_req_handler:add_session(AppId,SessionId,AccountId,Location, CreatedTime, UAId),
  update_last_session_id(AppId, SessionId).

-spec get_active_sessions(AppId::string(), AccountId::string()) -> {ok, ActiveSessionList::list()} | not_found.
%% get_active_session(AppId, AccountId) -> {ok, [ActiveSessionList]}.
get_active_sessions(AppId, AccountId) ->
  Response = ddb_req_handler:get_active_sessions(AppId, AccountId),
  IdList = ddb_util:check_and_convert_to_list(Response),
  ddb_response:make_list_response(IdList).

-spec get_all_active_sessions(AppId::string()) -> {ok, ActiveSessionList::list()} | not_found.
get_all_active_sessions(AppId) ->
  Response = ddb_req_handler:get_all_active_sessions(AppId),
  IdList = ddb_util:check_and_convert_to_list(Response),
  ddb_response:make_list_response(IdList).

-spec update_last_req_time(AppId::string(),SessionId::string(),Time::string()) -> ok.
update_last_req_time(AppId, SessionId,Time) ->
  ddb_req_handler:update_last_req_time(AppId, SessionId,Time).

%% *** CHANNEL_TABLE ***
-spec create_channel(AppId::string(), ChannelId::string(), ChannelName::string(), CreatedBy::string(),
    CreatedTime::string(), UserList::list()) -> ok | {error, Reason::any()}.
create_channel(AppId, ChannelId, ChannelName, CreatedBy, CreatedTime, UserList) ->
  ddb_req_handler:create_channel(AppId, ChannelId, ChannelName, CreatedBy, CreatedTime, UserList,[CreatedBy]),
  ddb_req_handler:add_to_account_channel_table(AppId, UserList,[ChannelId]),
  ddb_req_handler:add_to_channel_subscription_table(AppId,ChannelId,CreatedTime,UserList),
  update_last_channel_id(AppId, ChannelId).

-spec channel_authentication(AppId::string(), ChannelName::string()) -> {ChannelId::string()} | not_found.
channel_authentication(AppId, ChannelName) ->
  Response = ddb_req_handler:channel_authentication(AppId, ChannelName),
  ddb_response:make_single_response(Response).

-spec remove_channels(AppId::string(), ChannelIdAccountList::list()) -> ok | {error, Reason::any()}.
%% remove_channels(AppId, [{ChannelId, [AcclountList]}])
remove_channels(AppId, ChannelIdAccountList) ->
  ddb_req_handler:remove_channels(AppId, ChannelIdAccountList).

%% Subscription and unsubscription of channel will affect in 3 tables.(Chnl,AcntChnl and ChnlSubscrn tables)
-spec subscribe_channel(AppId::string(),AccountId::string(), ChannelId::string()) -> ok | {error, Reason::any()}.
subscribe_channel(AppId, AccountId, ChannelId) ->
  CreatedTime = ddb_util:get_time_stamp(),
  add_to_account_channel_table(AppId, AccountId, [ChannelId]),
  add_accounts_to_channel_table(AppId, ChannelId, [AccountId]),
  ddb_req_handler:add_to_channel_subscription_table(AppId,ChannelId,CreatedTime,[AccountId]).

add_to_account_channel_table(AppId, AccountId, ChannelIdList) ->
  ddb_req_handler:add_to_account_channel_table(AppId, [AccountId],ChannelIdList).

add_accounts_to_channel_table(AppId, ChannelId, UserList) ->
  ddb_req_handler:add_list_to_channel(?ACCOUNT_LIST, AppId, ChannelId, UserList).

-spec unsubscribe_channel(AppId::string(),AccountId::string(), ChannelId::string()) -> ok | {error, Reason::any()}.
unsubscribe_channel(AppId, AccountId, ChannelId) ->
  remove_channels_from_account_channel_table(AppId, AccountId, [ChannelId]),
  remove_accounts_from_channel(AppId, ChannelId, [AccountId]),
  update_channel_subscription_type(AppId, AccountId, ChannelId,?UNSUBSCRIBED).

remove_channels_from_account_channel_table(AppId, AccountId, ChannelList) ->
  ddb_req_handler:remove_channel_list_from_account_channel_table(AppId, [AccountId], ChannelList).

remove_accounts_from_channel(AppId, ChannelId, UserList) ->
  ddb_req_handler:remove_list_from_channel(?ACCOUNT_LIST, AppId, ChannelId, UserList).

update_channel_subscription_type(AppId, AccountId, ChannelId, SubscriptionType) ->
  ddb_req_handler:update_channel_subscription_type(AppId, AccountId, ChannelId, SubscriptionType).

-spec add_admins_to_channel(AppId::string(),ChannelId::string(),AdminList::list()) -> ok | {error, Reason::any()}.
add_admins_to_channel(AppId, ChannelId, AdminList) ->
  ddb_req_handler:add_list_to_channel(?ADMIN_LIST, AppId, ChannelId, AdminList).

-spec remove_admins_from_channel(AppId::string(),ChannelId::string(),AdminList::list()) -> ok | {error, Reason::any()}.
remove_admins_from_channel(AppId, ChannelId, AdminList) ->
  ddb_req_handler:remove_list_from_channel(?ADMIN_LIST, AppId, ChannelId, AdminList).

-spec get_admins_from_channel(AppId::string(), ChannelId::string()) -> {ok, AdminList::list()} | not_found.
get_admins_from_channel(AppId, ChannelId) ->
  Response = ddb_req_handler:get_admins_from_channel(AppId, ChannelId),
  IdList = ddb_util:check_and_convert_to_list(Response),
  {ok, IdList}.

-spec add_channel_members(AppId::string(),ChannelId::string(),Members::list()) -> ok | {error, Reason::any()}.
add_channel_members(AppId,ChannelId,Members) ->
  add_accounts_to_channel_table(AppId, ChannelId, Members).

-spec remove_channel_members(AppId::string(),ChannelId::string(),Members::list()) -> ok | {error, Reason::any()}.
remove_channel_members(AppId,ChannelId,Members) ->
  remove_accounts_from_channel(AppId,ChannelId,Members).

-spec get_all_channel_ids(AppId::string()) -> {ok, IdLis::list()} | not_found.
get_all_channel_ids(AppId) ->
  Response = ddb_req_handler:get_all_channel_ids(AppId),
  IdList = ddb_util:check_and_convert_to_list(Response),
  {ok, IdList}.

-spec get_active_channels(AppId::string()) -> {ok, ChannelDetails::list()} | not_found.
%% get_active_channels(AppId) -> [{ChnlId,ChannelName,[UserList],[AdminList]}]
get_active_channels(AppId) ->
  Response = ddb_req_handler:get_active_channels(AppId),
  ddb_response:make_list_response(Response).

-spec get_all_account_channels(AppId::string()) -> {ok, AccountChnlList::list()} | not_found.
get_all_account_channels(AppId) ->
  Response = ddb_req_handler:get_all_account_channels(AppId),
  ddb_response:make_list_response(Response).

%% ***** MESSAGE *****
-spec save_direct_message(AppId::string(),MsgId::string(),FromId::string(),ToId::string(),MessageType::string(),
    Message::string(),Time::string()) -> ok | {error, Reason::any()}.
save_direct_message(AppId,MsgId,FromId,ToId,MessageType,Message,Time) ->
  ddb_req_handler:save_direct_message(AppId, MsgId,FromId,ToId,MessageType,Message,Time),
  save_msg_details_in_msg_queue(AppId,FromId,MsgId,?SENT,Time),
  save_msg_details_in_msg_queue(AppId,ToId,MsgId,?RECEIVED,Time),
  update_last_message_id(AppId, MsgId).
%%   EncodedMsg = ddb_util:encode_to_base64(PlainMessage),
%%   Response = ddb_req_handler:save_direct_message(AppId, MsgId,FromId,ToId,MessageType,Message,Time),
%%   case ddb_response:validate_response(Response) of
%%     ok ->
%%       save_msg_details_in_msg_queue(AppId,FromId,MsgId,?SENT,Time),
%%       save_msg_details_in_msg_queue(AppId,ToId,MsgId,?RECEIVED,Time),
%%       update_last_message_id(AppId, MsgId);
%%     Any ->
%%       Any
%%   end.

save_msg_details_in_msg_queue(AppId,AccountId,MsgId,MsgStatus,MsgTime) ->
  ddb_req_handler:save_msg_details_in_msg_queue(AppId,AccountId,MsgId,MsgStatus,MsgTime).
%%   ddb_response:validate_response(Response).

-spec save_channel_message(AppId::string(),MsgId::string(),FromId::string(),ToId::string(),MessageType::string(),
    Message::string(),Time::string(),UserList::list()) -> ok | {error, Reason::any()}.
save_channel_message(AppId,MsgId,FromId,ToId,MessageType,Message,Time,UserList) ->
%%   EncodedMsg = ddb_util:encode_to_base64(PlainMessage),
  ddb_req_handler:save_channel_message(AppId, MsgId,FromId,ToId,MessageType,Message,Time),
  save_msg_details_in_msg_queue(AppId,FromId,MsgId,?SENT,Time),
  ddb_req_handler:save_msg_details_in_msg_queue_for_acc_list(AppId,MsgId,Time,UserList),
  update_last_message_id(AppId, MsgId).

-spec save_group_message(AppId::string(),MsgId::string(),FromId::string(),ToIdList::list(),
    MessageType::string(),Message::string(),MsgTime::string()) -> ok | {error, Reason::any()}.
save_group_message(AppId,MsgId,FromId,ToIdList,MessageType,Message,MsgTime) ->
  ddb_req_handler:save_group_message(AppId,MsgId,FromId,ToIdList,MessageType,Message,MsgTime),
  ddb_req_handler:save_msg_details_in_msg_queue(AppId,FromId,MsgId,?SENT, MsgTime),
  ddb_req_handler:save_msg_details_in_msg_queue_for_acc_list(AppId,MsgId,MsgTime,ToIdList),
  update_last_message_id(AppId, MsgId).

-spec get_message_by_message_id(AppId::string(),MsgId::string()) ->
  {MsgId::string(), MsgType::string(), Message::string(), Time::string(),
    FromId::string(),ToType::string(),ToId::string()} | {error, Reason::any()}.
get_message_by_message_id(AppId, MsgId) ->
  Response = ddb_req_handler:get_message_by_message_id(AppId, MsgId),
  ddb_response:make_single_response(Response).

-spec get_message_by_account_id(AppId::string(),AccountId::string(), UAId::string()) ->
  {ok, MessageRecords::list()} | {error, Reason::any()}.
get_message_by_account_id(AppId, AccountId, UAId) ->
  case get_user_agent_logout_time(AppId, AccountId, UAId) of
    {ok, LogoutTime} ->
      case get_message_ids(AppId, AccountId, LogoutTime) of
        {ok, MsgIdList} ->
          get_message_by_message_ids(AppId, MsgIdList);
        data_not_found ->
          io:format("Message Ids for AccountId ~p is not found~n",[AccountId]),
          not_found;
        Any ->
          Any
      end;
    not_found ->
      io:format("LogoutTime for AccountId ~p is not found~n",[AccountId]),
      not_found;
    Any ->
      Any
  end.

get_user_agent_logout_time(AppId, AccountId, UAId) ->
  case ddb_req_handler:get_application_account_logout_time(AppId, AccountId, UAId) of
    [] -> not_found;
    [{Time}] -> {ok, Time}
  end.

get_last_logout_time(AppId, AccountId) ->
  case ddb_req_handler:get_application_account_logout_time(AppId, AccountId) of
    [] -> not_found;
    Data ->
      TimeList = ddb_util:check_and_convert_to_list(Data),
      {ok, lists:max(TimeList)}
  end.

get_message_ids(AppId, AccountId, LogoutTime) ->
  Ids = ddb_req_handler:get_message_ids(AppId, AccountId, LogoutTime),
  IdList= ddb_util:check_and_convert_to_list(Ids),
  {ok, IdList}.

get_message_by_message_ids(AppId, MsgIdList) ->
  Response = ddb_req_handler:get_message_by_message_ids(AppId, MsgIdList),
  ddb_response:make_list_response(Response).

-spec get_message_by_time(AppId::string(),AccountId::string(),Time::string()) ->
  {ok, MessageRecords::list()} | {error, Reason::any()}.
get_message_by_time(AppId, AccountId, LogoutTime) ->
  {ok, MsgIdList} = get_message_ids(AppId, AccountId, LogoutTime),
  get_message_by_message_ids(AppId, MsgIdList).

update_message_sent_time(AppId, MsgId, Time) ->
  ddb_req_handler:update_message_sent_time(AppId, MsgId,Time).

get_message_delivered_time(AppId, Time) ->
  Response =  ddb_req_handler:get_message_delivered_time(AppId, Time),
  ddb_response:form_delivery_report(Response).

get_message_delivered_time_by_message_id(AppId, MsgId) ->
  ddb_req_handler:get_message_delivered_time_by_message_id(AppId, MsgId).

%% Discussed. not required.
%% -spec get_message_by_last_message_id(AppId::string(),AccountId::string(), MsgId::string()) ->
%%   {ok, MessageRecords::list()} | {error, Reason::any()}.
%% get_message_by_last_message_id(AppId, AccountId, MsgId) ->
%%   ok.


%% *** LAST_ID ***
-spec add_last_id(AppId::string(), MsgId::string(), SessionId::string(),
    ChannelId::string()) -> ok | {error, Reason::any()}.
add_last_id(AppId,ChannelId,MessageId,SessionId) ->
  [StrChannelId,StrMessageId,StrSessionId] = ddb_util:strip_ids([ChannelId,MessageId,SessionId]),
  ddb_req_handler:add_last_id(AppId,StrChannelId,StrMessageId,StrSessionId).

-spec update_last_message_id(AppId::string(), MsgId::string()) -> ok | {error, Reason::any()}.
update_last_message_id(AppId, MsgId) ->
  update_last_id(?MESSAGE_ID,AppId,MsgId).

update_last_session_id(AppId, SessionId) ->
  update_last_id(?SESSION_ID,AppId,SessionId).

update_last_channel_id(AppId, ChannelId) ->
  update_last_id(?CHANNEL_ID,AppId,ChannelId).

update_last_id(Key, AppId, Id) ->
  [StrId] = ddb_util:strip_ids([Id]),
  ddb_req_handler:update_last_id(Key, AppId, StrId).

-spec get_last_message_id(AppId::string()) -> {ok, MsgId::string()} | not_found.
get_last_message_id(AppId) ->
  get_last_id(?MESSAGE_ID, AppId).

-spec get_last_session_id(AppId::string()) -> {ok, SsnId::string()} | not_found.
get_last_session_id(AppId) ->
  get_last_id(?SESSION_ID, AppId).

-spec get_last_channel_id(AppId::string()) -> {ok, ChnlId::string()} | not_found.
get_last_channel_id(AppId) ->
  get_last_id(?CHANNEL_ID, AppId).

get_last_id(Key, Id) ->
  case ddb_req_handler:get_last_id(Key, Id) of
    [] -> not_found;
    [{Val}] -> {ok, Val}
  end.

-spec get_all_last_ids(AppId::string()) -> {AppId::string(), MessageId::string(), SessionId::string(),
  ChannelId::string()} | not_found.
get_all_last_ids(AppId) ->
  Response = ddb_req_handler:get_all_last_ids(AppId),
  ddb_response:make_single_response(Response).

-spec get_all_last_ids() -> {ok, {AppId::string(),MsgId::string(),SsnId::string(),ChnlId::string()}} | not_found.
get_all_last_ids() ->
  Response = ddb_req_handler:get_all_last_ids(),
  ddb_response:make_list_response(Response).

%% remove_last_message_id(AppId) -> ok.


%% ***** KEY_PAIR_TABLE *****
-spec save_inital_auto_values(AppId::string(),AccountId::string()) -> ok.
save_inital_auto_values(AppId,AccountId) ->
  [StrAppId,StrAccountId] = ddb_util:strip_ids([AppId,AccountId]),
  ddb_req_handler:save_inital_auto_values(StrAppId,StrAccountId).

-spec add_key_value_AppId(Value::string()) -> ok | {error, Reason::any()}.
add_key_value_AppId(Value) ->
  add_key_value(?APP_ID,Value).

-spec add_key_value_AccountId(Value::string()) -> ok | {error, Reason::any()}.
add_key_value_AccountId(Value) ->
  add_key_value(?ACCOUNT_ID,Value).

-spec add_key_value_AccessKey(Value::string()) -> ok | {error, Reason::any()}.
add_key_value_AccessKey(Value) ->
  add_key_value(?ACCESS_ID,Value).

add_key_value(Key,Value) ->
  [StrValue] = ddb_util:strip_ids([Value]),
  ddb_req_handler:add_key_value(Key,StrValue).

-spec get_key_value_AppId() -> {ok, Value::string()} | {error, Reason::any()}.
get_key_value_AppId() ->
  get_key_value(?APP_ID).

-spec get_key_value_AccountId() -> {ok, Value::string()} | {error, Reason::any()}.
get_key_value_AccountId() ->
  get_key_value(?ACCOUNT_ID).

-spec get_key_value_AccessKey() -> {ok, Value::string()} | {error, Reason::any()}.
get_key_value_AccessKey() ->
  get_key_value(?ACCESS_ID).

get_key_value(Id) ->
  Response = ddb_req_handler:get_key_value(Id),
  ddb_response:make_single_response(Response).

-spec get_all_key_values() ->
  {ok, [{AppId::string(),AccountId::string(),AccessId::string()}]} | {error, Reason::any()}.
get_all_key_values() ->
  Response = ddb_req_handler:get_all_key_values(),
  ddb_response:get_key_pair_values(Response).

%% ***** ACCOUNT_LOGOUT_TIME_TABLE *****
-spec update_logout_and_sessions_to_inactive(AppId::string(),AccountSessionList::list()) -> ok.
%% update_logout_and_sessions_to_inactive(AppId, [{AccId, [{SessionId,UAId,CloseTime}]},{}...]) %% Currentent format
update_logout_and_sessions_to_inactive(AppId, AccountSessionList) ->
  ddb_req_handler:update_logout_and_sessions_to_inactive(AppId, AccountSessionList).

add_account_logout_time(AppId, AccountId,UAId,AccountTime) ->
  ddb_req_handler:add_account_logout_time(AppId, AccountId,UAId,AccountTime).

-spec update_application_account_logout_time(AppId::string(),AccId::string(),UAId::string(),Time::string()) -> ok.
update_application_account_logout_time(AppId, AccountId, UAId, LogoutTime) ->
  ddb_req_handler:update_application_account_logout_time(AppId, AccountId, UAId, LogoutTime).

%% ***** USER_AGENT_TABLE *****
-spec update_user_agent(UAId::string(),AppId::string(),AccountId::string()) -> ok.
update_user_agent(UAId,AppId,AccountId) ->
  ddb_req_handler:update_user_agent(UAId,AppId,AccountId).

%% ***** ACCOUNT_CHANNEL_TABLE *****
get_account_channels(AppId, AccountId) ->
  case ddb_req_handler:get_account_channels(AppId, AccountId) of
    [] -> not_found;
    [{ChannelList}] -> {ok, ddb_util:check_and_convert_to_list(ChannelList)}
  end.

delete_all_channels(AppId) ->
  ddb_req_handler:delete_all_channels(AppId). %% Validated response

%%%-------------------------------------------------------------------
%%% @author natesh
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Aug 2015 12:24 PM
%%%-------------------------------------------------------------------
-module(ddb_response).
-author("natesh").

-include("json_keys.hrl").
%% API
-compile([export_all]).

make_single_response([]) -> not_found;
make_single_response([Data]) -> Data.

make_list_response([]) -> not_found;
make_list_response(Data) -> {ok, Data}.

validate_response(Response) ->
  case Response of
    {ok, {{_,200,"OK"},_Header, _ResponseData}} ->
      ok;
    {ok, {{_,400,_},_Header, ErrorMessage}} ->
      {error, ErrorMessage};
    _Any ->
      {error,_Any}
  end.


get_response(Response) ->
  case Response of
    {ok, {{_,200,"OK"},_Header, ResponseData}} ->
      get_data(ResponseData);
    {ok, {{_,400,_},_Header, ErrorMessage}} ->
      {error, ErrorMessage};
    _Any ->
      {error, _Any}
  end.


get_data(ResponseData) ->
  Data = json_parse(ResponseData),
  case Data of
    [{_Count,Value}, Items, _ScannedCount] ->
      case Value > 0 of
        true ->
          {_, AttributeList} = Items,
          {ok, lists:append(AttributeList)};
        false ->
          data_not_found
      end;
    [{_Item, AttributeList}] ->
      {ok, AttributeList};
    [] ->
      data_not_found
  end.

get_batch_response(Response) ->
  case Response of
    {ok, {{_,200,"OK"},_Header, ResponseData}} ->
      get_batch_data(ResponseData);
    {ok, {{_,400,_},_Header, ErrorMessage}} ->
      {error, ErrorMessage};
    _Any ->
      {error, _Any}
  end.

get_batch_data(ResponseData) ->
  [{_Responses, [{_Table, AttributeList}]},_UnprocessedKeys] = json_parse(ResponseData),
  case length(AttributeList) > 0 of
    true ->
      {ok, AttributeList};
    false ->
      data_not_found
  end.

get_single_value(ResponseList) ->
  case hd(ResponseList) of
    {_Key,[{_,Value}]} ->
      Value;
    _Any ->
      {error, _Any}
  end.

%% This function fetch the values in the same order as it is returned from DB.
get_list_of_values(ResponseList) ->
  Val = lists:foldl(fun(X, Acc) ->
    {_Key,[{_,Value}]} = X,
    [Value|Acc]
  end,
    [], ResponseList),
  lists:reverse(Val).

%% This function fetch the values and sort in an order.

get_user_values(ResponseList) ->
  UserKeyList = get_account_keys(),
  sort_single_item_response(ResponseList,UserKeyList).

get_application_details(ResponseList) ->
  AppKeyList = get_application_keys(),
  sort_single_item_response(ResponseList, AppKeyList).

get_active_applications(ResponseList) ->
  AppKeyList = get_application_keys(),
  sort_all_response(ResponseList,AppKeyList, AppKeyList,[],[]).

get_active_accounts(ResponseList) ->
  UserKeyList = get_account_keys(),
  sort_all_response(ResponseList,UserKeyList, UserKeyList,[],[]).

get_active_channels(ResponseList) ->
  ChannelKeyList = get_channel_keys(),
  sort_all_response(ResponseList,ChannelKeyList, ChannelKeyList,[],[]).

get_all_account_channels(ResponseList) ->
  KeyValList = get_list_of_values(ResponseList),
  form_key_and_value(KeyValList, []).
%% get_active_sessions(ResponseList) ->


get_account_values(ResponseList) ->
  AccountKeyList = get_account_keys(),
  sort_single_item_response(ResponseList,AccountKeyList).

get_message_values(ResponseList) ->
  MsgKeyList = get_message_keys(),
  sort_single_item_response(ResponseList,MsgKeyList).

get_list_of_message_values(ResponseList) ->
  get_list_of_message_values(ResponseList, []).

get_list_of_message_values([], Acc) -> lists:flatten(lists:reverse(Acc));
get_list_of_message_values([H|Rest], Acc) ->
  MsgKeyList = get_message_keys(),
  SortedList = sort_all_response(H, MsgKeyList, MsgKeyList,[],[]),
  get_list_of_message_values(Rest, [SortedList|Acc]).


%% get_key_pair_values(ResponseList) ->
%%   KeyValList = get_list_of_values(ResponseList),
%%   KeyPairTupleList = form_key_and_value(KeyValList, []),
%%   KeyPairList = get_key_pair_keys(),
%%   sort_key_val_response(KeyPairTupleList,KeyPairList).

get_key_pair_values(ResponseList) ->
  KeyPairList = get_key_pair_keys(),
  sort_key_val_response(ResponseList,KeyPairList).

form_key_and_value([], Acc) -> lists:reverse(Acc);
form_key_and_value([A,B|Rest], Acc) ->
  form_key_and_value(Rest, [{A,B}|Acc]).

get_all_last_ids(ResponseList) ->
  LastIdList = get_last_id_keys(),
  sort_all_response(ResponseList,LastIdList, LastIdList,[],[]).

sort_key_val_response(KeyPairTupleList,KeyPairList) ->
  ValueList = lists:foldl(fun(X, Acc) ->
    case lists:keyfind(binary_to_list(X), 1, KeyPairTupleList) of
      {_Key,Value}when is_integer(Value) ->
        [integer_to_list(Value)|Acc];
      {_Key,Value} ->
        [Value|Acc];
      false ->
        Acc
    end
  end,
    [], KeyPairList),
  lists:reverse(ValueList).

sort_single_item_response(ResponseList, KeyList) ->
  ValueList = lists:foldl(fun(X, Acc) ->
    case lists:keyfind(binary_to_atom(X, latin1), 1, ResponseList) of
      {_Key,[{_,Value}]} ->
        [Value|Acc];
      false ->
        Acc
    end
  end,
    [], KeyList),
  lists:reverse(ValueList).

sort_all_response([], [], _LastIdList, Acc, Result) ->
  [list_to_tuple(lists:reverse(Acc))|Result];

sort_all_response(ResponseList, [], KeyList, Acc, Result) ->
  sort_all_response(ResponseList, KeyList, KeyList, [], [list_to_tuple(lists:reverse(Acc))|Result]);

sort_all_response(ResponseList, [H|Rest], KeyList, Acc, Result) ->
     case lists:keytake(binary_to_atom(H, latin1), 1, ResponseList) of
      {value, {_Key,[{_,Value}]}, NewList} ->
        sort_all_response(NewList, Rest, KeyList, [Value|Acc], Result);
      false ->
        sort_all_response(ResponseList, Rest, KeyList, Acc, Result)
    end.


%% ***** GET KEYS *****

get_application_keys() ->
  [?APP_ID,?APP_NAME,?ACCESS_LIMIT,?APP_KEY,?SECRET_KEY,?APP_STATUS].

get_message_keys() ->
  [?MESSAGE_ID, ?MESSAGE_TYPE, ?MESSAGE, ?CREATED_TIME, ?FROM_ID, ?TO_TYPE, ?TO_ID].

%% get_user_keys() ->
%%   [?USER_ID,?USER_NAME,?PHONE_NUMBER,?EMAIL,?PASSWORD,?BUDDY_LIST,?CHANNEL_LIST].

get_last_id_keys() ->
  [?APP_ID, ?MESSAGE_ID, ?SESSION_ID, ?CHANNEL_ID].

get_account_keys() ->
  [?ACCOUNT_ID,?ACCOUNT_NAME,?ACCOUNT_TYPE,?ACCESS_ID,?LOGOUT_TIME,?PASSWORD].

get_key_pair_keys() ->
  [?APP_ID,?ACCOUNT_ID,?ACCESS_ID].

get_channel_keys() ->
  [?CHANNEL_ID,?CHANNEL_NAME,?ACCOUNT_LIST,?ADMIN_LIST].

json_parse(JsonData) ->
  {ok,Tokens,_} = jsonTokensGen:string(JsonData),
  {ok,[Res]}=jsonParser:parse(Tokens),
  Res.

form_delivery_report([]) -> invalid_time;
form_delivery_report(Response) -> form_delivery_report(Response, []).

form_delivery_report([], Acc) -> lists:reverse(Acc);
form_delivery_report([{MsgId,FromId,ToId,CreatedTime,"0"}|Rest], Acc) ->
  form_delivery_report(Rest, [{MsgId,FromId,ToId,CreatedTime,"0","unread_message"}|Acc]);
form_delivery_report([{MsgId,FromId,ToId,CreatedTime,SentTime}|Rest], Acc) ->
  TimeTaken = ddb_util:time_diff(CreatedTime,SentTime),
  form_delivery_report(Rest, [{MsgId,FromId,ToId,CreatedTime,SentTime,TimeTaken}|Acc]);
form_delivery_report(_Any, _Acc) -> "unread_message".
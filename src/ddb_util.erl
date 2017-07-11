%%%-------------------------------------------------------------------
%%% @author natesh
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Jul 2015 7:26 PM
%%%-------------------------------------------------------------------
-module(ddb_util).
-author("natesh").

%% API
%% -export([]).
-compile([export_all]).

%% For DDB signature
get_date_time_for_ddb_sign() ->
  {{Y,M,D},{Hr,Min,Sec}} = erlang:universaltime(),
  Datestamp = lists:flatten(io_lib:format("~4..0B~2..0B~2..0B",[Y,M,D])),
  Timestamp = Datestamp ++ lists:flatten(io_lib:format("T~2..0B~2..0B~2..0BZ",[Hr,Min,Sec])),
  {list_to_binary(Datestamp), list_to_binary(Timestamp)}.

get_date_time() ->
  erlang:universaltime().

get_time_stamp() ->
  DateTime = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
  integer_to_list(DateTime).

time_diff(T1,T2) -> list_to_integer(T2) - list_to_integer(T1).

%% Hexdigest
hash_encr_hexdigest(Data) ->
  base16(crypto:hash(sha256, Data)).

hmac_encr_hexdigest(Key, Data) ->
  base16(crypto:hmac(sha256, Key, Data)).


base16(Data) ->  << <<(hex(N div 16)), (hex(N rem 16))>> || <<N>> <= Data >>.

%% Convert an integer in the 0-16 range to a hexadecimal byte
%% representation.
hex(N) when N >= 0, N < 10 ->
  N + $0;
hex(N) when N < 16 ->
  N - 10 + $a.

binary_join([], _) -> <<"">>;
binary_join([H|[]], _) -> H;
binary_join(L, Sep) when is_list(Sep)  ->
  binary_join(L, list_to_binary(Sep));
binary_join([H|T], Sep) ->
  binary_join(T, H, Sep).

binary_join([], Acc, _) ->
  Acc;
binary_join([H|T], Acc, Sep) ->
  binary_join(T, <<Acc/binary, Sep/binary, H/binary>>, Sep).


%% Encode and decode the message content
encode_to_base64(Data) ->
  base64:encode(Data).

decode_from_base64(Data) ->
  base64:decode(Data).

strip_ids(IdList) -> strip_ids(IdList,[]).

strip_ids([],Acc) -> lists:reverse(Acc);
strip_ids([H|Rest],Acc) ->
  StringId = lists:last(re:split(H,"_",[{return,list}])),
  strip_ids(Rest, [StringId|Acc]).

join_app_account(Val1,Val2) -> [string:join([Val1,Val2], "/")].

name_index(AttribList) -> append_binary(lists:append(AttribList,[<<"index">>]),<<"-">>).

append_binary(List,Connector)when is_binary(Connector) -> list_to_binary(char_append(List,Connector,[])).
%% append_string(List,Connector)when is_list(Connector) -> lists:concat(char_append(List,Connector,[])).

char_append([],_,_) -> [];
char_append([LastChar|[]],_,Result) -> lists:reverse([LastChar|Result]);
char_append([H|T],Connector,Result) -> char_append(T,Connector,[Connector|[H|Result]]).

check_and_convert_to_binary(Value) when is_binary(Value) -> Value;
check_and_convert_to_binary(Value) when is_list(Value) -> list_to_binary(Value).

merge_table_name(Name1,Name2) ->
  Bin1= check_and_convert_to_binary(Name1),
  <<Bin1/binary,Name2/binary>>.

check_and_convert_to_list(IntList) -> check_and_convert_to_list(IntList, []).

check_and_convert_to_list([],Acc) -> lists:reverse(Acc);
check_and_convert_to_list([H|Rest], Acc) when is_list(H) -> check_and_convert_to_list(Rest, [H|Acc]);
check_and_convert_to_list([H|Rest], Acc) when is_integer(H) -> check_and_convert_to_list(Rest, [integer_to_list(H)|Acc]);
check_and_convert_to_list([H|Rest], Acc) when is_binary(H) -> check_and_convert_to_list(Rest, [binary_to_list(H)|Acc]);
check_and_convert_to_list([{H}|Rest], Acc) when is_list(H) -> check_and_convert_to_list(Rest, [H|Acc]);
check_and_convert_to_list([{H}|Rest], Acc) when is_binary(H) -> check_and_convert_to_list(Rest, [binary_to_list(H)|Acc]);
check_and_convert_to_list([{H}|Rest], Acc) when is_integer(H) -> check_and_convert_to_list(Rest, [integer_to_list(H)|Acc]).




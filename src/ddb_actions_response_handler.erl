%%%-------------------------------------------------------------------
%%% @author regupathy.b
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Oct 2015 11:10 AM
%%%-------------------------------------------------------------------
-module(ddb_actions_response_handler).
-author("regupathy.b").


%% API
-export([ddb_get_item/1,ddb_put_item/1,ddb_scan/1,ddb_query/1,ddb_batch_get_request/2,ddb_batch_write_request/1,ddb_update/1]).
-export([ddb_get_item/2,ddb_scan/3,ddb_query/2,ddb_batch_get_request/1]).

-include("ddb_actions.hrl").
%% ====================================================================
%% API functions
%% ====================================================================

ddb_get_item([]) -> [];
ddb_get_item([{}]) -> [];
ddb_get_item([{<<"Item">>,KeyValueList}]) -> KeyValueList.

ddb_get_item([],_ProjectionKeys) -> [];
ddb_get_item([{}],_ProjectionKeys) -> [];
ddb_get_item([{<<"Item">>,KeyValueList}],ProjectionKeys) -> rearrange_item(KeyValueList,ProjectionKeys).

ddb_put_item([]) -> ok;
ddb_put_item([{}]) -> ok.

ddb_scan([{<<"Count">>,_},{<<"Items">>,Items},{<<"ScannedCount">>,_}]) -> Items.

ddb_scan([{<<"Count">>,_},{<<"Items">>,Items},{<<"ScannedCount">>,_}], OrderKeys,_RequestData) ->
  rearrange_items(Items, OrderKeys);

ddb_scan([{<<"Count">>,_},{<<"Items">>,Items},{<<"LastEvaluatedKey">>,[]},{<<"ScannedCount">>,_}], OrderKeys,_Request) ->
  rearrange_items(Items, OrderKeys);

ddb_scan([{<<"Count">>,_},{<<"Items">>,Items},{<<"LastEvaluatedKey">>,LastEKey},{<<"ScannedCount">>,_}], OrderKeys,Request) ->
  io:format("Last Eval Key ~p~n",[LastEKey]),
  lists_append(rearrange_items(Items, OrderKeys), erl_ddb:hit_ddb(ddb_actions:scan_next(Request,LastEKey))).

ddb_query([{<<"Count">>,_},{<<"Items">>,Items},{<<"ScannedCount">>,_}]) -> Items.

ddb_query([{<<"Count">>,_},{<<"Items">>,Items},{<<"ScannedCount">>,_}], OrderKeys) -> rearrange_items(Items, OrderKeys).

ddb_update([]) -> ok;
ddb_update([{}]) -> ok.

ddb_batch_get_request([{<<"Responses">>,TableValues},{<<"UnprocessedKeys">>,UnprocessedKeys}],ProjectionKeys) ->
  lists:append(batch_value_arrange(TableValues,ProjectionKeys,[]),handle_unprocessed_items(?BATCH_GET_ITEM,UnprocessedKeys)).

ddb_batch_get_request([{<<"Responses">>,TableValues},{<<"UnprocessedKeys">>,UnprocessedKeys}]) ->
  lists:append(TableValues,handle_unprocessed_items(?BATCH_GET_ITEM,UnprocessedKeys)).

ddb_batch_write_request([{<<"UnprocessedItems">>,UnprocessedItems}]) ->
%%   io:format("~n UnprocessedItems ~n ~p~n",[UnprocessedItems]),
  handle_unprocessed_items(?BATCH_WRITE_ITEM,UnprocessedItems).

%% ====================================================================
%% Internal functions
%% ====================================================================

handle_unprocessed_items(_Action,[]) ->  [];
handle_unprocessed_items(_Action,[{}]) ->  [];
handle_unprocessed_items(Action,Items) ->
  erl_ddb:hit_ddb({Action,take_first_elements(Items),[{<<"RequestItems">>,Items}]}).

batch_value_arrange([],_,Result) -> lists:reverse(Result);
batch_value_arrange([{TableName,KeyValues}|Rest],ProjectionKeys,Result) ->
  {_,ProKeys} = lists:keyfind(TableName,1,ProjectionKeys),
  batch_value_arrange(Rest,ProjectionKeys,[{TableName,rearrange_items(KeyValues,ProKeys)}|Result]).

rearrange_item([],_ProjectionKeys) -> [];
rearrange_item(Items,ProjectionKeys) -> rearrange_items(ProjectionKeys,[Items],[]).
rearrange_items([],_ProjectionKeys) ->  [];
rearrange_items(Items,ProjectionKeys) ->  rearrange_items(ProjectionKeys,Items,[]).
rearrange_items(_Keys,[],Result) -> lists:reverse(Result);
rearrange_items(Keys,[H|T],Result) ->
  case arrange_values(Keys,H) of

    wrong -> rearrange_items(Keys,T,[H|Result]);

    Data -> rearrange_items(Keys,T,[Data|Result])

  end.

arrange_values(Keys,ValueSet) -> arrange_values(Keys,ValueSet,[],[]).
arrange_values([],_,_,Response) -> list_to_tuple(lists:reverse(Response));
arrange_values(_,[],_,_Response) -> wrong;
arrange_values([Key|Rest],[{Key,[{_,Val}]}|ValRest],UnMatch,Result) ->
  arrange_values(Rest,lists_append(ValRest,UnMatch),[],[Val|Result]);
arrange_values(Keys,[Set|ValRest],UnMatch,Result) ->
  arrange_values(Keys,ValRest,[Set|UnMatch],Result).

%% ====================================================================
%% Helper functions
%% ====================================================================

lists_append([],List) -> List;
lists_append([H|T],List) -> lists_append(T,[H|List]).

take_first_elements(Values) -> take_first_elements(Values,[]).
take_first_elements([{}],Result) -> lists:reverse(Result);
take_first_elements([],Result) -> lists:reverse(Result);
take_first_elements([{FirstElement,_}],Result) -> take_first_elements([],[FirstElement |Result]).




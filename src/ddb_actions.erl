%%%-------------------------------------------------------------------
%%% @author regupathy.b
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Sep 2015 4:06 PM
%%%-------------------------------------------------------------------
-module(ddb_actions).
-author("regupathy.b").

-include("json_keys.hrl").
-include("ddb_tables.hrl").


-define(MAX_BATCH_READ,100).
-define(MAX_BATCH_WRITE,25).

%% API
%% -export([]).
-compile([export_all]).

-export([delete_item/2,put_item/2,update_delete_item/3,update_set_item/3,scan/3]).

-export([update_add_item/3,put_request/1,delete_request/1,batch_write_request/1]).

%% -export([get_item/3,condition_with_index/4,condition_with_index/3,condition/3,condition/2]).

-export([batch_get_request/1,split_batch_requests/1]).

-include("ddb_actions.hrl").

%% ====================================================================
%% API functions
%% ====================================================================

update_throughput(TableName,ReadCount,WriteCount) ->
  {?UPDATE_TABLE,[TableName],[ddb_table(TableName),provisioned_throughput(ReadCount,WriteCount)]}.

describe_table(TableName) ->
  {?DESCRIBE_TABLE,[TableName],[ddb_table(TableName)]}.

delete_item(TableName,KeyValueSet) ->
  {?DELETE_ITEM,[TableName],[ddb_table(TableName),ddb_key(KeyValueSet)]}.

put_item(TableName,KeyValueSet) ->
  {?PUT_ITEM,[TableName],[ddb_table(TableName),ddb_items(KeyValueSet)]}.

get_item(TableName,Keys,ProjectionExp) ->
  {?GET_ITEM,[TableName],[ddb_table(TableName),ddb_key(Keys),ddb_projection_expression(ProjectionExp)]}.

update_delete_item(TableName,KeySet,ValueSet) ->
  {?UPDATE_ITEM,[TableName],ddb_update_item(<<"delete">>,TableName,KeySet,verify_update_valueset(<<" ">>,ValueSet))}.

update_set_item(TableName,KeySet,ValueSet) ->
  {?UPDATE_ITEM,[TableName],ddb_update_item(<<"set">>,TableName,KeySet,verify_update_valueset(operation(#operation.eq),ValueSet))}.

update_add_item(TableName,KeySet,ValueSet) ->
  {?UPDATE_ITEM,[TableName],ddb_update_item(<<"add">>,TableName,KeySet,verify_update_valueset(<<" ">>,ValueSet))}.

scan(TableName,FilterExpressionKeyValues, ProjectionKeys) ->
  {?SCAN,[TableName],lists:append([[ddb_table(TableName)|ddb_filter_expression(FilterExpressionKeyValues)],
  [ddb_projection_expression(ProjectionKeys)]])}.

scan_next({?SCAN,[TableName],[{<<"ExclusiveStartKey">>,_}|Rest]},LastEvalKey) ->
  {?SCAN,[TableName],[{<<"ExclusiveStartKey">>,LastEvalKey}|Rest]};

scan_next({?SCAN,[TableName],Rest},LastEvalKey) ->
  {?SCAN,[TableName],[{<<"ExclusiveStartKey">>,LastEvalKey}|Rest]}.

scan(TableName,ProjectionKeys) ->
  {?SCAN,[TableName],[ddb_table(TableName),ddb_projection_expression(ProjectionKeys)]}.

query_with_index_projection(TableName,Indexs,CondtionKeys,ProjectionKeys) ->
  {?QUERY,[TableName],lists:append([
    [ddb_table(TableName),ddb_index(Indexs)],
    ddb_key_condition_expression(CondtionKeys),
    [ddb_projection_expression(ProjectionKeys)]])}.

query_with_index(TableName,Indexs,CondtionKeys) ->
  {?QUERY,[TableName],lists:append([
    [ddb_table(TableName),ddb_index(Indexs)],
    ddb_key_condition_expression(CondtionKeys)])}.

query_with_projection(TableName,CondtionKeys,ProjectionKeys) ->
  {?QUERY,[TableName],lists:append([
    [ddb_table(TableName)],
    ddb_key_condition_expression(CondtionKeys),
    [ddb_projection_expression(ProjectionKeys)]])}.

query(TableName,CondtionKeys) ->
  {?QUERY,[TableName],[ddb_table(TableName)|ddb_key_condition_expression(CondtionKeys)]}.

put_request(ItemKeyValues) ->
  [{<<"PutRequest">>,[ddb_items(ItemKeyValues)]}].

delete_request(KeyValueSet) ->
  [{<<"DeleteRequest">>,[ddb_key(KeyValueSet)]}].

batch_get_request(Request) -> {?BATCH_GET_ITEM,take_first_elements(Request),[{<<"RequestItems">>,batch_get_request(Request,[])}]}.
batch_get_request([],Result) ->  lists:reverse(Result);
batch_get_request([{Table,Keys}|Rest],Result) ->
  Req = {Table,[ddb_keys(Keys)]},
  batch_get_request(Rest,[Req|Result]);
batch_get_request([{Table,Keys,ProjectionKeys}|Rest],Result) ->
  Req = {Table,[ddb_keys(Keys),ddb_projection_expression(ProjectionKeys)]},
  batch_get_request(Rest,[Req|Result]).

batch_write_request(TableNameWithRequest) ->
  {?BATCH_WRITE_ITEM,take_first_elements(TableNameWithRequest),[{<<"RequestItems">>,TableNameWithRequest}]}.

split_batch_requests([{<<"RequestItems">>,Requests}]) ->
  [[{<<"RequestItems">>,X}]|| X <- ddb_batch_request_spliter(Requests,1,[],[])].

order_batch_result(Keys,BatchValueSet) ->
  order_batch_result(Keys,BatchValueSet,[],[]).

%% ====================================================================
%% Internal functions
%% ====================================================================

ddb_table(TableName) -> {?TABLE_NAME,TableName}.

ddb_projection_expression(ColumnNames) -> {<<"ProjectionExpression">>, append_binary(ColumnNames,<<",">>)}.

ddb_index(ColumnNames) -> {<<"IndexName">>,append_binary(lists:append(ColumnNames,[<<"index">>]),<<"-">>)}.

ddb_key(KeyValues) -> {<<"Key">>,ddb_key(KeyValues,[])}.
ddb_key([],KeyValues)-> lists:reverse(KeyValues);
ddb_key([{ColumnName,Value}|Rest],Items) ->
  ddb_key(Rest,[{ColumnName,[{<<"S">>,to_binary(Value)}]}|Items]);
ddb_key([{ColumnName,Datatype,Value}|Rest],Items) ->
  ddb_key(Rest,[{ColumnName,[{Datatype,Value}]}|Items]).

ddb_keys(Keys) -> {<<"Keys">>,ddb_keys(Keys,[])}.
ddb_keys([],Result) -> lists:reverse(Result);
ddb_keys([H|T],Result) -> ddb_keys(T,[ddb_key(H,[])|Result]).

ddb_items(Items) -> {<<"Item">>,ddb_items(Items,[])}.
ddb_items([],Items) -> lists:reverse(Items);
ddb_items([{ColumnName,Value}|Rest],Items) ->
  ddb_items(Rest,[{ColumnName,[{<<"S">>,to_binary(Value)}]}|Items]);
ddb_items([{ColumnName,Datatype,Value}|Rest],Items) ->
  ddb_items(Rest,[{ColumnName,[{Datatype,Value}]}|Items]).

ddb_update_item(Action,TableName,KeySet,ValueSet) ->
  lists:append([ddb_table(TableName),ddb_key(KeySet)],ddb_update_expression(Action,ValueSet)).

ddb_filter_expression(KeyOperationValueSet) ->
  {Expresssion,ExpressionAttribute} = ddb_expression(KeyOperationValueSet,<<" AND ">>),
  [{<<"FilterExpression">>,Expresssion},ExpressionAttribute].

ddb_key_condition_expression(KeyOperationValueSet) ->
  {Expresssion,ExpressionAttribute} = ddb_expression(KeyOperationValueSet,<<" AND ">>),
  [{<<"KeyConditionExpression">>,Expresssion},ExpressionAttribute].

ddb_update_expression(Action, KeyOperationValueSet) ->
  {Expresssion,ExpressionAttribute} = ddb_expression(KeyOperationValueSet,<<" , ">>),
  [{<<"UpdateExpression">>,<<Action/binary,<<" ">>/binary,Expresssion/binary>>},ExpressionAttribute].

ddb_expression(KeyOperationValueSet,Connector) -> ddb_expression(1, KeyOperationValueSet,[],[],Connector).
ddb_expression(_,[],Items,Expression,Connector)->
  { append_binary(lists:reverse(Expression), Connector),
    ddb_expression_attribute_values(Items)};
ddb_expression(Level,[{ColumnName,Operation,{Datatype,Value}}|Rest],Items,Expression,Connector) ->
  Key = list_to_binary(":val"++integer_to_list(Level)),
  Exp = append_binary([ColumnName,Key],Operation),
  ddb_expression(Level+1,Rest,[{Key,Datatype,Value}|Items],[Exp|Expression],Connector);
ddb_expression(Level,[{ColumnName,Operation,Value}|Rest],Items,Expression,Connector) ->
  ddb_expression(Level,[{ColumnName,Operation,{<<"S">>,to_binary(Value)}}|Rest],Items,Expression,Connector).

ddb_expression_attribute_values(ValueSet) ->
  {<<"ExpressionAttributeValues">>,ddb_expression_attribute_values(ValueSet,[])}.
ddb_expression_attribute_values([],ValueSets) -> lists:reverse(ValueSets);
ddb_expression_attribute_values([{ColumnName,Value}|Rest],Items) ->
  ddb_expression_attribute_values(Rest,[{ColumnName,[{<<"S">>,to_binary(Value)}]}|Items]);
ddb_expression_attribute_values([{ColumnName,Datatype,Value}|Rest],Items) ->
  ddb_expression_attribute_values(Rest,[{ColumnName,[{Datatype,Value}]}|Items]).

ddb_batch_request_spliter([],_,[],Result) ->
  lists:reverse(Result);
ddb_batch_request_spliter([],_,BalKeys,Result) ->
  lists:reverse([BalKeys|Result]);
ddb_batch_request_spliter([{TableName,[{<<"Keys">>,Keys},ProjectionKeys]}|Rest],CurrentCount,BalKeys,Result) ->
  MaxCount = ?MAX_BATCH_READ,
  {NextCount,Temp,NewResult}=spliter(MaxCount,CurrentCount,Keys,{read,TableName,ProjectionKeys},Result,BalKeys),
  ddb_batch_request_spliter(Rest,NextCount,Temp,NewResult);

ddb_batch_request_spliter([{TableName,[{<<"Keys">>,Keys}]}|Rest],CurrentCount,BalKeys,Result) ->
  MaxCount = ?MAX_BATCH_READ,
  {NextCount,Temp,NewResult}=spliter(MaxCount,CurrentCount,Keys,{read,TableName},Result,BalKeys),
  ddb_batch_request_spliter(Rest,NextCount,Temp,NewResult);

ddb_batch_request_spliter([{TableName,WriteRequests}|Rest],CurrentCount,BalKeys,Result) ->
  MaxCount = ?MAX_BATCH_WRITE,
  {NextCount,Temp,NewResult}=spliter(MaxCount,CurrentCount,WriteRequests,{write,TableName},Result,BalKeys),
  ddb_batch_request_spliter(Rest,NextCount,Temp,NewResult).

provisioned_throughput(NewReadCount,NewWriteCount) ->
  {<<"ProvisionedThroughput">>,[{<<"ReadCapacityUnits">>,NewReadCount},{<<"WriteCapacityUnits">>,NewWriteCount}]}.

spliter(MaxCount,CurrentCount,Requests,SaveSchema,ResultSet,Temp)->
  case split_by_count(MaxCount,CurrentCount,Requests) of

    {[],[],[],CurrentCount} -> {CurrentCount,Temp,ResultSet};

    {[],[],TempSet,BalanceCount} ->
      {BalanceCount,[add_request(SaveSchema,TempSet)|Temp],ResultSet};

    {[],CompleteSet,[],BalanceCount} ->
      Sets = [[add_request(SaveSchema,K)] || K <- CompleteSet],
      {BalanceCount,[],append_list(Sets,ResultSet)};

    {[],CompleteSet,TempSet,BalanceCount} ->
      Sets = [[add_request(SaveSchema,K)] || K <- CompleteSet],
      {BalanceCount,[add_request(SaveSchema,TempSet)],append_list(Sets,ResultSet)};

    {FirstSet,[],[],BalanceCount} ->
      if BalanceCount =:= 1
        ->  {BalanceCount,[],[lists:reverse([add_request(SaveSchema,FirstSet)|Temp])|ResultSet]};
        true ->  {BalanceCount,[add_request(SaveSchema,FirstSet)|Temp],ResultSet}
      end;

    {FirstSet,[],TempSet,BalanceCount} ->
      {BalanceCount,[add_request(SaveSchema,TempSet)],[lists:reverse([add_request(SaveSchema,FirstSet)|Temp])|ResultSet]};

    {FirstSet,CompleteSet,[],BalanceCount} ->
      Sets = [[add_request(SaveSchema,K)] || K <- CompleteSet],
      {BalanceCount,[],append_list(Sets,[lists:reverse([add_request(SaveSchema,FirstSet)|Temp])|ResultSet])};

    {FirstSet,CompleteSet,TempSet,BalanceCount} ->
      Sets = [[add_request(SaveSchema,K)] || K <- CompleteSet],
      {BalanceCount,[add_request(SaveSchema,TempSet)],append_list(Sets,[add_request(SaveSchema,FirstSet)|ResultSet])}

  end.

add_request({write,TableName},Requests) -> {TableName,Requests};
add_request({read,TableName,Projections},Requests) -> {TableName,[{<<"Keys">>,Requests},Projections]};
add_request({read,TableName},Requests) -> {TableName,[{<<"Keys">>,Requests}]}.


order_batch_result([],_,_Temp,Result) ->
  lists:reverse(Result);

order_batch_result(Keys,[],Temp,Result) ->
  order_batch_result(tl(Keys),Temp,[],Result);

order_batch_result([[{Key,Val}]|Rest],[[{Key,[{_,Val}]}|_]= Data|KeyBatchValueSet],Temp,Result) ->
  order_batch_result(Rest,append_list(KeyBatchValueSet,Temp),[],[Data|Result]);

order_batch_result([[{Key1,Val1},{Key2,Val2}]|Rest],[[{Key1,[{_,Val1}]},{Key2,[{_,Val2}]}|_]= Data|KeyBatchValueSet],Temp,Result) ->
  order_batch_result(Rest,append_list(KeyBatchValueSet,Temp),[],[Data|Result]);

order_batch_result([[{_Key,Val}]|Rest],[[Val|_]= Data|KeyBatchValueSet],Temp,Result) ->
  order_batch_result(Rest,append_list(KeyBatchValueSet,Temp),[],[Data|Result]);

order_batch_result([[{_Key1,Val1},{_Key2,Val2}]|Rest],[[Val1,Val2|_]= Data|KeyBatchValueSet],Temp,Result) ->
  order_batch_result(Rest,append_list(KeyBatchValueSet,Temp),[],[Data|Result]);

order_batch_result(Keys,[Data|KeyBatchValueSet],Temp,Result) ->
  order_batch_result(Keys,KeyBatchValueSet,[Data|Temp],Result).

verify_update_valueset(Operation,KeyValue) -> verify_update_valueset(Operation,KeyValue,[]).
verify_update_valueset(_,[],Result) ->lists:reverse(Result);
verify_update_valueset(Operation,[{Key,Value}|Rest],Result) ->
  verify_update_valueset(Operation,Rest,[{Key,Operation,Value}|Result]);
verify_update_valueset(Operation,[{Key,DataType,Value}|Rest],Result) ->
  verify_update_valueset(Operation,Rest,[{Key,Operation,{DataType,Value}}|Result]).

%% ====================================================================
%% Helper functions
%% ====================================================================

append_list([],List) -> List;
append_list([H|T],List) -> append_list(T,[H|List]).

split_by_count(MaxCount,1,List) ->
  {CompleteSet,TempSet,BalanceCount} = split_by_count(MaxCount,1,List,[],[]),
  {[],CompleteSet,TempSet,BalanceCount};

split_by_count(MaxCount,CurrentCount,List) ->
  case split_by_count(MaxCount,CurrentCount,List,[],[]) of

    {[],TempSet,BalanceCount} ->
      {[],[],TempSet,BalanceCount};

    {[FirstSet|CompleteSet],TempSet,BalanceCount} ->
      {FirstSet,CompleteSet,TempSet,BalanceCount}
  end.

split_by_count(MaxCount,MaxCount,[],TempList,ResultSet) ->
  {lists:reverse([lists:reverse(TempList)|ResultSet]),[],1};
split_by_count(_,BalanceCount,[],TempList,ResultSet) ->
  {lists:reverse(ResultSet),lists:reverse(TempList),BalanceCount};
split_by_count(MaxCount,MaxCount,[H|T],TempList,ResultSet) ->
  split_by_count(MaxCount,1,T,[],[lists:reverse([H|TempList])|ResultSet]);
split_by_count(MaxCount,TempCount,[H|T],TempList,ResultSet) ->
  split_by_count(MaxCount,TempCount+1,T,[H|TempList],ResultSet).


append_binary(List,Connector)when is_binary(Connector) -> list_to_binary(char_append(List,Connector,[])).
%% append_string(List,Connector)when is_list(Connector) -> lists:concat(char_append(List,Connector,[])).

char_append([],_,_) -> [];
char_append([LastChar|[]],_,Result) -> lists:reverse([LastChar|Result]);
char_append([H|T],Connector,Result) -> char_append(T,Connector,[Connector|[H|Result]]).

to_binary(N) when is_integer(N) -> integer_to_binary(N);
to_binary(Bin) when is_binary(Bin) -> Bin;
to_binary(List) when is_list(List) -> list_to_binary(List).

take_first_elements(Values) -> take_first_elements(Values,[]).
take_first_elements([],Result) -> lists:reverse(Result);
take_first_elements([{FirstElement,_}|Rest],Result) -> take_first_elements(Rest,[FirstElement |Result]);
take_first_elements([_|Rest],Result) -> take_first_elements(Rest,Result).





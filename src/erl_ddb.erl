%%%-------------------------------------------------------------------
%%% @author regupathy.b
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Jul 2015 4:11 PM
%%%-------------------------------------------------------------------
-module(erl_ddb).
-author("regupathy.b").

-include("ddb.hrl").
-include("ddb_tables.hrl").


%% API
-export([start/0, send_data/2,hit_ddb/1,batch_hit_ddb/1,get_throughput/1,parse/1]).
-include("ddb_actions.hrl").

%% ====================================================================
%% API functions
%% ====================================================================

start() ->
  inets:start(),
  ssl:start().

send_data(Action, ReqParameters) ->
  EndPoint = ?DDB_END_POINT,
  ContentType = binary_to_list(?DDB_CONTENT_TYPE),
  Headers = ddb_sign:get_headers(Action, ReqParameters),
  httpc:request(post, {EndPoint, Headers, ContentType, ReqParameters}, [], []).

hit_ddb({Action,Tables,Data}) ->
%%   io:format("Data ~p~n",[{Action,Data}]),
  Projections = get_projection_keys(Action,Data),
%%   io:format("Projections ~p~n",[Projections]),
  JsonData = jsonGenerator:create_json(Data),
  Response = get_ddb_response(Action,Tables,JsonData),
%%   io:format("Response ~p~n",[Response]),
  handle_ddb_reponse(Action,Projections,Response,{Action,Tables,Data}).

batch_hit_ddb({Action,Tables,RequestData}) ->
  BatchList = ddb_actions:split_batch_requests(RequestData),
  merge_same_table_values([hit_ddb({Action,Tables,Data})|| Data <- BatchList]).

%% ====================================================================
%% Internal functions
%% ====================================================================

merge_same_table_values([]) -> [];
merge_same_table_values([H|T]) ->
  merge_same_table_values([H],T).
merge_same_table_values(Total,[]) ->
  lists:reverse(Total);
merge_same_table_values([[{Table,Values1}]|Total],[[{Table,Values2}]|Rest]) ->
  merge_same_table_values([{Table,lists:append(Values1,Values2)}|Total],Rest);
merge_same_table_values(Total,[[{Table,Values}]|Rest]) ->
  merge_same_table_values([{Table,Values}|Total],Rest);
merge_same_table_values(Total,[_|Rest]) ->
  merge_same_table_values(Total,Rest).


handle_ddb_reponse(?GET_ITEM,no_projection,Data,_) ->  ddb_actions_response_handler:ddb_get_item(Data);
handle_ddb_reponse(?GET_ITEM,Projections,Data,_) ->  ddb_actions_response_handler:ddb_get_item(Data,Projections);
handle_ddb_reponse(?PUT_ITEM,_,Data,_) -> ddb_actions_response_handler:ddb_put_item(Data);
handle_ddb_reponse(?QUERY,no_projection,Data,_) -> ddb_actions_response_handler:ddb_query(Data);
handle_ddb_reponse(?QUERY,Projections,Data,_) -> ddb_actions_response_handler:ddb_query(Data,Projections);
handle_ddb_reponse(?SCAN,no_projection,Response,_RequestData) -> ddb_actions_response_handler:ddb_scan(Response);
handle_ddb_reponse(?SCAN,Projections,Response,RequestData) -> ddb_actions_response_handler:ddb_scan(Response,Projections,RequestData);
handle_ddb_reponse(?UPDATE_ITEM,_,Response,_) -> ddb_actions_response_handler:ddb_update(Response);
handle_ddb_reponse(?BATCH_GET_ITEM,no_projection,Response,_) -> ddb_actions_response_handler:ddb_batch_get_request(Response);
handle_ddb_reponse(?BATCH_GET_ITEM,Projections,Response,_) -> ddb_actions_response_handler:ddb_batch_get_request(Response,Projections);
handle_ddb_reponse(?BATCH_WRITE_ITEM,_,Response,_) -> ddb_actions_response_handler:ddb_batch_write_request(Response);

handle_ddb_reponse(?CREATE_TABLE,_,Response,_) -> Response;
handle_ddb_reponse(?DELETE_ITEM,_,Response,_) -> Response;
handle_ddb_reponse(?DELETE_TABLE,_,Response,_) -> Response;
handle_ddb_reponse(?LIST_TABLES,_,Response,_) -> Response;
handle_ddb_reponse(?DESCRIBE_TABLE,_,Response,_) -> Response;
handle_ddb_reponse(?UPDATE_TABLE,_,Response,_) -> Response;
handle_ddb_reponse(Action,_,_Response,_) -> throw({unknown_action,Action}).

get_ddb_response(Action, Tables,JsonData) ->
  case process_response(send_data(Action,JsonData)) of
    {ok,Response} -> Response;
    retry ->increase_ProvisionedThroughput(Action,Tables),
      get_ddb_response(Action, Tables,JsonData)
  end.

process_response(Response) ->
  case Response of
    {ok, {{_,200,"OK"},_Header, ResponseData}} ->
%%       io:format("Response Data ~p~n",[ResponseData]),
      {ok,json_parser(ResponseData)};
    {ok, {{_,400,_},_Header, ErrorMessage}} ->
      handle_expections(parse(ErrorMessage));
    Any ->
      throw({ddb_hit_error,Any})
  end.

%%
%% handle_expections([{__type,"com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"}|_])->
%%    retry;
%%
%% handle_expections([{__type,"com.amazonaws.dynamodb.v20120810#ResourceInUseException"}|_])->
%%   retry;

handle_expections(ErrorMessage) ->  throw({ddb_hit_error,ErrorMessage}).


get_projection_keys(?BATCH_GET_ITEM,[{<<"RequestItems">>,TableNameWithRequest}]) ->  loop(TableNameWithRequest,[]);
get_projection_keys(?GET_ITEM, [{?TABLE_NAME,_TableName}|GetData]) -> get_projections(GetData);
get_projection_keys(?QUERY,[{?TABLE_NAME,_TableName}|QueryData]) ->  get_projections(QueryData);
get_projection_keys(?SCAN, [{?TABLE_NAME,_TableName}|ScanData]) -> get_projections(ScanData);
get_projection_keys(?SCAN, [{<<"ExclusiveStartKey">>,_},{?TABLE_NAME,_TableName}|ScanData]) -> get_projections(ScanData);
get_projection_keys(_Action,_Data) -> no_need.


loop([],Result) -> lists:reverse(Result);
loop([{TableName,Data}|Rest],Result) ->
  loop(Rest,[{TableName,get_projections(Data)}|Result]).

get_projections(Data) ->
  case lists:keyfind(<<"ProjectionExpression">>,1,Data) of
    {_,ProjectionList} -> to_binarys(re:split(ProjectionList,<<",">>,[{return,list}]),[]);

    _ -> no_projection
  end.

increase_ProvisionedThroughput(_Action,[])   -> ok;
increase_ProvisionedThroughput(Action,[TableName|Rest])   ->
  {ReadCapacity,WriteCapacity} = get_throughput(TableName),
  NewReadCapacity = update_count(Action,read_actions(),ReadCapacity,10),
  NewWriteCapacity = update_count(Action,write_actions(),WriteCapacity,10),
  Data = ddb_actions:update_throughput(TableName,NewReadCapacity,NewWriteCapacity),
  hit_ddb(Data),
  increase_ProvisionedThroughput(Action,Rest).

get_throughput(TableName) ->
  Data = ddb_actions:describe_table(TableName),
  [{<<"Table">>,Informations}] = hit_ddb(Data),
  {_, ThroughPuts} =lists:keyfind(<<"ProvisionedThroughput">>,1,Informations),
  {_, ReadCapacity} =lists:keyfind(<<"ReadCapacityUnits">>,1,ThroughPuts),
  {_, WriteCapacity} =lists:keyfind(<<"WriteCapacityUnits">>,1,ThroughPuts),
  {ReadCapacity,WriteCapacity}.

update_count(Action,Actions,OldCount,Incr) ->
  case lists:member(Action,Actions) of
    true -> OldCount+Incr;

    false -> OldCount
  end.

%% ====================================================================
%% Helper functions
%% ====================================================================

to_binarys([],Result) -> lists:reverse(Result);
to_binarys([H|T],Result) -> to_binarys(T,[list_to_binary(string:strip(H))|Result]).

json_parser(JSONDATA)when is_list(JSONDATA) ->json_parser(list_to_binary(JSONDATA));
json_parser(JSONDATA)when is_binary(JSONDATA) ->
  try erljson_decoder:scan(JSONDATA) of
    {ok,JSON} -> JSON
  catch
    ErrorType:Reason ->
      throw({json_parser_error,ErrorType,Reason})
  end.

parse(JsonData) ->
  io:format("Json data is ~p~n",[JsonData]),
  {ok,Tokens,_} = jsonTokensGen:string(JsonData),
%%   io:format("Tokens ~p~n",[Tokens]),
  {ok,[Res]}=jsonParser:parse(Tokens),
  Res.


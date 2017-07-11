%%%-------------------------------------------------------------------
%%% @author natesh
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Jul 2015 4:14 PM
%%%-------------------------------------------------------------------
-module(ddb_sign).
-author("natesh").

-include("ddb.hrl").
%% API
-export([get_headers/2]).

get_headers(Action, RequestParameters) ->
  Method = ?POST,
  Service = ?DDB_SERVICE,
  Host = ?DDB_HOST,
  Region = ?DDB_REGION,
  ContentType = ?DDB_CONTENT_TYPE,
  AmzTarget = <<?DDB_VERSION/binary, ".", Action/binary>>,
  AccessKey = ?DDB_ACCESS_KEY,
  SecretKey = ?DDB_SECRET_KEY,
  Algorithm = ?DDB_ALGORITHM,
  {Datestamp, Timestamp} = ddb_util:get_date_time_for_ddb_sign(),
%%   ContentLength = integer_to_list(length(RequestParameters)),
  CanonicalUri = ?DDB_URI,
  CanonicalQuerystring = ?DDB_QUERY_STRING,
  CanonicalHeaders = <<"content-type:", ContentType/binary, "\nhost:", Host/binary,
    "\nx-amz-date:", Timestamp/binary, "\nx-amz-target:", AmzTarget/binary, "\n">>,
  SignedHeaders = <<"content-type;host;x-amz-date;x-amz-target">>,
  PayloadHash = ddb_util:hash_encr_hexdigest(RequestParameters),
  CanonicalRequest = ddb_util:binary_join([Method, CanonicalUri, CanonicalQuerystring,
    CanonicalHeaders, SignedHeaders, PayloadHash], <<"\n">>),
  CredentialScope = ddb_util:binary_join([Datestamp, Region, Service, <<"aws4_request">>], "/"),
  HashCancalReq = ddb_util:hash_encr_hexdigest(CanonicalRequest),
  StringToSign = ddb_util:binary_join([Algorithm, Timestamp, CredentialScope, HashCancalReq], <<"\n">>),
  SigningKey = get_signature_key(SecretKey, Datestamp, Region, Service),
  Signature = ddb_util:hmac_encr_hexdigest(SigningKey, StringToSign),
%%   Task4
  AuthorizationHeader = authorization_header(Algorithm, AccessKey, CredentialScope, SignedHeaders, Signature),
  HeaderList = [
    {<<"host">>, Host},
    {<<"content-type">>, ContentType},
    {<<"x-amz-date">>, Timestamp},
    {<<"x-amz-target">>, AmzTarget},
    {<<"x-amz-content-sha256">>, PayloadHash},
    {<<"Authorization">>, AuthorizationHeader}
    ],
  binary_tuple_to_list(HeaderList, []).



get_signature_key(SecretKey, Datestamp, Region, Service) ->
  Key = <<"AWS4", SecretKey/binary>>,
  KDate = sign(Key, Datestamp),
  KRegion = sign(KDate, Region),
  KService = sign(KRegion, Service),
  sign(KService, "aws4_request").

sign(Key, Msg) ->
%%   BinMsg = unicode:characters_to_binary(Msg, utf8),
  crypto:hmac(sha256, Key, Msg).

authorization_header(Algorithm, AccessKey, CredentialScope, SignedHeaders, Signature) ->
  <<Algorithm/binary, " Credential=", AccessKey/binary, "/", CredentialScope/binary,
      ", SignedHeaders=", SignedHeaders/binary, ", Signature=", Signature/binary>>.


binary_tuple_to_list([],Result) ->lists:reverse(Result);

binary_tuple_to_list([{Key,Value}|Rest],Result)when ( is_binary(Key) and is_binary(Value)) ->
  binary_tuple_to_list(Rest,[{binary_to_list(Key),binary_to_list(Value)}|Result]);

binary_tuple_to_list([{Key,Value}|Rest],Result)when ( is_binary(Key) and is_list(Value)) ->
  binary_tuple_to_list(Rest,[{binary_to_list(Key),Value}|Result]);

binary_tuple_to_list([{Key,Value}|Rest],Result)when (is_list(Key) and is_binary(Value)) ->
  binary_tuple_to_list(Rest,[{Key,binary_to_list(Value)}|Result]);

binary_tuple_to_list([{Key,Value}|Rest],Result)when (is_list(Key) and is_list(Value)) ->
  binary_tuple_to_list(Rest,[{Key,Value}|Result]).
%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emq_clientid_unique).

-behaviour(gen_server).

-author("Ren Mingxin <renmingxin@gmail.com>").

-include_lib("emqttd/include/emqttd.hrl").
-include_lib("emqttd/include/emqttd_protocol.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([load/1, unload/0]).

%% Hooks functions
-export([on_client_authed/3, on_client_disconnected/3]).

%% API Function Exports
-export([start_link/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2]).

-record(mqtt_client_conned, {client_id, client_pid}).
-record(state, {}).

%% Called when the plugin application start
load(Env) ->
    emqttd:hook('client.authed', fun ?MODULE:on_client_authed/3, [Env]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]).

on_client_authed(_ConnAck, Client = #mqtt_client{client_id = ClientId, client_pid = ClientPid}, _Env) ->
    case mnesia:dirty_read(mqtt_client_conned, iolist_to_binary(ClientId)) of
    [_ClientConn] ->
        exit({shutdown, kill});
    [] ->
        mnesia:dirty_write(#mqtt_client_conned{client_id = ClientId, client_pid = ClientPid}),
        {ok, Client}
    end.

on_client_disconnected(_Reason, _Client = #mqtt_client{client_id = ClientId}, _Env) ->
    mnesia:dirty_delete(mqtt_client_conned, iolist_to_binary(ClientId)),
    ok.

%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.authed', fun ?MODULE:on_client_authed/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start the client_unique
-spec(start_link(Env :: list()) -> {ok, pid()} | ignore | {error, any()}).
start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Env], []).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([_Env]) ->
    {Type, Copies} = {set, ram_copies},
    ok = ekka_mnesia:create_table(mqtt_client_conned, [
                {type, Type},
                {Copies, [node()]},
                {record_name, mqtt_client_conned},
                {attributes, record_info(fields, mqtt_client_conned)},
                {storage_properties, [{ets, [compressed]}]}]),
    ok = ekka_mnesia:copy_table(mqtt_client_conned),
    {ok, #state{}}.

handle_call(_Req, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


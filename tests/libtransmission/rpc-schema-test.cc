// This file Copyright © Mnemosyne LLC.
// It may be used under GPLv2 (SPDX: GPL-2.0-only), GPLv3 (SPDX: GPL-3.0-only),
// or any future license endorsed by Mnemosyne LLC.
// License text can be found in the licenses/ folder.

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <rfl.hpp>
#include <rfl/json.hpp>

#include <libtransmission/rpc-schema.h>
#include <libtransmission/rpcimpl.h>
#include <libtransmission/transmission.h>

#include "test-fixtures.h"

using namespace std::literals;

namespace tr::test
{

namespace
{

[[nodiscard]] std::string execJson(tr_session* session, std::string_view const request_json)
{
    auto response = std::string{};
    tr_rpc_request_exec_json(session, request_json, [&response](std::string&& resp) { response = std::move(resp); });
    return response;
}

} // namespace

TEST(RpcSchemaTest, RequestIdRoundTripAndValidation)
{
    using tr::rpc::JsonRpcRequestEnvelope;
    using tr::rpc::RequestId;

    // Integer id
    {
        auto const parsed = rfl::json::read<JsonRpcRequestEnvelope>(R"({"jsonrpc":"2.0","method":"session_stats","id":42})");
        ASSERT_TRUE(parsed) << parsed.error().what();
        EXPECT_FALSE(parsed->id.value().get().is_notification());
        EXPECT_EQ(parsed->id.value().get(), RequestId::from_int(42));
    }

    // String id
    {
        auto const parsed = rfl::json::read<JsonRpcRequestEnvelope>(
            R"({"jsonrpc":"2.0","method":"session_stats","id":"req-99"})");
        ASSERT_TRUE(parsed) << parsed.error().what();
        EXPECT_EQ(parsed->id.value().get(), RequestId::from_string("req-99"));
    }

    // Floating-point id
    {
        auto const parsed = rfl::json::read<JsonRpcRequestEnvelope>(R"({"jsonrpc":"2.0","method":"session_stats","id":0.5})");
        ASSERT_TRUE(parsed) << parsed.error().what();
        EXPECT_EQ(parsed->id.value().get(), RequestId::from_double(0.5));
    }

    // Explicit null id (not a notification)
    {
        auto const parsed = rfl::json::read<JsonRpcRequestEnvelope>(R"({"jsonrpc":"2.0","method":"session_stats","id":null})");
        ASSERT_TRUE(parsed) << parsed.error().what();
        EXPECT_FALSE(parsed->id.value().get().is_notification());
        EXPECT_EQ(parsed->id.value().get(), RequestId::null_id());
    }

    // Omitted id (notification)
    {
        auto const parsed = rfl::json::read<JsonRpcRequestEnvelope>(R"({"jsonrpc":"2.0","method":"session_stats"})");
        ASSERT_TRUE(parsed) << parsed.error().what();
        EXPECT_TRUE(parsed->id.value().get().is_notification());
    }

    // Invalid id types (boolean, array, object) -> ErrorKind::invalid_type -> INVALID_REQUEST (-32600)
    for (auto const bad_json : std::array{
             R"({"jsonrpc":"2.0","method":"session_stats","id":true})"sv,
             R"({"jsonrpc":"2.0","method":"session_stats","id":[]})"sv,
             R"({"jsonrpc":"2.0","method":"session_stats","id":{}})"sv,
         })
    {
        auto const parsed = rfl::json::read<JsonRpcRequestEnvelope>(bad_json);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error().kind(), rfl::ErrorKind::invalid_type);
        EXPECT_EQ(tr::rpc::classify_envelope_error(parsed.error().kind()), JsonRpc::Error::INVALID_REQUEST);
    }
}

TEST(RpcSchemaTest, EnvelopeRawParamsAndResponsesRoundTrip)
{
    using tr::rpc::JsonRpcErrorData;
    using tr::rpc::JsonRpcErrorObject;
    using tr::rpc::JsonRpcErrorResponse;
    using tr::rpc::JsonRpcRequestEnvelope;
    using tr::rpc::JsonRpcSuccessResponse;
    using tr::rpc::RequestId;

    // Request envelope with deferred rfl::json::Raw params
    auto const req = rfl::json::read<JsonRpcRequestEnvelope>(
        R"({"jsonrpc":"2.0","method":"torrent_get","params":{"ids":[1,"deadbeef"],"fields":["id","name"]},"id":7})");
    ASSERT_TRUE(req) << req.error().what();
    EXPECT_EQ(req->method.value(), "torrent_get");
    ASSERT_TRUE(req->params.value().has_value());

    // Parse deferred params subtree directly into typed TorrentGetParams
    auto const params = rfl::json::read<tr::rpc::TorrentGetParams>(req->params.value()->str());
    ASSERT_TRUE(params) << params.error().what();
    ASSERT_TRUE(params->fields.value().has_value());
    EXPECT_EQ(*params->fields.value(), (std::vector<std::string>{ "id", "name" }));
    ASSERT_TRUE(params->ids.value().has_value());
    auto const* ids_vec = std::get_if<std::vector<tr::rpc::TorrentIdElement>>(&*params->ids.value());
    ASSERT_NE(ids_vec, nullptr);
    ASSERT_EQ(ids_vec->size(), 2U);
    EXPECT_EQ(std::get<int64_t>((*ids_vec)[0]), 1);
    EXPECT_EQ(std::get<std::string>((*ids_vec)[1]), "deadbeef");

    // Success response round-trip with rfl::json::Raw result
    auto const ok_resp = JsonRpcSuccessResponse{
        .result = rfl::json::Raw{ R"({"blocklist_size":1234})" },
        .id = RequestId::from_int(7),
    };
    auto const ok_json = rfl::json::write(ok_resp);
    auto const ok_back = rfl::json::read<JsonRpcSuccessResponse>(ok_json);
    ASSERT_TRUE(ok_back) << ok_back.error().what();
    EXPECT_EQ(ok_back->id.value(), RequestId::from_int(7));
    auto const bl_res = rfl::json::read<tr::rpc::BlocklistUpdateResult>(ok_back->result.value().str());
    ASSERT_TRUE(bl_res) << bl_res.error().what();
    EXPECT_EQ(bl_res->blocklist_size.value(), 1234);

    // Error response round-trip
    auto const err_resp = JsonRpcErrorResponse{
        .error = JsonRpcErrorObject{
            .code = static_cast<int16_t>(JsonRpc::Error::INVALID_PARAMS),
            .message = std::string{ JsonRpc::Error::to_string(JsonRpc::Error::INVALID_PARAMS) },
            .data = JsonRpcErrorData{ .error_string = "bad param"s },
        },
        .id = RequestId::from_string("err-1"),
    };
    auto const err_json = rfl::json::write(err_resp);
    auto const err_back = rfl::json::read<JsonRpcErrorResponse>(err_json);
    ASSERT_TRUE(err_back) << err_back.error().what();
    EXPECT_EQ(err_back->error.value().code.value(), -32602);
    EXPECT_EQ(err_back->error.value().message.value(), "Invalid params");
    ASSERT_TRUE(err_back->error.value().data.value().has_value());
    EXPECT_EQ(err_back->error.value().data.value()->error_string.value(), "bad param");
    EXPECT_EQ(err_back->id.value(), RequestId::from_string("err-1"));
}

TEST(RpcSchemaTest, ErrorKindClassification)
{
    // Syntax error -> -32700 PARSE_ERROR
    {
        auto const res = rfl::json::read<tr::rpc::JsonRpcRequestEnvelope>("{not json");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().kind(), rfl::ErrorKind::syntax_error);
        EXPECT_EQ(tr::rpc::classify_envelope_error(res.error().kind()), JsonRpc::Error::PARSE_ERROR);
        EXPECT_EQ(tr::rpc::classify_params_error(res.error().kind()), JsonRpc::Error::PARSE_ERROR);
    }

    // Wrong jsonrpc version -> -32600 INVALID_REQUEST
    {
        auto const res = rfl::json::read<tr::rpc::JsonRpcRequestEnvelope>(
            R"({"jsonrpc":"1.0","method":"session_stats","id":1})");
        ASSERT_FALSE(res);
        EXPECT_EQ(tr::rpc::classify_envelope_error(res.error().kind()), JsonRpc::Error::INVALID_REQUEST);
    }

    // Missing required method -> -32600 INVALID_REQUEST
    {
        auto const res = rfl::json::read<tr::rpc::JsonRpcRequestEnvelope>(R"({"jsonrpc":"2.0","id":1})");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().kind(), rfl::ErrorKind::missing_field);
        EXPECT_EQ(tr::rpc::classify_envelope_error(res.error().kind()), JsonRpc::Error::INVALID_REQUEST);
    }

    // Missing required param (`free_space.path`) -> -32602 INVALID_PARAMS
    {
        auto const res = rfl::json::read<tr::rpc::FreeSpaceParams>("{}");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().kind(), rfl::ErrorKind::missing_field);
        EXPECT_EQ(tr::rpc::classify_params_error(res.error().kind()), JsonRpc::Error::INVALID_PARAMS);
    }

    // Invalid enum literal (`port_test.ip_protocol`) -> -32602 INVALID_PARAMS
    {
        auto const res = rfl::json::read<tr::rpc::PortTestParams>(R"({"ip_protocol":"icmp"})");
        ASSERT_FALSE(res);
        EXPECT_EQ(tr::rpc::classify_params_error(res.error().kind()), JsonRpc::Error::INVALID_PARAMS);
    }

    // Wrong param type (`torrent_remove.delete_local_data`) -> -32602 INVALID_PARAMS
    {
        auto const res = rfl::json::read<tr::rpc::TorrentRemoveParams>(R"({"delete_local_data":"yes"})");
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().kind(), rfl::ErrorKind::invalid_type);
        EXPECT_EQ(tr::rpc::classify_params_error(res.error().kind()), JsonRpc::Error::INVALID_PARAMS);
    }
}

TEST(RpcSchemaTest, TypedMethodStructsRoundTrip)
{
    // SessionStatsResult
    {
        auto const stats = tr::rpc::SessionStatsResult{
            .active_torrent_count = 3,
            .cumulative_stats = tr::rpc::SessionTransferStats{
                .downloaded_bytes = 1024,
                .files_added = 10,
                .seconds_active = 3600,
                .session_count = 5,
                .uploaded_bytes = 2048,
            },
            .current_stats = tr::rpc::SessionTransferStats{
                .downloaded_bytes = 512,
                .files_added = 2,
                .seconds_active = 120,
                .session_count = 1,
                .uploaded_bytes = 256,
            },
            .download_speed = 50000,
            .paused_torrent_count = 1,
            .torrent_count = 4,
            .upload_speed = 10000,
        };
        auto const json = rfl::json::write(stats);
        auto const back = rfl::json::read<tr::rpc::SessionStatsResult>(json);
        ASSERT_TRUE(back) << back.error().what();
        EXPECT_EQ(back->active_torrent_count.value(), 3);
        EXPECT_EQ(back->cumulative_stats.value().uploaded_bytes.value(), 2048);
        EXPECT_EQ(back->current_stats.value().downloaded_bytes.value(), 512);
        EXPECT_EQ(back->torrent_count.value(), 4);
    }

    // GroupSetParams & GroupGetResult
    {
        auto const set_in = rfl::json::read<tr::rpc::GroupSetParams>(
            R"({"name":"fast","honors_session_limits":false,"speed_limit_down":500,"speed_limit_down_enabled":true})");
        ASSERT_TRUE(set_in) << set_in.error().what();
        EXPECT_EQ(set_in->name.value(), "fast");
        EXPECT_EQ(set_in->honors_session_limits.value(), false);
        EXPECT_EQ(set_in->speed_limit_down.value(), 500);
        EXPECT_EQ(set_in->speed_limit_down_enabled.value(), true);

        auto const get_out = tr::rpc::GroupGetResult{
            .group = std::vector<tr::rpc::BandwidthGroupInfo>{
                tr::rpc::BandwidthGroupInfo{
                    .honors_session_limits = false,
                    .name = "fast",
                    .speed_limit_down = 500,
                    .speed_limit_down_enabled = true,
                    .speed_limit_up = 0,
                    .speed_limit_up_enabled = false,
                },
            },
        };
        auto const back = rfl::json::read<tr::rpc::GroupGetResult>(rfl::json::write(get_out));
        ASSERT_TRUE(back) << back.error().what();
        ASSERT_EQ(back->group.value().size(), 1U);
        EXPECT_EQ(back->group.value()[0].name.value(), "fast");
        EXPECT_EQ(back->group.value()[0].speed_limit_down.value(), 500);
    }

    // SessionSetParams with Literal enums
    {
        auto const parsed = rfl::json::read<tr::rpc::SessionSetParams>(
            R"({"encryption":"required","preferred_transports":["utp","tcp"],"seed_ratio_limit":2.5})");
        ASSERT_TRUE(parsed) << parsed.error().what();
        ASSERT_TRUE(parsed->encryption.has_value());
        EXPECT_EQ(parsed->encryption->str(), "required");
        ASSERT_TRUE(parsed->preferred_transports.has_value());
        ASSERT_EQ(parsed->preferred_transports->size(), 2U);
        EXPECT_EQ((*parsed->preferred_transports)[0].str(), "utp");
        EXPECT_EQ((*parsed->preferred_transports)[1].str(), "tcp");
        EXPECT_DOUBLE_EQ(*parsed->seed_ratio_limit, 2.5);
    }
}

TEST(RpcSchemaTest, EmitsDraft202012JsonSchemaCoveringAll24Methods)
{
    auto const schema_str = rfl::json::to_schema<tr::rpc::RpcProtocolSchema>(
        YYJSON_WRITE_PRETTY_TWO_SPACES,
        "Transmission JSON-RPC 2.0 protocol schema");
    ASSERT_FALSE(schema_str.empty());

    // Must be valid JSON
    auto const parsed = rfl::json::read<rfl::Generic>(schema_str);
    ASSERT_TRUE(parsed) << parsed.error().what();
    auto const root = parsed->to_object();
    ASSERT_TRUE(root) << root.error().what();

    // Draft 2020-12 meta-schema URI
    auto const meta_schema = root->get("$schema");
    ASSERT_TRUE(meta_schema);
    EXPECT_EQ(*meta_schema->to_string(), "https://json-schema.org/draft/2020-12/schema");

    // $comment preserved
    auto const comment = root->get("$comment");
    ASSERT_TRUE(comment);
    EXPECT_EQ(*comment->to_string(), "Transmission JSON-RPC 2.0 protocol schema");

    // Root $ref points to RpcProtocolSchema inside $defs
    auto const root_ref = root->get("$ref");
    ASSERT_TRUE(root_ref);
    EXPECT_EQ(*root_ref->to_string(), "#/$defs/tr__rpc__RpcProtocolSchema");

    // $defs must contain RpcProtocolSchema, RpcEnvelopeContract, and RpcMethodsContract with all 24 RPC methods
    auto const defs = root->get("$defs");
    ASSERT_TRUE(defs);
    auto const defs_obj = defs->to_object();
    ASSERT_TRUE(defs_obj);
    EXPECT_GE(defs_obj->size(), 50U);

    auto const proto_def = defs_obj->get("tr__rpc__RpcProtocolSchema");
    ASSERT_TRUE(proto_def);
    auto const proto_props = proto_def->to_object()->get("properties")->to_object();
    ASSERT_TRUE(proto_props);
    EXPECT_TRUE(proto_props->get("envelope").has_value());
    EXPECT_TRUE(proto_props->get("methods").has_value());

    EXPECT_TRUE(defs_obj->get("tr__rpc__RpcEnvelopeContract").has_value());
    EXPECT_TRUE(defs_obj->get("tr__rpc__JsonRpcRequestEnvelope").has_value());
    EXPECT_TRUE(defs_obj->get("tr__rpc__JsonRpcSuccessResponse").has_value());
    EXPECT_TRUE(defs_obj->get("tr__rpc__JsonRpcErrorResponse").has_value());

    auto const methods_def = defs_obj->get("tr__rpc__RpcMethodsContract");
    ASSERT_TRUE(methods_def);
    auto const methods_props = methods_def->to_object()->get("properties")->to_object();
    ASSERT_TRUE(methods_props);

    static auto constexpr All24Methods = std::array<std::string_view, 24>{
        "blocklist_update"sv,
        "free_space"sv,
        "group_get"sv,
        "group_set"sv,
        "port_test"sv,
        "queue_move_bottom"sv,
        "queue_move_down"sv,
        "queue_move_top"sv,
        "queue_move_up"sv,
        "session_close"sv,
        "session_get"sv,
        "session_set"sv,
        "session_stats"sv,
        "torrent_add"sv,
        "torrent_get"sv,
        "torrent_reannounce"sv,
        "torrent_remove"sv,
        "torrent_rename_path"sv,
        "torrent_set"sv,
        "torrent_set_location"sv,
        "torrent_start"sv,
        "torrent_start_now"sv,
        "torrent_stop"sv,
        "torrent_verify"sv,
    };

    for (auto const method : All24Methods)
    {
        EXPECT_TRUE(methods_props->get(std::string{ method }).has_value()) << "Missing method in schema: " << method;
    }
    EXPECT_EQ(methods_props->size(), All24Methods.size());

    // Check schema annotations (descriptions, deprecations, jsonrpc "2.0" enum, Literal enums)
    auto const env_req_def = defs_obj->get("tr__rpc__JsonRpcRequestEnvelope");
    ASSERT_TRUE(env_req_def);
    auto const env_req_props = env_req_def->to_object()->get("properties")->to_object();
    ASSERT_TRUE(env_req_props);
    auto const jsonrpc_enum = env_req_props->get("jsonrpc")->to_object()->get("enum")->to_array();
    ASSERT_TRUE(jsonrpc_enum);
    ASSERT_EQ(jsonrpc_enum->size(), 1U);
    EXPECT_EQ(*(*jsonrpc_enum)[0].to_string(), "2.0");

    EXPECT_NE(schema_str.find(R"("deprecated": true)"), std::string::npos);
    EXPECT_NE(schema_str.find(R"("deprecationMessage": "Use tracker_list instead.")"), std::string::npos);
    EXPECT_NE(schema_str.find(R"("ipv4")"), std::string::npos);
    EXPECT_NE(schema_str.find(R"("preferred")"), std::string::npos);
    EXPECT_NE(schema_str.find(R"("utp")"), std::string::npos);
}

using RpcJsonWireTest = SessionTest;

TEST_F(RpcJsonWireTest, EndToEndJsonRpc20WireErrorsAndRoundTrips)
{
    // -32700 Parse error
    {
        auto const resp_json = execJson(session_, "{malformed");
        auto const err = rfl::json::read<tr::rpc::JsonRpcErrorResponse>(resp_json);
        ASSERT_TRUE(err) << resp_json;
        EXPECT_EQ(err->error.value().code.value(), -32700);
        EXPECT_EQ(err->error.value().message.value(), "Parse error");
        EXPECT_EQ(err->id.value(), tr::rpc::RequestId::null_id());
    }

    // -32600 Invalid Request (scalar root, wrong version, bad id type, empty batch)
    for (auto const bad_req : std::array{
             "42"sv,
             R"({"jsonrpc":"1.0","method":"session_stats","id":1})"sv,
             R"({"jsonrpc":"2.0","method":"session_stats","id":true})"sv,
             "[]"sv,
         })
    {
        auto const resp_json = execJson(session_, bad_req);
        auto const err = rfl::json::read<tr::rpc::JsonRpcErrorResponse>(resp_json);
        ASSERT_TRUE(err) << "For input " << bad_req << ", got: " << resp_json;
        EXPECT_EQ(err->error.value().code.value(), -32600);
        EXPECT_EQ(err->error.value().message.value(), "Invalid Request");
        EXPECT_EQ(err->id.value(), tr::rpc::RequestId::null_id());
    }

    // -32601 Method not found
    {
        auto const resp_json = execJson(session_, R"({"jsonrpc":"2.0","method":"no_such_method","id":"m1"})");
        auto const err = rfl::json::read<tr::rpc::JsonRpcErrorResponse>(resp_json);
        ASSERT_TRUE(err) << resp_json;
        EXPECT_EQ(err->error.value().code.value(), -32601);
        EXPECT_EQ(err->id.value(), tr::rpc::RequestId::from_string("m1"));
    }

    // -32602 Invalid params (missing required field, wrong field type, bad enum literal, non-object params)
    for (auto const bad_params_req : std::array{
             R"({"jsonrpc":"2.0","method":"free_space","params":{},"id":10})"sv,
             R"({"jsonrpc":"2.0","method":"torrent_remove","params":{"delete_local_data":"nope"},"id":11})"sv,
             R"({"jsonrpc":"2.0","method":"port_test","params":{"ip_protocol":"icmp"},"id":12})"sv,
             R"({"jsonrpc":"2.0","method":"session_set","params":{"encryption":"bogus"},"id":13})"sv,
             R"({"jsonrpc":"2.0","method":"session_stats","params":[1,2,3],"id":14})"sv,
         })
    {
        auto const resp_json = execJson(session_, bad_params_req);
        auto const err = rfl::json::read<tr::rpc::JsonRpcErrorResponse>(resp_json);
        ASSERT_TRUE(err) << "For input " << bad_params_req << ", got: " << resp_json;
        EXPECT_EQ(err->error.value().code.value(), -32602) << resp_json;
        EXPECT_EQ(err->error.value().message.value(), "Invalid params");
    }

    // Notification yields empty response string
    {
        auto const resp_json = execJson(session_, R"({"jsonrpc":"2.0","method":"session_stats"})");
        EXPECT_TRUE(resp_json.empty());
    }

    // group_set -> group_get typed round-trip over wire JSON
    {
        auto const set_resp = execJson(
            session_,
            R"({"jsonrpc":"2.0","method":"group_set","params":{"name":"ci-group","speed_limit_down":250,"speed_limit_down_enabled":true},"id":20})");
        auto const set_ok = rfl::json::read<tr::rpc::JsonRpcSuccessResponse>(set_resp);
        ASSERT_TRUE(set_ok) << set_resp;
        EXPECT_EQ(set_ok->id.value(), tr::rpc::RequestId::from_int(20));

        auto const get_resp = execJson(
            session_,
            R"({"jsonrpc":"2.0","method":"group_get","params":{"name":"ci-group"},"id":21})");
        auto const get_ok = rfl::json::read<tr::rpc::JsonRpcSuccessResponse>(get_resp);
        ASSERT_TRUE(get_ok) << get_resp;
        auto const groups = rfl::json::read<tr::rpc::GroupGetResult>(get_ok->result.value().str());
        ASSERT_TRUE(groups) << get_ok->result.value().str();
        ASSERT_EQ(groups->group.value().size(), 1U);
        EXPECT_EQ(groups->group.value()[0].name.value(), "ci-group");
        EXPECT_EQ(groups->group.value()[0].speed_limit_down.value(), 250);
        EXPECT_TRUE(groups->group.value()[0].speed_limit_down_enabled.value());
    }

    // Batch request over wire JSON (omits notification, preserves order & errors)
    {
        auto const batch_resp = execJson(
            session_,
            R"([
                {"jsonrpc":"2.0","method":"session_stats","id":1},
                {"jsonrpc":"2.0","method":"session_stats"},
                {"jsonrpc":"2.0","method":"free_space","params":{},"id":2},
                {"jsonrpc":"2.0","method":"unknown_fn","id":"three"}
            ])");
        auto const items = rfl::json::read<std::vector<rfl::json::Raw>>(batch_resp);
        ASSERT_TRUE(items) << batch_resp;
        ASSERT_EQ(items->size(), 3U);

        auto const r0 = rfl::json::read<tr::rpc::JsonRpcSuccessResponse>((*items)[0].str());
        ASSERT_TRUE(r0) << (*items)[0].str();
        EXPECT_EQ(r0->id.value(), tr::rpc::RequestId::from_int(1));

        auto const r1 = rfl::json::read<tr::rpc::JsonRpcErrorResponse>((*items)[1].str());
        ASSERT_TRUE(r1) << (*items)[1].str();
        EXPECT_EQ(r1->error.value().code.value(), -32602);
        EXPECT_EQ(r1->id.value(), tr::rpc::RequestId::from_int(2));

        auto const r2 = rfl::json::read<tr::rpc::JsonRpcErrorResponse>((*items)[2].str());
        ASSERT_TRUE(r2) << (*items)[2].str();
        EXPECT_EQ(r2->error.value().code.value(), -32601);
        EXPECT_EQ(r2->id.value(), tr::rpc::RequestId::from_string("three"));
    }
}

} // namespace tr::test

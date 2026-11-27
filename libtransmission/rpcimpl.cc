// This file Copyright © Mnemosyne LLC.
// It may be used under GPLv2 (SPDX: GPL-2.0-only), GPLv3 (SPDX: GPL-3.0-only),
// or any future license endorsed by Mnemosyne LLC.
// License text can be found in the licenses/ folder.

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <functional>
#include <iterator>
#include <memory>
#include <numeric>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <small/map.hpp>

#include <libdeflate.h>

#include <rfl.hpp>
#include <rfl/from_generic.hpp>
#include <rfl/json.hpp>
#include <rfl/to_generic.hpp>

#include "libtransmission/transmission.h"

#include "libtransmission/announcer.h"
#include "libtransmission/crypto-utils.h"
#include "libtransmission/error.h"
#include "libtransmission/file-utils.h"
#include "libtransmission/file.h"
#include "libtransmission/log.h"
#include "libtransmission/net.h"
#include "libtransmission/peer-mgr.h"
#include "libtransmission/api-compat.h"
#include "libtransmission/quark.h"
#include "libtransmission/rpc-schema.h"
#include "libtransmission/rpcimpl.h"
#include "libtransmission/session.h"
#include "libtransmission/string-utils.h"
#include "libtransmission/torrent-ctor.h"
#include "libtransmission/torrent.h"
#include "libtransmission/tr-assert.h"
#include "libtransmission/tr-strbuf.h"
#include "libtransmission/types.h"
#include "libtransmission/utils.h"
#include "libtransmission/values.h"
#include "libtransmission/variant.h"
#include "libtransmission/version.h"
#include "libtransmission/web-utils.h"
#include "libtransmission/web.h"

using namespace std::literals;
using namespace tr::Values;

namespace JsonRpc
{
// https://www.jsonrpc.org/specification#error_object
namespace Error
{
[[nodiscard]] std::string_view to_string(Code const code)
{
    switch (code)
    {
    case PARSE_ERROR:
        return "Parse error"sv;
    case INVALID_REQUEST:
        return "Invalid Request"sv;
    case METHOD_NOT_FOUND:
        return "Method not found"sv;
    case INVALID_PARAMS:
        return "Invalid params"sv;
    case INTERNAL_ERROR:
        return "Internal error"sv;
    case SUCCESS:
        return "success"sv;
    case SET_ANNOUNCE_LIST:
        return "error setting announce list"sv;
    case INVALID_TRACKER_LIST:
        return "Invalid tracker list"sv;
    case PATH_NOT_ABSOLUTE:
        return "path is not absolute"sv;
    case UNRECOGNIZED_INFO:
        return "unrecognized info"sv;
    case SYSTEM_ERROR:
        return "system error"sv;
    case FILE_IDX_OOR:
        return "file index out of range"sv;
    case PIECE_IDX_OOR:
        return "piece index out of range"sv;
    case HTTP_ERROR:
        return "HTTP error from backend service"sv;
    case CORRUPT_TORRENT:
        return "invalid or corrupt torrent file"sv;
    case INVALID_BLOCKLIST_DATA:
        return "couldn't parse any valid blocklist rules"sv;
    default:
        return {};
    }
}

namespace
{
[[nodiscard]] tr_variant::Map build_data(std::string_view error_string, tr_variant::Map&& result)
{
    auto ret = tr_variant::Map{ 2U };

    if (!std::empty(error_string))
    {
        ret.try_emplace(TR_KEY_error_string, error_string);
    }

    if (!std::empty(result))
    {
        ret.try_emplace(TR_KEY_result, std::move(result));
    }

    return ret;
}

[[nodiscard]] tr_variant::Map build(Code code, tr_variant::Map&& data)
{
    auto ret = tr_variant::Map{ 3U };
    ret.try_emplace(TR_KEY_code, code);
    ret.try_emplace(TR_KEY_message, tr_variant::unmanaged_string(to_string(code)));
    if (!std::empty(data))
    {
        ret.try_emplace(TR_KEY_data, std::move(data));
    }

    return ret;
}
} // namespace
} // namespace Error

namespace
{
[[nodiscard]] constexpr bool is_valid_id(tr_variant const& val)
{
    switch (val.index())
    {
    // https://www.jsonrpc.org/specification#request_object
    // An identifier established by the Client that MUST contain a String,
    // Number, or NULL value if included. .. The value SHOULD normally not
    // be Null and Numbers SHOULD NOT contain fractional parts
    case tr_variant::StringIndex:
    case tr_variant::StringViewIndex:
    case tr_variant::IntIndex:
    case tr_variant::DoubleIndex:
    case tr_variant::NullIndex:
        return true;

    default:
        return false;
    }
}

// https://www.jsonrpc.org/specification#response_object
[[nodiscard]] tr_variant::Map build_response(Error::Code code, tr_variant id, tr_variant::Map&& body, bool is_jsonrpc = true)
{
    if (is_jsonrpc && !is_valid_id(id))
    {
        TR_ASSERT(false);
        id = nullptr;
    }

    auto ret = tr_variant::Map{ 3U };
    ret.try_emplace(TR_KEY_jsonrpc, tr_variant::unmanaged_string(Version));
    if (code == Error::SUCCESS)
    {
        ret.try_emplace(TR_KEY_result, std::move(body));
    }
    else
    {
        ret.try_emplace(TR_KEY_error, Error::build(code, std::move(body)));
    }
    ret.try_emplace(TR_KEY_id, std::move(id));

    return ret;
}
} // namespace
} // namespace JsonRpc

namespace
{
auto constexpr RecentlyActiveSeconds = time_t{ 60 };
auto constexpr RpcVersion = int64_t{ 18 }; // TODO: 18 == 6.0.0, bump after all 6.0.x releases and before releasing 6.1.0
auto constexpr RpcVersionMin = int64_t{ 14 };

enum class TrFormat : uint8_t
{
    Object,
    Table
};

// ---

[[nodiscard]] std::string build_json_error_response(
    JsonRpc::Error::Code code,
    tr::rpc::RequestId const& id,
    std::string_view error_string,
    std::optional<rfl::json::Raw> partial_result = std::nullopt);

[[nodiscard]] std::string build_json_success_response(tr::rpc::RequestId const& id, rfl::json::Raw result_raw);

/* For functions that can't be immediately executed, like torrentAdd,
 * this is the callback data used to pass a response to the caller
 * when the task is complete */
struct tr_rpc_idle_data
{
    tr_variant id;
    tr::rpc::RequestId rpc_id = tr::rpc::RequestId::omitted();
    tr_session* session = nullptr;
    tr_variant::Map args_out;
    tr_rpc_response_func callback;
    tr_rpc_json_response_func json_callback;

    // TODO: api-compat
    bool is_jsonrpc = false;
};

void tr_rpc_idle_done(struct tr_rpc_idle_data* data, JsonRpc::Error::Code code, std::string_view errmsg)
{
    using namespace JsonRpc;

    if (data->json_callback)
    {
        if (data->is_jsonrpc)
        {
            if (data->rpc_id.is_notification())
            {
                data->json_callback(std::string{});
            }
            else if (code == Error::SUCCESS)
            {
                auto result_json = tr_variant_serde::json().compact().to_string(tr_variant{ std::move(data->args_out) });
                data->json_callback(build_json_success_response(data->rpc_id, rfl::json::Raw{ std::move(result_json) }));
            }
            else
            {
                auto partial = std::optional<rfl::json::Raw>{};
                if (!std::empty(data->args_out))
                {
                    partial = rfl::json::Raw{ tr_variant_serde::json().compact().to_string(
                        tr_variant{ std::move(data->args_out) }) };
                }
                data->json_callback(build_json_error_response(code, data->rpc_id, errmsg, std::move(partial)));
            }
        }
        else
        {
            auto const is_success = code == Error::SUCCESS;
            auto response = tr_variant{ build_response(
                code,
                std::move(data->id),
                is_success ? std::move(data->args_out) : Error::build_data(errmsg, std::move(data->args_out)),
                false) };
            tr::api_compat::convert(response, tr::api_compat::Style::Tr4);
            data->json_callback(tr_variant_serde::json().compact().to_string(response));
        }
    }
    else if (!data->is_jsonrpc || data->id.has_value())
    {
        auto const is_success = code == Error::SUCCESS;
        auto response = tr_variant{ build_response(
            code,
            std::move(data->id),
            is_success ? std::move(data->args_out) : Error::build_data(errmsg, std::move(data->args_out)),
            data->is_jsonrpc) };
        if (!data->is_jsonrpc)
        {
            tr::api_compat::convert(response, tr::api_compat::Style::Tr4);
        }
        data->callback(std::move(response));
    }
    else // notification
    {
        data->callback({});
    }

    delete data;
}

// --- reflect-cpp <-> tr_variant interop helpers ---

[[nodiscard]] rfl::Generic tr_variant_to_rfl_generic(tr_variant const& var)
{
    switch (var.index())
    {
    case tr_variant::NullIndex:
    case tr_variant::NoneIndex:
        return rfl::Generic{};

    case tr_variant::BoolIndex:
        return rfl::Generic{ *var.get_if<tr_variant::BoolIndex>() };

    case tr_variant::IntIndex:
        return rfl::Generic{ *var.get_if<tr_variant::IntIndex>() };

    case tr_variant::DoubleIndex:
        return rfl::Generic{ *var.get_if<tr_variant::DoubleIndex>() };

    case tr_variant::StringIndex:
    case tr_variant::StringViewIndex:
        return rfl::Generic{ std::string{ *var.value_if<std::string_view>() } };

    case tr_variant::VectorIndex:
        {
            auto const& src = *var.get_if<tr_variant::Vector>();
            auto arr = rfl::Generic::Array{};
            arr.reserve(std::size(src));
            for (auto const& elem : src)
            {
                arr.emplace_back(tr_variant_to_rfl_generic(elem));
            }
            return rfl::Generic{ std::move(arr) };
        }

    case tr_variant::MapIndex:
        {
            auto const& src = *var.get_if<tr_variant::Map>();
            auto obj = rfl::Generic::Object{};
            for (auto const& [key, val] : src)
            {
                obj.insert(std::string{ tr_quark_get_string_view(key) }, tr_variant_to_rfl_generic(val));
            }
            return rfl::Generic{ std::move(obj) };
        }

    default:
        return rfl::Generic{};
    }
}

[[nodiscard]] rfl::Generic tr_variant_map_to_rfl_generic(tr_variant::Map const& map)
{
    auto obj = rfl::Generic::Object{};
    for (auto const& [key, val] : map)
    {
        obj.insert(std::string{ tr_quark_get_string_view(key) }, tr_variant_to_rfl_generic(val));
    }
    return rfl::Generic{ std::move(obj) };
}

[[nodiscard]] tr_variant rfl_generic_to_tr_variant(rfl::Generic const& g)
{
    return std::visit(
        [](auto const& val) -> tr_variant
        {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, std::nullopt_t>)
            {
                return nullptr;
            }
            else if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, int64_t> || std::is_same_v<T, double>)
            {
                return val;
            }
            else if constexpr (std::is_same_v<T, std::string>)
            {
                return tr_variant{ val };
            }
            else if constexpr (std::is_same_v<T, rfl::Generic::Array>)
            {
                auto vec = tr_variant::Vector{};
                vec.reserve(std::size(val));
                for (auto const& elem : val)
                {
                    vec.emplace_back(rfl_generic_to_tr_variant(elem));
                }
                return tr_variant{ std::move(vec) };
            }
            else if constexpr (std::is_same_v<T, rfl::Generic::Object>)
            {
                auto map = tr_variant::Map{ std::size(val) };
                for (auto const& [k, v] : val)
                {
                    map.try_emplace(tr_quark_new(k), rfl_generic_to_tr_variant(v));
                }
                return tr_variant{ std::move(map) };
            }
        },
        g.variant());
}

template<typename T>
[[nodiscard]] rfl::Result<T> parse_rpc_params(tr_variant::Map const& args_in)
{
    return rfl::from_generic<T>(tr_variant_map_to_rfl_generic(args_in));
}

template<typename T>
void write_rpc_result(T const& res, tr_variant::Map& args_out)
{
    auto g = rfl::to_generic(res);
    if (auto const* obj = std::get_if<rfl::Generic::Object>(&g.variant()); obj != nullptr)
    {
        args_out.reserve(std::size(args_out) + std::size(*obj));
        for (auto const& [k, v] : *obj)
        {
            args_out.try_emplace(tr_quark_new(k), rfl_generic_to_tr_variant(v));
        }
    }
}

template<typename T>
[[nodiscard]] tr_variant to_tr_variant(T const& value)
{
    return rfl_generic_to_tr_variant(rfl::to_generic(value));
}

[[nodiscard]] tr_variant request_id_to_tr_variant(tr::rpc::RequestId const& id)
{
    if (!id.present)
    {
        return {};
    }

    return std::visit(
        [](auto const& v) -> tr_variant
        {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::nullptr_t>)
            {
                return nullptr;
            }
            else
            {
                return tr_variant{ v };
            }
        },
        id.value);
}

[[nodiscard]] std::string build_json_error_response(
    JsonRpc::Error::Code const code,
    tr::rpc::RequestId const& id,
    std::string_view const error_string,
    std::optional<rfl::json::Raw> partial_result)
{
    auto err_obj = tr::rpc::JsonRpcErrorObject{
        .code = static_cast<int16_t>(code),
        .message = std::string{ JsonRpc::Error::to_string(code) },
    };

    if (!std::empty(error_string) || partial_result.has_value())
    {
        auto data = tr::rpc::JsonRpcErrorData{};
        if (!std::empty(error_string))
        {
            data.error_string = std::string{ error_string };
        }
        if (partial_result.has_value())
        {
            data.result = std::move(partial_result);
        }
        err_obj.data = std::move(data);
    }

    auto const resp = tr::rpc::JsonRpcErrorResponse{
        .error = std::move(err_obj),
        .id = id.present ? id : tr::rpc::RequestId::null_id(),
    };
    return rfl::json::write(resp);
}

[[nodiscard]] std::string build_json_success_response(tr::rpc::RequestId const& id, rfl::json::Raw result_raw)
{
    auto const resp = tr::rpc::JsonRpcSuccessResponse{
        .result = std::move(result_raw),
        .id = id.present ? id : tr::rpc::RequestId::null_id(),
    };
    return rfl::json::write(resp);
}

// ---

[[nodiscard]] auto getTorrents(tr_session* session, std::optional<tr::rpc::TorrentIds> const& ids)
{
    auto torrents_vec = std::vector<tr_torrent*>{};
    auto& torrents = session->torrents();

    if (!ids.has_value())
    {
        return torrents.get_all();
    }

    torrents_vec.reserve(std::size(torrents));
    auto const add_id = [&torrents, &torrents_vec](int64_t const val)
    {
        if (auto* tor = torrents.get(static_cast<tr_torrent_id_t>(val)); tor != nullptr)
        {
            torrents_vec.push_back(tor);
        }
    };
    auto const add_str = [&torrents, &torrents_vec, session](std::string_view const val)
    {
        if (val == tr_quark_get_string_view(TR_KEY_recently_active))
        {
            auto const cutoff = std::max(tr_time() - RecentlyActiveSeconds, session->torrentsLoadedTime());
            auto const recent = torrents.get_matching([cutoff](auto* walk) { return walk->has_changed_since(cutoff); });
            std::ranges::copy(recent, std::back_inserter(torrents_vec));
        }
        else if (auto* tor = torrents.get(val); tor != nullptr)
        {
            torrents_vec.push_back(tor);
        }
    };

    std::visit(
        [&](auto const& selector)
        {
            using T = std::decay_t<decltype(selector)>;
            if constexpr (std::is_same_v<T, int64_t>)
            {
                add_id(selector);
            }
            else if constexpr (std::is_same_v<T, std::string>)
            {
                add_str(selector);
            }
            else if constexpr (std::is_same_v<T, std::vector<tr::rpc::TorrentIdElement>>)
            {
                for (auto const& elem : selector)
                {
                    std::visit(
                        [&](auto const& item)
                        {
                            using E = std::decay_t<decltype(item)>;
                            if constexpr (std::is_same_v<E, int64_t>)
                            {
                                add_id(item);
                            }
                            else
                            {
                                add_str(item);
                            }
                        },
                        elem);
                }
            }
        },
        *ids);

    return torrents_vec;
}

void notifyBatchQueueChange(tr_session* session, std::vector<tr_torrent*> const& torrents)
{
    for (auto* tor : torrents)
    {
        session->rpcNotify(TR_RPC_TORRENT_CHANGED, tor->id());
    }

    session->rpcNotify(TR_RPC_SESSION_QUEUE_POSITIONS_CHANGED);
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> queueMoveTop(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    auto const torrents = getTorrents(session, params->ids.value());
    tr_torrent::queue_move_top(torrents);
    notifyBatchQueueChange(session, torrents);
    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> queueMoveUp(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    auto const torrents = getTorrents(session, params->ids.value());
    tr_torrent::queue_move_up(torrents);
    notifyBatchQueueChange(session, torrents);
    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> queueMoveDown(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    auto const torrents = getTorrents(session, params->ids.value());
    tr_torrent::queue_move_down(torrents);
    notifyBatchQueueChange(session, torrents);
    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> queueMoveBottom(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    auto const torrents = getTorrents(session, params->ids.value());
    tr_torrent::queue_move_bottom(torrents);
    notifyBatchQueueChange(session, torrents);
    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentStart(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    auto torrents = getTorrents(session, params->ids.value());
    std::ranges::sort(torrents, tr_torrent::CompareQueuePosition);
    for (auto* tor : torrents)
    {
        if (!tor->is_running())
        {
            tr_torrentStart(tor);
            session->rpcNotify(TR_RPC_TORRENT_STARTED, tor->id());
        }
    }

    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentStartNow(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    auto torrents = getTorrents(session, params->ids.value());
    std::ranges::sort(torrents, tr_torrent::CompareQueuePosition);
    for (auto* tor : torrents)
    {
        if (!tor->is_running())
        {
            tr_torrentStartNow(tor);
            session->rpcNotify(TR_RPC_TORRENT_STARTED, tor->id());
        }
    }

    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentStop(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    for (auto* tor : getTorrents(session, params->ids.value()))
    {
        if (tor->activity() != TR_STATUS_STOPPED)
        {
            tor->stop_soon();
            session->rpcNotify(TR_RPC_TORRENT_STOPPED, tor->id());
        }
    }

    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentRemove(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentRemoveParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }

    auto const delete_flag = params->delete_local_data.value().value_or(false);
    auto const type = delete_flag ? TR_RPC_TORRENT_TRASHING : TR_RPC_TORRENT_REMOVING;

    for (auto* tor : getTorrents(session, params->ids.value()))
    {
        if (auto const status = session->rpcNotify(type, tor->id()); (status & TR_RPC_NOREMOVE) == 0)
        {
            tr_torrentRemove(tor, delete_flag);
        }
    }

    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentReannounce(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    for (auto* tor : getTorrents(session, params->ids.value()))
    {
        if (tr_torrentCanManualUpdate(tor))
        {
            tr_torrentManualUpdate(tor);
            session->rpcNotify(TR_RPC_TORRENT_CHANGED, tor->id());
        }
    }

    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentVerify(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    auto const params = parse_rpc_params<tr::rpc::TorrentActionParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }
    for (auto* tor : getTorrents(session, params->ids.value()))
    {
        tr_torrentVerify(tor);
        session->rpcNotify(TR_RPC_TORRENT_CHANGED, tor->id());
    }

    return { JsonRpc::Error::SUCCESS, std::string{} };
}

// ---

namespace make_torrent_field_helpers
{
[[nodiscard]] auto make_file_wanted_vec(tr_torrent const& tor)
{
    auto const n_files = tor.file_count();
    auto vec = tr_variant::Vector{};
    vec.reserve(n_files);
    for (tr_file_index_t idx = 0U; idx != n_files; ++idx)
    {
        vec.emplace_back(tr_torrentFile(&tor, idx).wanted);
    }
    return tr_variant{ std::move(vec) };
}

[[nodiscard]] auto make_labels_vec(tr_torrent const& tor)
{
    auto const& labels = tor.labels();
    auto const n_labels = std::size(labels);
    auto vec = tr_variant::Vector{};
    vec.reserve(n_labels);
    for (auto const& label : labels)
    {
        vec.emplace_back(tr_variant::unmanaged_string(label.sv()));
    }
    return tr_variant{ std::move(vec) };
}

[[nodiscard]] auto make_file_priorities_vec(tr_torrent const& tor)
{
    auto const n_files = tor.file_count();
    auto vec = tr_variant::Vector{};
    vec.reserve(n_files);
    for (tr_file_index_t idx = 0U; idx != n_files; ++idx)
    {
        vec.emplace_back(tr_torrentFile(&tor, idx).priority);
    }
    return tr_variant{ std::move(vec) };
}

[[nodiscard]] auto make_file_stats_vec(tr_torrent const& tor)
{
    auto const n_files = tor.file_count();
    auto stats = std::vector<tr::rpc::TorrentFileStats>{};
    stats.reserve(n_files);
    for (tr_file_index_t idx = 0U; idx != n_files; ++idx)
    {
        auto const file = tr_torrentFile(&tor, idx);
        stats.push_back(
            tr::rpc::TorrentFileStats{
                .bytes_completed = static_cast<int64_t>(file.have),
                .priority = static_cast<int64_t>(file.priority),
                .wanted = file.wanted,
            });
    }
    return to_tr_variant(stats);
}

[[nodiscard]] auto make_bytes_completed_vec(tr_torrent const& tor)
{
    auto const n_files = tor.file_count();
    auto vec = tr_variant::Vector{};
    vec.reserve(n_files);
    tr_logAddDebug("Running bytes completed");
    for (tr_file_index_t idx = 0U; idx != n_files; ++idx)
    {
        vec.emplace_back(tr_torrentFile(&tor, idx).have);
    }
    return tr_variant{ std::move(vec) };
}

[[nodiscard]] auto make_file_vec(tr_torrent const& tor)
{
    auto const n_files = tor.file_count();
    auto files = std::vector<tr::rpc::TorrentFileInfo>{};
    files.reserve(n_files);
    for (tr_file_index_t idx = 0U; idx != n_files; ++idx)
    {
        auto const file = tr_torrentFile(&tor, idx);
        files.push_back(
            tr::rpc::TorrentFileInfo{
                .begin_piece = static_cast<int64_t>(file.beginPiece),
                .bytes_completed = static_cast<int64_t>(file.have),
                .end_piece = static_cast<int64_t>(file.endPiece),
                .length = static_cast<int64_t>(file.length),
                .name = std::string{ file.name },
            });
    }
    return to_tr_variant(files);
}

[[nodiscard]] auto make_webseed_vec(tr_torrent const& tor)
{
    auto const n_webseeds = tor.webseed_count();
    auto vec = tr_variant::Vector{};
    for (size_t idx = 0U; idx != n_webseeds; ++idx)
    {
        vec.emplace_back(tor.webseed(idx));
    }
    return tr_variant{ std::move(vec) };
}

[[nodiscard]] auto make_webseed_ex_vec(tr_torrent const& tor)
{
    auto const n_webseeds = tor.webseed_count();
    auto webseeds = std::vector<tr::rpc::TorrentWebseedEx>{};
    webseeds.reserve(n_webseeds);
    for (size_t idx = 0U; idx != n_webseeds; ++idx)
    {
        auto const webseed = tr_torrentWebseed(&tor, idx);
        webseeds.push_back(
            tr::rpc::TorrentWebseedEx{
                .url = std::string{ webseed.url },
                .is_downloading = webseed.is_downloading,
                .download_bytes_per_second = static_cast<int64_t>(webseed.download_bytes_per_second),
            });
    }
    return to_tr_variant(webseeds);
}

[[nodiscard]] auto make_tracker_vec(tr_torrent const& tor)
{
    auto const& trackers = tor.announce_list();
    auto vec = std::vector<tr::rpc::TorrentTrackerInfo>{};
    vec.reserve(std::size(trackers));
    for (auto const& tracker : trackers)
    {
        vec.push_back(
            tr::rpc::TorrentTrackerInfo{
                .announce = std::string{ tracker.announce.sv() },
                .id = static_cast<int64_t>(tracker.id),
                .scrape = std::string{ tracker.scrape.sv() },
                .sitename = std::string{ tracker.announce_parsed.sitename },
                .tier = static_cast<int64_t>(tracker.tier),
            });
    }
    return to_tr_variant(vec);
}

[[nodiscard]] auto make_tracker_stats_vec(tr_torrent const& tor)
{
    auto const n_trackers = tr_torrentTrackerCount(&tor);
    auto vec = tr_variant::Vector{};
    vec.reserve(n_trackers);
    for (size_t idx = 0U; idx != n_trackers; ++idx)
    {
        auto const tracker = tr_torrentTracker(&tor, idx);
        auto stats_map = tr_variant::Map{ 28U };
        stats_map.try_emplace(TR_KEY_announce, tracker.announce);
        stats_map.try_emplace(TR_KEY_announce_state, tracker.announceState);
        stats_map.try_emplace(TR_KEY_download_count, tracker.downloadCount);
        stats_map.try_emplace(TR_KEY_downloader_count, tracker.downloader_count);
        stats_map.try_emplace(TR_KEY_has_announced, tracker.hasAnnounced);
        stats_map.try_emplace(TR_KEY_has_scraped, tracker.hasScraped);
        stats_map.try_emplace(TR_KEY_host, tracker.host_and_port);
        stats_map.try_emplace(TR_KEY_id, tracker.id);
        stats_map.try_emplace(TR_KEY_is_backup, tracker.isBackup);
        stats_map.try_emplace(TR_KEY_last_announce_peer_count, tracker.lastAnnouncePeerCount);
        stats_map.try_emplace(TR_KEY_last_announce_result, tracker.lastAnnounceResult);
        stats_map.try_emplace(TR_KEY_last_announce_start_time, tracker.lastAnnounceStartTime);
        stats_map.try_emplace(TR_KEY_last_announce_succeeded, tracker.lastAnnounceSucceeded);
        stats_map.try_emplace(TR_KEY_last_announce_time, tracker.lastAnnounceTime);
        stats_map.try_emplace(TR_KEY_last_announce_timed_out, tracker.lastAnnounceTimedOut);
        stats_map.try_emplace(TR_KEY_last_scrape_result, tracker.lastScrapeResult);
        stats_map.try_emplace(TR_KEY_last_scrape_start_time, tracker.lastScrapeStartTime);
        stats_map.try_emplace(TR_KEY_last_scrape_succeeded, tracker.lastScrapeSucceeded);
        stats_map.try_emplace(TR_KEY_last_scrape_time, tracker.lastScrapeTime);
        stats_map.try_emplace(TR_KEY_last_scrape_timed_out, tracker.lastScrapeTimedOut);
        stats_map.try_emplace(TR_KEY_leecher_count, tracker.leecherCount);
        stats_map.try_emplace(TR_KEY_next_announce_time, tracker.nextAnnounceTime);
        stats_map.try_emplace(TR_KEY_next_scrape_time, tracker.nextScrapeTime);
        stats_map.try_emplace(TR_KEY_scrape, tracker.scrape);
        stats_map.try_emplace(TR_KEY_scrape_state, tracker.scrapeState);
        stats_map.try_emplace(TR_KEY_seeder_count, tracker.seederCount);
        stats_map.try_emplace(TR_KEY_sitename, tracker.sitename);
        stats_map.try_emplace(TR_KEY_tier, tracker.tier);
        vec.emplace_back(std::move(stats_map));
    }
    return tr_variant{ std::move(vec) };
}

[[nodiscard]] auto make_peer_vec(tr_torrent const& tor)
{
    auto const peers = tr_torrentPeers(&tor);
    auto peers_vec = tr_variant::Vector{};
    peers_vec.reserve(std::size(peers));
    for (auto const& peer : peers)
    {
        auto peer_map = tr_variant::Map{ 20U };
        peer_map.try_emplace(TR_KEY_address, peer.addr);
        peer_map.try_emplace(TR_KEY_client_is_choked, peer.client_is_choked);
        peer_map.try_emplace(TR_KEY_client_is_interested, peer.client_is_interested);
        peer_map.try_emplace(TR_KEY_client_name, peer.user_agent);
        peer_map.try_emplace(TR_KEY_peer_id, tr_base64_encode(std::string_view{ peer.peer_id.data(), peer.peer_id.size() }));
        peer_map.try_emplace(TR_KEY_flag_str, peer.flag_str);
        peer_map.try_emplace(TR_KEY_is_downloading_from, peer.is_downloading_from);
        peer_map.try_emplace(TR_KEY_is_encrypted, peer.is_encrypted);
        peer_map.try_emplace(TR_KEY_is_incoming, peer.is_incoming);
        peer_map.try_emplace(TR_KEY_is_utp, peer.is_utp);
        peer_map.try_emplace(TR_KEY_is_uploading_to, peer.is_uploading_to);
        peer_map.try_emplace(TR_KEY_supports_holepunch, peer.supports_holepunch);
        peer_map.try_emplace(TR_KEY_peer_is_choked, peer.peer_is_choked);
        peer_map.try_emplace(TR_KEY_peer_is_interested, peer.peer_is_interested);
        peer_map.try_emplace(TR_KEY_port, peer.port);
        peer_map.try_emplace(TR_KEY_progress, peer.progress);
        peer_map.try_emplace(TR_KEY_rate_to_client, peer.rate_to_client.base_quantity());
        peer_map.try_emplace(TR_KEY_rate_to_peer, peer.rate_to_peer.base_quantity());
        peer_map.try_emplace(TR_KEY_bytes_to_peer, peer.bytes_to_peer);
        peer_map.try_emplace(TR_KEY_bytes_to_client, peer.bytes_to_client);
        peers_vec.emplace_back(std::move(peer_map));
    }
    return tr_variant{ std::move(peers_vec) };
}

[[nodiscard]] auto make_peer_counts_map(tr_stat const& st)
{
    auto const& from = st.peers_from;
    return to_tr_variant(
        tr::rpc::TorrentPeersFrom{
            .from_cache = static_cast<int64_t>(from[TR_PEER_FROM_RESUME]),
            .from_dht = static_cast<int64_t>(from[TR_PEER_FROM_DHT]),
            .from_holepunch = static_cast<int64_t>(from[TR_PEER_FROM_HOLEPUNCH]),
            .from_incoming = static_cast<int64_t>(from[TR_PEER_FROM_INCOMING]),
            .from_lpd = static_cast<int64_t>(from[TR_PEER_FROM_LPD]),
            .from_ltep = static_cast<int64_t>(from[TR_PEER_FROM_LTEP]),
            .from_pex = static_cast<int64_t>(from[TR_PEER_FROM_PEX]),
            .from_tracker = static_cast<int64_t>(from[TR_PEER_FROM_TRACKER]),
        });
}

[[nodiscard]] auto make_piece_availability_vec(tr_torrent const& tor)
{
    auto const n_pieces = tor.piece_count();
    auto vec = tr_variant::Vector{};
    vec.reserve(n_pieces);
    for (tr_piece_index_t idx = 0U; idx != n_pieces; ++idx)
    {
        vec.emplace_back(tr_peerMgrPieceAvailability(&tor, idx));
    }
    return tr_variant{ std::move(vec) };
}

[[nodiscard]] auto make_piece_bitfield(tr_torrent const& tor)
{
    if (tor.has_metainfo())
    {
        auto const bytes = tor.create_piece_bitfield();
        return tr_variant{ tr_base64_encode({ reinterpret_cast<char const*>(std::data(bytes)), std::size(bytes) }) };
    }

    return tr_variant::unmanaged_string(""sv);
}
} // namespace make_torrent_field_helpers

[[nodiscard]] auto constexpr isSupportedTorrentGetField(tr_quark key)
{
    switch (key)
    {
    case TR_KEY_activity_date:
    case TR_KEY_added_date:
    case TR_KEY_availability:
    case TR_KEY_bandwidth_priority:
    case TR_KEY_bytes_completed:
    case TR_KEY_comment:
    case TR_KEY_corrupt_ever:
    case TR_KEY_creator:
    case TR_KEY_date_created:
    case TR_KEY_desired_available:
    case TR_KEY_done_date:
    case TR_KEY_download_dir:
    case TR_KEY_download_limit:
    case TR_KEY_download_limited:
    case TR_KEY_downloaded_ever:
    case TR_KEY_edit_date:
    case TR_KEY_error:
    case TR_KEY_error_string:
    case TR_KEY_eta:
    case TR_KEY_eta_idle:
    case TR_KEY_file_count:
    case TR_KEY_file_stats:
    case TR_KEY_files:
    case TR_KEY_group:
    case TR_KEY_hash_string:
    case TR_KEY_have_unchecked:
    case TR_KEY_have_valid:
    case TR_KEY_honors_session_limits:
    case TR_KEY_id:
    case TR_KEY_is_finished:
    case TR_KEY_is_private:
    case TR_KEY_is_stalled:
    case TR_KEY_labels:
    case TR_KEY_left_until_done:
    case TR_KEY_magnet_link:
    case TR_KEY_manual_announce_time:
    case TR_KEY_max_connected_peers:
    case TR_KEY_metadata_percent_complete:
    case TR_KEY_name:
    case TR_KEY_peer_limit:
    case TR_KEY_peers:
    case TR_KEY_peers_connected:
    case TR_KEY_peers_from:
    case TR_KEY_peers_getting_from_us:
    case TR_KEY_peers_sending_to_us:
    case TR_KEY_percent_complete:
    case TR_KEY_percent_done:
    case TR_KEY_piece_count:
    case TR_KEY_piece_size:
    case TR_KEY_pieces:
    case TR_KEY_primary_mime_type:
    case TR_KEY_priorities:
    case TR_KEY_queue_position:
    case TR_KEY_rate_download:
    case TR_KEY_rate_upload:
    case TR_KEY_recheck_progress:
    case TR_KEY_seconds_downloading:
    case TR_KEY_seconds_seeding:
    case TR_KEY_seed_idle_limit:
    case TR_KEY_seed_idle_mode:
    case TR_KEY_seed_ratio_limit:
    case TR_KEY_seed_ratio_mode:
    case TR_KEY_sequential_download:
    case TR_KEY_sequential_download_from_piece:
    case TR_KEY_size_when_done:
    case TR_KEY_source:
    case TR_KEY_start_date:
    case TR_KEY_status:
    case TR_KEY_torrent_file:
    case TR_KEY_total_size:
    case TR_KEY_tracker_list:
    case TR_KEY_tracker_stats:
    case TR_KEY_trackers:
    case TR_KEY_upload_limit:
    case TR_KEY_upload_limited:
    case TR_KEY_upload_ratio:
    case TR_KEY_uploaded_ever:
    case TR_KEY_wanted:
    case TR_KEY_webseeds:
    case TR_KEY_webseeds_ex:
    case TR_KEY_webseeds_sending_to_us:
        return true;

    default:
        return false;
    }
}

[[nodiscard]] tr_variant make_torrent_field(tr_torrent const& tor, tr_stat const& st, tr_quark key)
{
    using namespace make_torrent_field_helpers;

    TR_ASSERT(isSupportedTorrentGetField(key));

    switch (key)
    {
    case TR_KEY_activity_date:
        return st.activity_date;
    case TR_KEY_added_date:
        return st.added_date;
    case TR_KEY_availability:
        return make_piece_availability_vec(tor);
    case TR_KEY_bandwidth_priority:
        return tor.get_priority();
    case TR_KEY_bytes_completed:
        return make_bytes_completed_vec(tor);
    case TR_KEY_comment:
        return tor.comment();
    case TR_KEY_corrupt_ever:
        return st.corrupt_ever;
    case TR_KEY_creator:
        return tor.creator();
    case TR_KEY_date_created:
        return tor.date_created();
    case TR_KEY_desired_available:
        return st.desired_available;
    case TR_KEY_done_date:
        return st.done_date;
    case TR_KEY_download_dir:
        return tr_variant::unmanaged_string(tor.download_dir().sv());
    case TR_KEY_download_limit:
        return tr_torrentGetSpeedLimit_KBps(&tor, tr_direction::Down);
    case TR_KEY_download_limited:
        return tor.uses_speed_limit(tr_direction::Down);
    case TR_KEY_downloaded_ever:
        return st.downloaded_ever;
    case TR_KEY_edit_date:
        return st.edit_date;
    case TR_KEY_error:
        return st.error;
    case TR_KEY_error_string:
        return st.error_string;
    case TR_KEY_eta:
        return st.eta;
    case TR_KEY_eta_idle:
        return st.eta_idle;
    case TR_KEY_file_count:
        return tor.file_count();
    case TR_KEY_file_stats:
        return make_file_stats_vec(tor);
    case TR_KEY_files:
        return make_file_vec(tor);
    case TR_KEY_group:
        return tr_variant::unmanaged_string(tor.bandwidth_group().sv());
    case TR_KEY_hash_string:
        return tr_variant::unmanaged_string(tor.info_hash_string().sv());
    case TR_KEY_have_unchecked:
        return st.have_unchecked;
    case TR_KEY_have_valid:
        return st.have_valid;
    case TR_KEY_honors_session_limits:
        return tor.uses_session_limits();
    case TR_KEY_id:
        return st.id;
    case TR_KEY_is_finished:
        return st.finished;
    case TR_KEY_is_private:
        return tor.is_private();
    case TR_KEY_is_stalled:
        return st.is_stalled;
    case TR_KEY_labels:
        return make_labels_vec(tor);
    case TR_KEY_left_until_done:
        return st.left_until_done;
    case TR_KEY_magnet_link:
        return tor.magnet();
    case TR_KEY_manual_announce_time:
        return tr_announcerNextManualAnnounce(&tor);
    case TR_KEY_max_connected_peers:
        return tor.peer_limit();
    case TR_KEY_metadata_percent_complete:
        return st.metadata_percent_complete;
    case TR_KEY_name:
        return tor.name();
    case TR_KEY_peer_limit:
        return tor.peer_limit();
    case TR_KEY_peers:
        return make_peer_vec(tor);
    case TR_KEY_peers_connected:
        return st.peers_connected;
    case TR_KEY_peers_from:
        return make_peer_counts_map(st);
    case TR_KEY_peers_getting_from_us:
        return st.peers_getting_from_us;
    case TR_KEY_peers_sending_to_us:
        return st.peers_sending_to_us;
    case TR_KEY_percent_complete:
        return st.percent_complete;
    case TR_KEY_percent_done:
        return st.percent_done;
    case TR_KEY_piece_count:
        return tor.piece_count();
    case TR_KEY_piece_size:
        return tor.piece_size();
    case TR_KEY_pieces:
        return make_piece_bitfield(tor);
    case TR_KEY_primary_mime_type:
        return tr_variant::unmanaged_string(tor.primary_mime_type());
    case TR_KEY_priorities:
        return make_file_priorities_vec(tor);
    case TR_KEY_queue_position:
        return st.queue_position;
    case TR_KEY_rate_download:
        return st.piece_download_speed.base_quantity();
    case TR_KEY_rate_upload:
        return st.piece_upload_speed.base_quantity();
    case TR_KEY_recheck_progress:
        return st.recheck_progress;
    case TR_KEY_seconds_downloading:
        return st.seconds_downloading;
    case TR_KEY_seconds_seeding:
        return st.seconds_seeding;
    case TR_KEY_seed_idle_limit:
        return tor.idle_limit_minutes();
    case TR_KEY_seed_idle_mode:
        return tor.idle_limit_mode();
    case TR_KEY_seed_ratio_limit:
        return tor.seed_ratio();
    case TR_KEY_seed_ratio_mode:
        return tor.seed_ratio_mode();
    case TR_KEY_sequential_download:
        return tor.is_sequential_download();
    case TR_KEY_sequential_download_from_piece:
        return tor.sequential_download_from_piece();
    case TR_KEY_size_when_done:
        return st.size_when_done;
    case TR_KEY_source:
        return tor.source();
    case TR_KEY_start_date:
        return st.start_date;
    case TR_KEY_status:
        return st.activity;
    case TR_KEY_torrent_file:
        return tor.torrent_file();
    case TR_KEY_total_size:
        return tor.total_size();
    case TR_KEY_tracker_list:
        return tor.announce_list().to_string();
    case TR_KEY_tracker_stats:
        return make_tracker_stats_vec(tor);
    case TR_KEY_trackers:
        return make_tracker_vec(tor);
    case TR_KEY_upload_limit:
        return tr_torrentGetSpeedLimit_KBps(&tor, tr_direction::Up);
    case TR_KEY_upload_limited:
        return tor.uses_speed_limit(tr_direction::Up);
    case TR_KEY_upload_ratio:
        return st.upload_ratio;
    case TR_KEY_uploaded_ever:
        return st.uploaded_ever;
    case TR_KEY_wanted:
        return make_file_wanted_vec(tor);
    case TR_KEY_webseeds:
        return make_webseed_vec(tor);
    case TR_KEY_webseeds_ex:
        return make_webseed_ex_vec(tor);
    case TR_KEY_webseeds_sending_to_us:
        return st.webseeds_sending_to_us;
    default:
        return tr_variant{};
    }
}

[[nodiscard]] auto make_torrent_info_map(tr_torrent* const tor, tr_quark const* const fields, size_t const field_count)
{
    auto const st = tr_torrentStat(tor);
    auto info_map = tr_variant::Map{ field_count };
    for (size_t i = 0; i < field_count; ++i)
    {
        info_map.try_emplace(fields[i], make_torrent_field(*tor, st, fields[i]));
    }
    return tr_variant{ std::move(info_map) };
}

[[nodiscard]] auto make_torrent_info_vec(tr_torrent* const tor, tr_quark const* const fields, size_t const field_count)
{
    auto const st = tr_torrentStat(tor);
    auto info_vec = tr_variant::Vector{};
    info_vec.reserve(field_count);
    for (size_t i = 0; i < field_count; ++i)
    {
        info_vec.emplace_back(make_torrent_field(*tor, st, fields[i]));
    }
    return tr_variant{ std::move(info_vec) };
}

[[nodiscard]] auto make_torrent_info(
    tr_torrent* const tor,
    TrFormat const format,
    tr_quark const* const fields,
    size_t const field_count)
{
    return format == TrFormat::Table ? make_torrent_info_vec(tor, fields, field_count) :
                                       make_torrent_info_map(tor, fields, field_count);
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentGet(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& args_out)
{
    using namespace JsonRpc;

    auto const params = parse_rpc_params<tr::rpc::TorrentGetParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }

    auto const torrents = getTorrents(session, params->ids.value());
    auto torrents_vec = tr_variant::Vector{};

    auto const format = params->format.value() && params->format.value()->str() == "table"sv ? TrFormat::Table :
                                                                                              TrFormat::Object;

    if (params->ids.value().has_value())
    {
        if (auto const* str_id = std::get_if<std::string>(&*params->ids.value());
            str_id != nullptr && *str_id == tr_quark_get_string_view(TR_KEY_recently_active))
        {
            auto const cutoff = tr_time() - RecentlyActiveSeconds;
            auto const ids = session->torrents().removedSince(cutoff);

            auto removed_vec = tr_variant::Vector{};
            removed_vec.reserve(std::size(ids));
            for (auto const& id : ids)
            {
                removed_vec.emplace_back(id);
            }
            args_out.try_emplace(TR_KEY_removed, std::move(removed_vec));
        }
    }

    auto keys = std::vector<tr_quark>{};
    if (params->fields.value().has_value())
    {
        auto const& fields_vec = *params->fields.value();
        keys.reserve(std::size(fields_vec));
        for (auto const& field_sv : fields_vec)
        {
            if (auto const key = tr_quark_lookup(field_sv); key && isSupportedTorrentGetField(*key))
            {
                keys.emplace_back(*key);
            }
        }
    }

    if (std::empty(keys))
    {
        return { Error::INVALID_PARAMS, "no fields specified"s };
    }

    if (format == TrFormat::Table)
    {
        /* first entry is an array of property names */
        auto names = tr_variant::Vector{};
        names.reserve(std::size(keys));
        std::ranges::transform(keys, std::back_inserter(names), [](tr_quark key) { return tr_quark_get_string_view(key); });
        torrents_vec.emplace_back(std::move(names));
    }

    for (auto* const tor : torrents)
    {
        torrents_vec.emplace_back(make_torrent_info(tor, format, std::data(keys), std::size(keys)));
    }

    args_out.try_emplace(TR_KEY_torrents, std::move(torrents_vec));
    return { Error::SUCCESS, std::string{} }; // no error message
}

// ---

[[nodiscard]] std::tuple<tr_torrent::labels_t, JsonRpc::Error::Code, std::string> make_labels(
    std::vector<std::string> const& labels_vec)
{
    using namespace JsonRpc;

    auto labels = tr_torrent::labels_t{};
    labels.reserve(std::size(labels_vec));
    for (auto const& raw_label : labels_vec)
    {
        auto const label = tr_strv_strip(raw_label);

        if (std::empty(label))
        {
            return { tr_torrent::labels_t{}, Error::INVALID_PARAMS, "labels cannot be empty"s };
        }

        if (tr_strv_contains(label, ','))
        {
            return { tr_torrent::labels_t{}, Error::INVALID_PARAMS, "labels cannot contain comma (,) character"s };
        }

        labels.emplace_back(tr_quark_new(label));
    }

    return { std::move(labels), Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> set_labels(tr_torrent* tor, std::vector<std::string> const& list)
{
    auto [labels, err, errmsg] = make_labels(list);
    if (err == JsonRpc::Error::SUCCESS)
    {
        tor->set_labels(labels);
    }
    return { err, std::move(errmsg) };
}

[[nodiscard]] std::tuple<std::vector<tr_file_index_t>, JsonRpc::Error::Code, std::string> get_file_indices(
    tr_torrent const* tor,
    std::vector<int64_t> const& files_vec)
{
    using namespace JsonRpc;

    auto const n_files = tor->file_count();

    auto files = std::vector<tr_file_index_t>{};
    files.reserve(n_files);

    if (std::empty(files_vec)) // if empty set, apply to all
    {
        files.resize(n_files);
        std::iota(std::begin(files), std::end(files), 0);
    }
    else
    {
        for (auto const val : files_vec)
        {
            if (auto const idx = static_cast<tr_file_index_t>(val); idx < n_files)
            {
                files.push_back(idx);
            }
            else
            {
                return { std::vector<tr_file_index_t>{}, Error::FILE_IDX_OOR, std::string{} };
            }
        }
    }

    return { std::move(files), Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> set_file_priorities(
    tr_torrent* tor,
    tr_priority_t priority,
    std::vector<int64_t> const& files_vec)
{
    auto [indices, err, errmsg] = get_file_indices(tor, files_vec);
    if (err == JsonRpc::Error::SUCCESS)
    {
        tor->set_file_priorities(std::data(indices), std::size(indices), priority);
    }
    return { err, std::move(errmsg) };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> set_sequential_download_from_piece(
    tr_torrent& tor,
    tr_piece_index_t piece)
{
    using namespace JsonRpc;

    if (piece >= tor.piece_count())
    {
        return { Error::PIECE_IDX_OOR, "piece to sequentially download from is outside pieces range"s };
    }

    tor.set_sequential_download_from_piece(piece);
    return { Error::SUCCESS, std::string{} }; // no error
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> set_file_dls(
    tr_torrent* tor,
    bool wanted,
    std::vector<int64_t> const& files_vec)
{
    auto [indices, err, errmsg] = get_file_indices(tor, files_vec);
    if (err == JsonRpc::Error::SUCCESS)
    {
        tor->set_files_wanted(std::data(indices), std::size(indices), wanted);
    }
    return { err, std::move(errmsg) };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> add_tracker_urls(
    tr_torrent* tor,
    std::vector<std::string> const& urls_vec)
{
    using namespace JsonRpc;

    auto ann = tor->announce_list();
    auto const baseline = ann;

    for (auto const& url : urls_vec)
    {
        ann.add(url);
    }

    if (ann == baseline) // unchanged
    {
        return { Error::SET_ANNOUNCE_LIST, std::string{} };
    }

    tor->set_announce_list(std::move(ann));
    return { Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> replace_trackers(
    tr_torrent* tor,
    std::vector<std::variant<int64_t, std::string>> const& urls_vec)
{
    using namespace JsonRpc;

    auto ann = tor->announce_list();
    auto const baseline = ann;

    for (size_t i = 0, vec_size = std::size(urls_vec); i + 1 < vec_size; i += 2U)
    {
        auto const* const id = std::get_if<int64_t>(&urls_vec[i]);
        auto const* const url = std::get_if<std::string>(&urls_vec[i + 1U]);

        if (id != nullptr && url != nullptr)
        {
            ann.replace(static_cast<tr_tracker_id_t>(*id), *url);
        }
    }

    if (ann == baseline) // unchanged
    {
        return { Error::SET_ANNOUNCE_LIST, std::string{} };
    }

    tor->set_announce_list(std::move(ann));
    return { Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> remove_trackers(
    tr_torrent* tor,
    std::vector<int64_t> const& ids_vec)
{
    using namespace JsonRpc;

    auto ann = tor->announce_list();
    auto const baseline = ann;

    for (auto const val : ids_vec)
    {
        ann.remove(static_cast<tr_tracker_id_t>(val));
    }

    if (ann == baseline) // unchanged
    {
        return { Error::SET_ANNOUNCE_LIST, std::string{} };
    }

    tor->set_announce_list(std::move(ann));
    return { Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentSet(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    using namespace JsonRpc;

    auto const params = parse_rpc_params<tr::rpc::TorrentSetParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }

    auto err = Error::SUCCESS;
    auto errmsg = std::string{};

    for (auto* tor : getTorrents(session, params->ids.value()))
    {
        if (auto const& val = params->bandwidth_priority.value(); val)
        {
            if (auto const priority = static_cast<tr_priority_t>(*val); tr_isPriority(priority))
            {
                tr_torrentSetPriority(tor, priority);
            }
        }

        if (auto const& val = params->group.value(); val)
        {
            tor->set_bandwidth_group(*val);
        }

        if (auto const& val = params->labels.value(); val && err == Error::SUCCESS)
        {
            std::tie(err, errmsg) = set_labels(tor, *val);
        }

        if (auto const& val = params->files_unwanted.value(); val && err == Error::SUCCESS)
        {
            std::tie(err, errmsg) = set_file_dls(tor, false, *val);
        }

        if (auto const& val = params->files_wanted.value(); val && err == Error::SUCCESS)
        {
            std::tie(err, errmsg) = set_file_dls(tor, true, *val);
        }

        if (auto const& val = params->peer_limit.value(); val)
        {
            tr_torrentSetPeerLimit(tor, *val);
        }

        if (auto const& val = params->priority_high.value(); val && err == Error::SUCCESS)
        {
            std::tie(err, errmsg) = set_file_priorities(tor, TR_PRI_HIGH, *val);
        }

        if (auto const& val = params->priority_low.value(); val && err == Error::SUCCESS)
        {
            std::tie(err, errmsg) = set_file_priorities(tor, TR_PRI_LOW, *val);
        }

        if (auto const& val = params->priority_normal.value(); val && err == Error::SUCCESS)
        {
            std::tie(err, errmsg) = set_file_priorities(tor, TR_PRI_NORMAL, *val);
        }

        if (auto const& val = params->download_limit.value(); val)
        {
            tr_torrentSetSpeedLimit_KBps(tor, tr_direction::Down, *val);
        }

        if (auto const& val = params->sequential_download.value(); val)
        {
            tor->set_sequential_download(*val);
        }

        if (auto const& val = params->sequential_download_from_piece.value(); val && err == Error::SUCCESS)
        {
            std::tie(err, errmsg) = set_sequential_download_from_piece(*tor, static_cast<tr_piece_index_t>(*val));
        }

        if (auto const& val = params->download_limited.value(); val)
        {
            tor->use_speed_limit(tr_direction::Down, *val);
        }

        if (auto const& val = params->honors_session_limits.value(); val)
        {
            tr_torrentUseSessionLimits(tor, *val);
        }

        if (auto const& val = params->upload_limit.value(); val)
        {
            tr_torrentSetSpeedLimit_KBps(tor, tr_direction::Up, *val);
        }

        if (auto const& val = params->upload_limited.value(); val)
        {
            tor->use_speed_limit(tr_direction::Up, *val);
        }

        if (auto const& val = params->seed_idle_limit.value(); val)
        {
            tor->set_idle_limit_minutes(static_cast<uint16_t>(*val));
        }

        if (auto const& val = params->seed_idle_mode.value(); val)
        {
            tor->set_idle_limit_mode(static_cast<tr_idlelimit>(*val));
        }

        if (auto const& val = params->seed_ratio_limit.value(); val)
        {
            tor->set_seed_ratio(*val);
        }

        if (auto const& val = params->seed_ratio_mode.value(); val)
        {
            tor->set_seed_ratio_mode(static_cast<tr_ratiolimit>(*val));
        }

        if (auto const& val = params->queue_position.value(); val)
        {
            tr_torrentSetQueuePosition(tor, static_cast<size_t>(*val));
        }

        if (auto const& val = params->tracker_add.value(); val)
        {
            std::tie(err, errmsg) = add_tracker_urls(tor, *val);
        }

        if (auto const& val = params->tracker_remove.value(); val)
        {
            std::tie(err, errmsg) = remove_trackers(tor, *val);
        }

        if (auto const& val = params->tracker_replace.value(); val)
        {
            std::tie(err, errmsg) = replace_trackers(tor, *val);
        }

        if (auto const& val = params->tracker_list.value(); val)
        {
            if (!tor->set_announce_list(*val))
            {
                err = Error::INVALID_TRACKER_LIST;
                errmsg = {};
            }
        }

        session->rpcNotify(TR_RPC_TORRENT_CHANGED, tor->id());
    }

    return { err, std::move(errmsg) };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> torrentSetLocation(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    using namespace JsonRpc;

    auto const params = parse_rpc_params<tr::rpc::TorrentSetLocationParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }

    auto const& location = params->location.value();
    if (tr_sys_path_is_relative(location))
    {
        return { Error::PATH_NOT_ABSOLUTE, "new location path is not absolute"s };
    }

    auto const move_flag = params->move.value().value_or(false);
    for (auto* tor : getTorrents(session, params->ids.value()))
    {
        tor->set_location(location, move_flag, nullptr);
        session->rpcNotify(TR_RPC_TORRENT_MOVED, tor->id());
    }

    return { Error::SUCCESS, std::string{} };
}

// ---

void torrentRenamePath(tr_session* session, tr_variant::Map const& args_in, struct tr_rpc_idle_data* idle_data)
{
    using namespace JsonRpc;

    auto const params = parse_rpc_params<tr::rpc::TorrentRenamePathParams>(args_in);
    if (!params)
    {
        tr_rpc_idle_done(idle_data, tr::rpc::classify_params_error(params.error().kind()), params.error().what());
        return;
    }

    auto const torrents = getTorrents(session, params->ids.value());
    if (std::size(torrents) != 1U)
    {
        tr_rpc_idle_done(idle_data, Error::INVALID_PARAMS, "torrent_rename_path requires 1 torrent"sv);
        return;
    }

    tr_torrent* const tor = torrents[0];
    auto const oldpath = params->path.value().value_or(""s);
    auto const newname = params->name.value().value_or(""s);

    write_rpc_result(
        tr::rpc::TorrentRenamePathResult{
            .id = static_cast<int64_t>(tor->id()),
            .path = oldpath,
            .name = newname,
        },
        idle_data->args_out);

    tor->rename_path(
        oldpath,
        newname,
        [idle_data](tr_torrent_id_t, std::string_view, std::string_view, tr_error const& error)
        {
            if (error.has_value())
            {
                tr_rpc_idle_done(idle_data, Error::SYSTEM_ERROR, error.message());
            }
            else
            {
                tr_rpc_idle_done(idle_data, Error::SUCCESS, {});
            }
        });
}

// ---

void onPortTested(tr_web::FetchResponse const& web_response)
{
    using namespace JsonRpc;

    auto const& [status, body, primary_ip, did_connect, did_timeout, user_data] = web_response;
    auto* data = static_cast<tr_rpc_idle_data*>(user_data);

    if (auto const addr = tr_address::from_string(primary_ip);
        !data->args_out.value_if<std::string_view>(TR_KEY_ip_protocol).has_value() && addr && addr->is_valid())
    {
        data->args_out.try_emplace(TR_KEY_ip_protocol, addr->is_ipv4() ? "ipv4"sv : "ipv6"sv);
    }

    if (status != 200)
    {
        tr_rpc_idle_done(
            data,
            Error::HTTP_ERROR,
            fmt::format(
                fmt::runtime(_("Couldn't test port: {error} ({error_code})")),
                fmt::arg("error", tr_webGetResponseStr(status)),
                fmt::arg("error_code", status)));
        return;
    }

    auto const port_is_open = tr_strv_starts_with(body, '1');
    auto res = tr::rpc::PortTestResult{ .port_is_open = port_is_open };
    write_rpc_result(res, data->args_out);
    tr_rpc_idle_done(data, Error::SUCCESS, {});
}

void portTest(tr_session* session, tr_variant::Map const& args_in, struct tr_rpc_idle_data* idle_data)
{
    using namespace JsonRpc;

    static auto constexpr TimeoutSecs = 20s;

    auto const params = parse_rpc_params<tr::rpc::PortTestParams>(args_in);
    if (!params)
    {
        tr_rpc_idle_done(idle_data, tr::rpc::classify_params_error(params.error().kind()), params.error().what());
        return;
    }

    auto const port = session->advertisedPeerPort();
    auto const url = fmt::format("https://portcheck.transmissionbt.com/{:d}", port.host());
    auto ip_proto = std::optional<tr_web::FetchOptions::IPProtocol>{};

    if (auto const& val = params->ip_protocol.value(); val)
    {
        if (val->str() == "ipv4"sv)
        {
            ip_proto = tr_web::FetchOptions::IPProtocol::V4;
            idle_data->args_out.try_emplace(TR_KEY_ip_protocol, "ipv4"sv);
        }
        else
        {
            ip_proto = tr_web::FetchOptions::IPProtocol::V6;
            idle_data->args_out.try_emplace(TR_KEY_ip_protocol, "ipv6"sv);
        }
    }

    auto options = tr_web::FetchOptions{
        url,
        [](tr_web::FetchResponse const& r) { onPortTested(r); },
        idle_data,
    };
    options.timeout_secs = TimeoutSecs;
    if (ip_proto)
    {
        options.ip_proto = *ip_proto;
    }
    session->fetch(std::move(options));
}

// ---

void onBlocklistFetched(tr_web::FetchResponse const& web_response)
{
    using namespace JsonRpc;

    auto const& [status, body, primary_ip, did_connect, did_timeout, user_data] = web_response;
    auto* data = static_cast<struct tr_rpc_idle_data*>(user_data);
    auto* const session = data->session;

    if (status != 200)
    {
        // we failed to download the blocklist...
        tr_rpc_idle_done(
            data,
            Error::HTTP_ERROR,
            fmt::format(
                fmt::runtime(_("Couldn't fetch blocklist: {error} ({error_code})")),
                fmt::arg("error", tr_webGetResponseStr(status)),
                fmt::arg("error_code", status)));
        return;
    }

    // see if we need to decompress the content
    auto content = std::vector<char>{};
    content.resize(1024 * 128);
    for (;;)
    {
        auto decompressor = std::unique_ptr<libdeflate_decompressor, void (*)(libdeflate_decompressor*)>{
            libdeflate_alloc_decompressor(),
            libdeflate_free_decompressor
        };
        auto actual_size = size_t{};
        auto const decompress_result = libdeflate_gzip_decompress(
            decompressor.get(),
            std::data(body),
            std::size(body),
            std::data(content),
            std::size(content),
            &actual_size);
        switch (decompress_result)
        {
        case LIBDEFLATE_INSUFFICIENT_SPACE:
            // need a bigger buffer
            content.resize(content.size() * 2);
            continue;

        case LIBDEFLATE_BAD_DATA:
            // couldn't decompress it; maybe we downloaded an uncompressed file
            content.assign(std::begin(body), std::end(body));
            break;

        case LIBDEFLATE_SUCCESS:
            // shrink buffer to actual size
            content.resize(actual_size);
            break;

        default:
            break;
        }
        break;
    }

    // tr_blocklistSetContent needs a source file,
    // so save content into a tmpfile
    auto const filename = tr_pathbuf{ session->configDir(), "/blocklist.tmp"sv };
    if (auto error = tr_error{}; !tr_file_save(filename, content, &error))
    {
        tr_rpc_idle_done(
            data,
            Error::SYSTEM_ERROR,
            fmt::format(
                fmt::runtime(_("Couldn't save '{path}': {error} ({error_code})")),
                fmt::arg("path", filename),
                fmt::arg("error", error.message()),
                fmt::arg("error_code", error.code())));
        return;
    }

    // feed it to the session and give the client a response
    auto const blocklist_size = tr_blocklistSetContent(session, filename);
    tr_sys_path_remove(filename);
    if (!blocklist_size)
    {
        tr_rpc_idle_done(data, Error::INVALID_BLOCKLIST_DATA, {});
        return;
    }
    write_rpc_result(tr::rpc::BlocklistUpdateResult{ .blocklist_size = static_cast<int64_t>(*blocklist_size) }, data->args_out);
    tr_rpc_idle_done(data, Error::SUCCESS, {});
}

void blocklistUpdate(tr_session* session, tr_variant::Map const& /*args_in*/, struct tr_rpc_idle_data* idle_data)
{
    session->fetch(
        {
            session->blocklistUrl(),
            [](tr_web::FetchResponse const& r) { onBlocklistFetched(r); },
            idle_data,
        });
}

// ---

void add_torrent_impl(struct tr_rpc_idle_data* data, tr_ctor& ctor)
{
    using namespace JsonRpc;

    tr_torrent* duplicate_of = nullptr;
    tr_torrent* tor = tr_torrentNew(&ctor, &duplicate_of);

    if (tor == nullptr && duplicate_of == nullptr)
    {
        tr_rpc_idle_done(data, Error::CORRUPT_TORRENT, {});
        return;
    }

    auto const make_summary = [](tr_torrent const* t)
    {
        return tr::rpc::TorrentAddedSummary{
            .id = static_cast<int64_t>(t->id()),
            .name = std::string{ t->name() },
            .hash_string = std::string{ t->info_hash_string().sv() },
        };
    };

    if (duplicate_of != nullptr)
    {
        write_rpc_result(tr::rpc::TorrentAddResult{ .torrent_duplicate = make_summary(duplicate_of) }, data->args_out);
        tr_rpc_idle_done(data, Error::SUCCESS, {});
        return;
    }

    data->session->rpcNotify(TR_RPC_TORRENT_ADDED, tor->id());
    write_rpc_result(tr::rpc::TorrentAddResult{ .torrent_added = make_summary(tor) }, data->args_out);
    tr_rpc_idle_done(data, Error::SUCCESS, {});
}

struct add_torrent_idle_data
{
    add_torrent_idle_data(tr_rpc_idle_data* data_in, tr_ctor&& ctor_in)
        : data{ data_in }
        , ctor{ std::move(ctor_in) }
    {
    }

    tr_rpc_idle_data* data;
    tr_ctor ctor;
};

void onMetadataFetched(tr_web::FetchResponse const& web_response)
{
    auto const& [status, body, primary_ip, did_connect, did_timeout, user_data] = web_response;
    auto* data = static_cast<struct add_torrent_idle_data*>(user_data);

    tr_logAddTrace(
        fmt::format(
            "torrentAdd: HTTP response code was {} ({}); response length was {} bytes",
            status,
            tr_webGetResponseStr(status),
            std::size(body)));

    if (status == 200 || status == 221) /* http or ftp success.. */
    {
        data->ctor.set_metainfo(body);
        add_torrent_impl(data->data, data->ctor);
    }
    else
    {
        tr_rpc_idle_done(
            data->data,
            JsonRpc::Error::HTTP_ERROR,
            fmt::format(
                fmt::runtime(_("Couldn't fetch torrent: {error} ({error_code})")),
                fmt::arg("error", tr_webGetResponseStr(status)),
                fmt::arg("error_code", status)));
    }

    delete data;
}

bool isCurlURL(std::string_view url)
{
    auto constexpr Schemes = std::array<std::string_view, 4>{ "http"sv, "https"sv, "ftp"sv, "sftp"sv };
    auto const parsed = tr_urlParse(url);
    return parsed && std::ranges::find(Schemes, parsed->scheme) != std::ranges::end(Schemes);
}

[[nodiscard]] auto file_list_from_list(std::vector<int64_t> const& idx_vec)
{
    auto files = std::vector<tr_file_index_t>{};
    files.reserve(std::size(idx_vec));
    for (auto const val : idx_vec)
    {
        files.emplace_back(static_cast<tr_file_index_t>(val));
    }
    return files;
}

void torrentAdd(tr_session* session, tr_variant::Map const& args_in, tr_rpc_idle_data* idle_data)
{
    using namespace JsonRpc;

    TR_ASSERT(idle_data != nullptr);

    auto const params = parse_rpc_params<tr::rpc::TorrentAddParams>(args_in);
    if (!params)
    {
        tr_rpc_idle_done(idle_data, tr::rpc::classify_params_error(params.error().kind()), params.error().what());
        return;
    }

    auto const filename = params->filename.value().value_or(""s);
    auto const metainfo_base64 = params->metainfo.value().value_or(""s);
    if (std::empty(filename) && std::empty(metainfo_base64))
    {
        tr_rpc_idle_done(idle_data, Error::INVALID_PARAMS, "no filename or metainfo specified"sv);
        return;
    }

    auto const& download_dir = params->download_dir.value();
    if (download_dir && tr_sys_path_is_relative(*download_dir))
    {
        tr_rpc_idle_done(idle_data, Error::PATH_NOT_ABSOLUTE, "download directory path is not absolute"sv);
        return;
    }

    auto ctor = tr_ctor{ session };

    // set the optional parameters

    auto const cookies = params->cookies.value().value_or(""s);

    if (download_dir && !std::empty(*download_dir))
    {
        ctor.set_download_dir(TR_FORCE, *download_dir);
    }

    if (auto const& val = params->paused.value(); val)
    {
        ctor.set_paused(TR_FORCE, *val);
    }

    if (auto const& val = params->peer_limit.value(); val)
    {
        ctor.set_peer_limit(TR_FORCE, static_cast<uint16_t>(*val));
    }

    if (auto const& val = params->bandwidth_priority.value(); val)
    {
        ctor.set_bandwidth_priority(static_cast<tr_priority_t>(*val));
    }

    if (auto const& val = params->files_unwanted.value(); val)
    {
        auto const files = file_list_from_list(*val);
        ctor.set_files_wanted(std::data(files), std::size(files), false);
    }

    if (auto const& val = params->files_wanted.value(); val)
    {
        auto const files = file_list_from_list(*val);
        ctor.set_files_wanted(std::data(files), std::size(files), true);
    }

    if (auto const& val = params->priority_low.value(); val)
    {
        auto const files = file_list_from_list(*val);
        ctor.set_file_priorities(std::data(files), std::size(files), TR_PRI_LOW);
    }

    if (auto const& val = params->priority_normal.value(); val)
    {
        auto const files = file_list_from_list(*val);
        ctor.set_file_priorities(std::data(files), std::size(files), TR_PRI_NORMAL);
    }

    if (auto const& val = params->priority_high.value(); val)
    {
        auto const files = file_list_from_list(*val);
        ctor.set_file_priorities(std::data(files), std::size(files), TR_PRI_HIGH);
    }

    if (auto const& val = params->labels.value(); val)
    {
        auto [labels, err, errmsg] = make_labels(*val);

        if (err != Error::SUCCESS)
        {
            tr_rpc_idle_done(idle_data, err, errmsg);
            return;
        }

        ctor.set_labels(std::move(labels));
    }

    if (auto const& val = params->sequential_download.value(); val)
    {
        ctor.set_sequential_download(TR_FORCE, *val);
    }

    if (auto const& val = params->sequential_download_from_piece.value(); val)
    {
        ctor.set_sequential_download_from_piece(TR_FORCE, static_cast<tr_piece_index_t>(*val));
    }

    tr_logAddTrace(fmt::format("torrentAdd: filename is '{}'", filename));

    if (isCurlURL(filename))
    {
        auto* const d = new add_torrent_idle_data{ idle_data, std::move(ctor) };
        auto options = tr_web::FetchOptions{
            filename,
            [](tr_web::FetchResponse const& r) { onMetadataFetched(r); },
            d,
        };
        options.cookies = cookies;
        session->fetch(std::move(options));
        return;
    }

    auto ok = false;

    if (std::empty(filename))
    {
        ok = ctor.set_metainfo(tr_base64_decode(metainfo_base64));
    }
    else if (tr_sys_path_exists(filename))
    {
        ok = ctor.set_metainfo_from_file(filename);
    }
    else
    {
        ok = ctor.set_metainfo_from_magnet_link(filename);
    }

    if (!ok)
    {
        tr_rpc_idle_done(idle_data, Error::UNRECOGNIZED_INFO, {});
        return;
    }

    add_torrent_impl(idle_data, ctor);
}

// ---

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> groupGet(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& args_out)
{
    auto const params = parse_rpc_params<tr::rpc::GroupGetParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }

    auto names = std::set<std::string_view>{};
    if (auto const& name_param = params->name.value(); name_param)
    {
        std::visit(
            [&names](auto const& val)
            {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, std::string>)
                {
                    names.insert(val);
                }
                else
                {
                    for (auto const& item : val)
                    {
                        names.insert(item);
                    }
                }
            },
            *name_param);
    }

    auto result = tr::rpc::GroupGetResult{};
    for (auto const& [name, group] : session->bandwidthGroups())
    {
        if (names.empty() || names.contains(name.sv()))
        {
            auto const limits = group->get_limits();
            result.group.value().push_back(
                tr::rpc::BandwidthGroupInfo{
                    .honors_session_limits = group->are_parent_limits_honored(tr_direction::Up),
                    .name = std::string{ name.sv() },
                    .speed_limit_down = static_cast<int64_t>(limits.down_limit.count(Speed::Units::KByps)),
                    .speed_limit_down_enabled = limits.down_limited,
                    .speed_limit_up = static_cast<int64_t>(limits.up_limit.count(Speed::Units::KByps)),
                    .speed_limit_up_enabled = limits.up_limited,
                });
        }
    }
    write_rpc_result(result, args_out);

    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> groupSet(
    tr_session* session,
    tr_variant::Map const& args_in,
    tr_variant::Map& /*args_out*/)
{
    using namespace JsonRpc;

    auto const params = parse_rpc_params<tr::rpc::GroupSetParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }

    auto const name = tr_strv_strip(params->name.value());
    if (std::empty(name))
    {
        return { Error::INVALID_PARAMS, "No group name given"s };
    }

    auto& group = session->getBandwidthGroup(name);
    auto limits = group.get_limits();

    if (auto const& val = params->speed_limit_down_enabled.value(); val)
    {
        limits.down_limited = *val;
    }

    if (auto const& val = params->speed_limit_up_enabled.value(); val)
    {
        limits.up_limited = *val;
    }

    if (auto const& val = params->speed_limit_down.value(); val)
    {
        limits.down_limit = Speed{ *val, Speed::Units::KByps };
    }

    if (auto const& val = params->speed_limit_up.value(); val)
    {
        limits.up_limit = Speed{ *val, Speed::Units::KByps };
    }

    group.set_limits(limits);

    if (auto const& val = params->honors_session_limits.value(); val)
    {
        group.honor_parent_limits(tr_direction::Up, *val);
        group.honor_parent_limits(tr_direction::Down, *val);
    }

    return { Error::SUCCESS, std::string{} };
}

// ---

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> sessionStats(
    tr_session* session,
    tr_variant::Map const& /*args_in*/,
    tr_variant::Map& args_out)
{
    auto const make_stats = [](auto const& stats)
    {
        return tr::rpc::SessionTransferStats{
            .downloaded_bytes = static_cast<int64_t>(stats.downloadedBytes),
            .files_added = static_cast<int64_t>(stats.filesAdded),
            .seconds_active = static_cast<int64_t>(stats.secondsActive),
            .session_count = static_cast<int64_t>(stats.sessionCount),
            .uploaded_bytes = static_cast<int64_t>(stats.uploadedBytes),
        };
    };

    auto const& torrents = session->torrents();
    auto const total = static_cast<int64_t>(std::size(torrents));
    auto const n_running = static_cast<int64_t>(std::count_if(
        std::begin(torrents),
        std::end(torrents),
        [](auto const* tor) { return tor->is_running(); }));

    write_rpc_result(
        tr::rpc::SessionStatsResult{
            .active_torrent_count = n_running,
            .cumulative_stats = make_stats(session->stats().cumulative()),
            .current_stats = make_stats(session->stats().current()),
            .download_speed = static_cast<int64_t>(session->piece_speed(tr_direction::Down).base_quantity()),
            .paused_torrent_count = total - n_running,
            .torrent_count = total,
            .upload_speed = static_cast<int64_t>(session->piece_speed(tr_direction::Up).base_quantity()),
        },
        args_out);

    return { JsonRpc::Error::SUCCESS, std::string{} };
}

[[nodiscard]] auto values_get_units()
{
    using namespace tr::Values;

    auto const make_units_vec = [](auto const& units)
    {
        auto units_vec = std::vector<std::string>{};
        for (size_t i = 0;; ++i)
        {
            auto const display_name = units.display_name(i);
            if (std::empty(display_name))
            {
                break;
            }
            units_vec.emplace_back(display_name);
        }
        return units_vec;
    };

    return to_tr_variant(
        tr::rpc::SessionUnits{
            .memory_bytes = static_cast<int64_t>(Memory::units().base()),
            .memory_units = make_units_vec(Memory::units()),
            .size_bytes = static_cast<int64_t>(Storage::units().base()),
            .size_units = make_units_vec(Storage::units()),
            .speed_bytes = static_cast<int64_t>(Speed::units().base()),
            .speed_units = make_units_vec(Speed::units()),
        });
}

using ErrorInfo = std::pair<JsonRpc::Error::Code, std::string>;
auto const error_none = ErrorInfo{ JsonRpc::Error::SUCCESS, std::string{} };

using SessionGetter = std::function<tr_variant(tr_session const& src)>;
using SessionSetter = std::function<void(tr_session& tgt, tr_variant const&, ErrorInfo& err)>;
using SessionAccessors = std::pair<SessionGetter, SessionSetter>;

[[nodiscard]] auto& session_accessors()
{
    static auto map = small::max_size_map<tr_quark, SessionAccessors, 64U>{};

    if (!std::empty(map))
    {
        return map;
    }

    map.try_emplace(
        TR_KEY_alt_speed_down,
        [](tr_session const& src) -> tr_variant { return tr_sessionGetAltSpeed_KBps(&src, tr_direction::Down); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tr_sessionSetAltSpeed_KBps(&tgt, tr_direction::Down, *val);
            }
        });

    map.try_emplace(
        TR_KEY_alt_speed_enabled,
        [](tr_session const& src) -> tr_variant { return tr_sessionUsesAltSpeed(&src); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionUseAltSpeed(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_alt_speed_time_begin,
        [](tr_session const& src) -> tr_variant { return tr_sessionGetAltSpeedBegin(&src); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tr_sessionSetAltSpeedBegin(&tgt, static_cast<size_t>(*val));
            }
        });

    map.try_emplace(
        TR_KEY_alt_speed_time_day,
        [](tr_session const& src) -> tr_variant { return tr_sessionGetAltSpeedDay(&src); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = tr::serializer::to_value<tr_sched_day>(src))
            {
                tr_sessionSetAltSpeedDay(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_alt_speed_time_enabled,
        [](tr_session const& src) -> tr_variant { return tr_sessionUsesAltSpeedTime(&src); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionUseAltSpeedTime(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_alt_speed_time_end,
        [](tr_session const& src) -> tr_variant { return tr_sessionGetAltSpeedEnd(&src); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tr_sessionSetAltSpeedEnd(&tgt, static_cast<size_t>(*val));
            }
        });

    map.try_emplace(
        TR_KEY_alt_speed_up,
        [](tr_session const& src) -> tr_variant { return tr_sessionGetAltSpeed_KBps(&src, tr_direction::Up); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tr_sessionSetAltSpeed_KBps(&tgt, tr_direction::Up, *val);
            }
        });

    map.try_emplace(
        TR_KEY_anti_brute_force_enabled,
        [](tr_session const& src) -> tr_variant { return src.is_anti_brute_force_enabled(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tgt.set_anti_brute_force_enabled(*val);
            }
        });

    map.try_emplace(
        TR_KEY_anti_brute_force_threshold,
        [](tr_session const& src) -> tr_variant { return src.get_anti_brute_force_limit(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tgt.set_anti_brute_force_limit(static_cast<size_t>(*val));
            }
        });

    map.try_emplace(
        TR_KEY_blocklist_enabled,
        [](tr_session const& src) -> tr_variant { return src.blocklist_enabled(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tgt.set_blocklist_enabled(*val);
            }
        });

    map.try_emplace(
        TR_KEY_blocklist_size,
        [](tr_session const& src) -> tr_variant { return tr_blocklistGetRuleCount(&src); },
        nullptr);

    map.try_emplace(
        TR_KEY_blocklist_url,
        [](tr_session const& src) -> tr_variant { return src.blocklistUrl(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<std::string_view>())
            {
                tgt.setBlocklistUrl(*val);
            }
        });

    map.try_emplace(
        TR_KEY_cache_size_mib,
        [](tr_session const& src) -> tr_variant { return src.unused_cache_size_mbytes(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tgt.set_unused_cache_size_mbytes(*val);
            }
        });

    map.try_emplace(TR_KEY_config_dir, [](tr_session const& src) -> tr_variant { return src.configDir(); }, nullptr);

    map.try_emplace(
        TR_KEY_default_trackers,
        [](tr_session const& src) -> tr_variant { return src.defaultTrackersStr(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<std::string_view>())
            {
                tgt.setDefaultTrackers(*val);
            }
        });

    map.try_emplace(
        TR_KEY_dht_enabled,
        [](tr_session const& src) -> tr_variant { return src.allowsDHT(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetDHTEnabled(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_download_dir,
        [](tr_session const& src) -> tr_variant { return src.downloadDir(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& err)
        {
            if (auto const download_dir = src.value_if<std::string_view>())
            {
                if (tr_sys_path_is_relative(*download_dir))
                {
                    err = { JsonRpc::Error::PATH_NOT_ABSOLUTE, "download directory path is not absolute"s };
                }
                else if (!std::empty(*download_dir))
                {
                    tgt.setDownloadDir(*download_dir);
                }
            }
        });

    map.try_emplace(
        TR_KEY_download_dir_free_space,
        [](tr_session const& src) -> tr_variant
        {
            // TODO(C++23): use std::optional::transform() instead
            if (auto const space = tr_sys_path_get_capacity(src.downloadDir()))
            {
                return space->available;
            }
            return -1;
        },
        nullptr);

    map.try_emplace(
        TR_KEY_download_queue_enabled,
        [](tr_session const& src) -> tr_variant { return src.queueEnabled(tr_direction::Down); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetQueueEnabled(&tgt, tr_direction::Down, *val);
            }
        });

    map.try_emplace(
        TR_KEY_download_queue_size,
        [](tr_session const& src) -> tr_variant { return src.queueSize(tr_direction::Down); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tr_sessionSetQueueSize(&tgt, tr_direction::Down, *val);
            }
        });

    map.try_emplace(
        TR_KEY_encryption,
        [](tr_session const& src) { return src.serialize_encryption_mode(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& err)
        {
            if (!tgt.deserialize_encryption_mode(src))
            {
                err = { JsonRpc::Error::INVALID_PARAMS, R"(must be one of "preferred", "required" or "allowed")"s };
            }
        });

    map.try_emplace(
        TR_KEY_idle_seeding_limit,
        [](tr_session const& src) -> tr_variant { return src.idleLimitMinutes(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<uint16_t>())
            {
                tr_sessionSetIdleLimit(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_idle_seeding_limit_enabled,
        [](tr_session const& src) -> tr_variant { return src.isIdleLimited(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetIdleLimited(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_incomplete_dir,
        [](tr_session const& src) -> tr_variant { return src.incompleteDir(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& err)
        {
            if (auto const dir = src.value_if<std::string_view>())
            {
                if (tr_sys_path_is_relative(*dir))
                {
                    err = { JsonRpc::Error::PATH_NOT_ABSOLUTE, "incomplete torrents directory path is not absolute"s };
                }
                else if (!std::empty(*dir))
                {
                    tgt.setIncompleteDir(*dir);
                }
            }
        });

    map.try_emplace(
        TR_KEY_incomplete_dir_enabled,
        [](tr_session const& src) -> tr_variant { return src.useIncompleteDir(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tgt.useIncompleteDir(*val);
            }
        });

    map.try_emplace(
        TR_KEY_lpd_enabled,
        [](tr_session const& src) -> tr_variant { return src.allowsLPD(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetLPDEnabled(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_peer_limit_global,
        [](tr_session const& src) -> tr_variant { return src.peerLimit(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tr_sessionSetPeerLimit(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_peer_limit_per_torrent,
        [](tr_session const& src) -> tr_variant { return src.peerLimitPerTorrent(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tr_sessionSetPeerLimitPerTorrent(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_peer_port,
        [](tr_session const& src) -> tr_variant { return src.advertisedPeerPort().host(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<uint16_t>())
            {
                tr_sessionSetPeerPort(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_peer_port_random_on_start,
        [](tr_session const& src) -> tr_variant { return src.isPortRandom(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetPeerPortRandomOnStart(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_pex_enabled,
        [](tr_session const& src) -> tr_variant { return src.allows_pex(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetPexEnabled(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_port_forwarding_enabled,
        [](tr_session const& src) -> tr_variant { return tr_sessionIsPortForwardingEnabled(&src); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetPortForwardingEnabled(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_preferred_transports,
        [](tr_session const& src) -> tr_variant { return src.save_preferred_transports(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& err)
        {
            if (!tgt.load_preferred_transports(src))
            {
                err = { JsonRpc::Error::INVALID_PARAMS, R"(the list must be unique with the values "utp" or "tcp")" };
            }
        });

    map.try_emplace(
        TR_KEY_queue_stalled_enabled,
        [](tr_session const& src) -> tr_variant { return src.queueStalledEnabled(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetQueueStalledEnabled(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_queue_stalled_minutes,
        [](tr_session const& src) -> tr_variant { return src.queueStalledMinutes(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<size_t>())
            {
                tr_sessionSetQueueStalledMinutes(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_rename_partial_files,
        [](tr_session const& src) -> tr_variant { return src.isIncompleteFileNamingEnabled(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetIncompleteFileNamingEnabled(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_reqq,
        [](tr_session const& src) -> tr_variant { return src.reqq(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tgt.set_reqq(*val);
            }
        });

    map.try_emplace(TR_KEY_rpc_version, [](tr_session const& /*src*/) -> tr_variant { return RpcVersion; }, nullptr);

    map.try_emplace(TR_KEY_rpc_version_minimum, [](tr_session const& /*src*/) -> tr_variant { return RpcVersionMin; }, nullptr);

    map.try_emplace(
        TR_KEY_rpc_version_semver,
        [](tr_session const& /*src*/) -> tr_variant { return tr_variant::unmanaged_string(TrRpcVersionSemver); },
        nullptr);

    map.try_emplace(
        TR_KEY_seed_ratio_limit,
        [](tr_session const& src) -> tr_variant { return src.desiredRatio(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<double>())
            {
                tr_sessionSetRatioLimit(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_seed_ratio_limited,
        [](tr_session const& src) -> tr_variant { return src.isRatioLimited(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetRatioLimited(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_seed_queue_enabled,
        [](tr_session const& src) -> tr_variant { return src.queueEnabled(tr_direction::Up); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetQueueEnabled(&tgt, tr_direction::Up, *val);
            }
        });

    map.try_emplace(
        TR_KEY_seed_queue_size,
        [](tr_session const& src) -> tr_variant { return src.queueSize(tr_direction::Up); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tr_sessionSetQueueSize(&tgt, tr_direction::Up, *val);
            }
        });

    map.try_emplace(
        TR_KEY_sequential_download,
        [](tr_session const& src) -> tr_variant { return src.sequential_download(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tgt.set_sequential_download(*val);
            }
        });

    map.try_emplace(TR_KEY_session_id, [](tr_session const& src) -> tr_variant { return src.sessionId(); }, nullptr);

    map.try_emplace(
        TR_KEY_speed_limit_down,
        [](tr_session const& src) -> tr_variant
        { return static_cast<int64_t>(src.speed_limit(tr_direction::Down).count(Speed::Units::KByps)); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tgt.set_speed_limit(tr_direction::Down, Speed{ *val, Speed::Units::KByps });
            }
        });

    map.try_emplace(
        TR_KEY_speed_limit_down_enabled,
        [](tr_session const& src) -> tr_variant { return src.is_speed_limited(tr_direction::Down); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionLimitSpeed(&tgt, tr_direction::Down, *val);
            }
        });

    map.try_emplace(
        TR_KEY_speed_limit_up,
        [](tr_session const& src) -> tr_variant
        { return static_cast<int64_t>(src.speed_limit(tr_direction::Up).count(Speed::Units::KByps)); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<int64_t>())
            {
                tgt.set_speed_limit(tr_direction::Up, Speed{ *val, Speed::Units::KByps });
            }
        });

    map.try_emplace(
        TR_KEY_speed_limit_up_enabled,
        [](tr_session const& src) -> tr_variant { return src.is_speed_limited(tr_direction::Up); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionLimitSpeed(&tgt, tr_direction::Up, *val);
            }
        });

    map.try_emplace(
        TR_KEY_start_added_torrents,
        [](tr_session const& src) -> tr_variant { return !src.shouldPauseAddedTorrents(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetPaused(&tgt, !*val);
            }
        });

    map.try_emplace(TR_KEY_tcp_enabled, [](tr_session const& src) -> tr_variant { return src.allowsTCP(); }, nullptr);

    map.try_emplace(
        TR_KEY_trash_original_torrent_files,
        [](tr_session const& src) -> tr_variant { return src.shouldDeleteSource(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetDeleteSource(&tgt, *val);
            }
        });

    map.try_emplace(TR_KEY_units, [](tr_session const& /*src*/) -> tr_variant { return values_get_units(); }, nullptr);

    map.try_emplace(
        TR_KEY_utp_enabled,
        [](tr_session const& src) -> tr_variant { return src.allowsUTP(); },
        [](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
        {
            if (auto const val = src.value_if<bool>())
            {
                tr_sessionSetUTPEnabled(&tgt, *val);
            }
        });

    map.try_emplace(
        TR_KEY_version,
        [](tr_session const& /*src*/) -> tr_variant { return tr_variant::unmanaged_string(LONG_VERSION_STRING); },
        nullptr);

    // `row` could have been replaced by structured bindings,
    // but it's not available until clang 16
    // https://github.com/llvm/llvm-project/commit/44f2baa3804a62ca793f0ff3e43aa71cea91a795
    for (auto const& row : tr_session::Scripts)
    {
        auto const script = row.script;

        map.try_emplace(
            row.enabled_key,
            [script](tr_session const& src) -> tr_variant { return src.useScript(script); },
            [script](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
            {
                if (auto const val = src.value_if<bool>())
                {
                    tgt.useScript(script, *val);
                }
            });

        map.try_emplace(
            row.filename_key,
            [script](tr_session const& src) -> tr_variant { return src.script(script); },
            [script](tr_session& tgt, tr_variant const& src, ErrorInfo& /*err*/)
            {
                if (auto const val = src.value_if<std::string_view>())
                {
                    tgt.setScript(script, *val);
                }
            });
    }

    return map;
}

[[nodiscard]] auto get_session_fields(std::optional<std::vector<std::string>> const& fields_vec)
{
    auto fields = std::set<tr_quark>{};

    if (fields_vec)
    {
        for (auto const& field_name : *fields_vec)
        {
            if (auto const field_id = tr_quark_lookup(field_name); field_id)
            {
                fields.insert(*field_id);
            }
        }
    }

    if (std::empty(fields)) // no fields specified; get them all
    {
        for (auto const& [key, accessor] : session_accessors())
        {
            fields.insert(key);
        }
    }

    return fields;
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> sessionSet(
    tr_session* session,
    tr_variant::Map const& params,
    tr_variant::Map& /*result*/)
{
    auto const parsed = parse_rpc_params<tr::rpc::SessionSetParams>(params);
    if (!parsed)
    {
        return { tr::rpc::classify_params_error(parsed.error().kind()), parsed.error().what() };
    }

    auto const& accessors = session_accessors();

    auto err = error_none;

    for (auto const& [key, val] : params)
    {
        if (auto const iter = accessors.find(key); iter != std::end(accessors))
        {
            if (auto const& [getter, setter] = iter->second; setter)
            {
                setter(*session, val, err);
            }
        }

        if (err != error_none)
        {
            break;
        }
    }

    session->rpcNotify(TR_RPC_SESSION_CHANGED);
    return err;
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> sessionGet(
    tr_session* session,
    tr_variant::Map const& params,
    tr_variant::Map& result)
{
    auto const parsed = parse_rpc_params<tr::rpc::SessionGetParams>(params);
    if (!parsed)
    {
        return { tr::rpc::classify_params_error(parsed.error().kind()), parsed.error().what() };
    }

    auto const& accessors = session_accessors();

    for (auto const key : get_session_fields(parsed->fields.value()))
    {
        if (auto const iter = accessors.find(key); iter != std::end(accessors))
        {
            if (auto const& [getter, setter] = iter->second; getter)
            {
                result.try_emplace(key, getter(*session));
            }
        }
    }

    return error_none;
}

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> freeSpace(
    tr_session* /*session*/,
    tr_variant::Map const& args_in,
    tr_variant::Map& args_out)
{
    using namespace JsonRpc;

    auto const params = parse_rpc_params<tr::rpc::FreeSpaceParams>(args_in);
    if (!params)
    {
        return { tr::rpc::classify_params_error(params.error().kind()), params.error().what() };
    }

    auto const& path = params->path.value();
    if (tr_sys_path_is_relative(path))
    {
        return { Error::PATH_NOT_ABSOLUTE, "directory path is not absolute"s };
    }

    // get the free space
    auto error = tr_error{};
    auto const capacity = tr_sys_path_get_capacity(path, &error);

    // response
    write_rpc_result(
        tr::rpc::FreeSpaceResult{
            .path = path,
            .size_bytes = capacity ? static_cast<int64_t>(capacity->available) : -1,
            .total_size = capacity ? static_cast<int64_t>(capacity->capacity) : -1,
        },
        args_out);

    if (error)
    {
        return { Error::SYSTEM_ERROR, std::string{ error.message() } };
    }
    return { Error::SUCCESS, std::string{} };
}

// ---

[[nodiscard]] std::pair<JsonRpc::Error::Code, std::string> sessionClose(
    tr_session* session,
    tr_variant::Map const& /*args_in*/,
    tr_variant::Map& /*args_out*/)
{
    session->rpcNotify(TR_RPC_SESSION_CLOSE);
    return { JsonRpc::Error::SUCCESS, std::string{} };
}

// ---

using SyncHandler = std::pair<JsonRpc::Error::Code, std::string> (*)(tr_session*, tr_variant::Map const&, tr_variant::Map&);

auto const sync_handlers = small::max_size_map<tr_quark, std::pair<SyncHandler, bool /*has_side_effects*/>, 20U>{ {
    { TR_KEY_free_space, { freeSpace, false } },
    { TR_KEY_group_get, { groupGet, false } },
    { TR_KEY_group_set, { groupSet, true } },
    { TR_KEY_queue_move_bottom, { queueMoveBottom, true } },
    { TR_KEY_queue_move_down, { queueMoveDown, true } },
    { TR_KEY_queue_move_top, { queueMoveTop, true } },
    { TR_KEY_queue_move_up, { queueMoveUp, true } },
    { TR_KEY_session_close, { sessionClose, true } },
    { TR_KEY_session_get, { sessionGet, false } },
    { TR_KEY_session_set, { sessionSet, true } },
    { TR_KEY_session_stats, { sessionStats, false } },
    { TR_KEY_torrent_get, { torrentGet, false } },
    { TR_KEY_torrent_reannounce, { torrentReannounce, true } },
    { TR_KEY_torrent_remove, { torrentRemove, true } },
    { TR_KEY_torrent_set, { torrentSet, true } },
    { TR_KEY_torrent_set_location, { torrentSetLocation, true } },
    { TR_KEY_torrent_start, { torrentStart, true } },
    { TR_KEY_torrent_start_now, { torrentStartNow, true } },
    { TR_KEY_torrent_stop, { torrentStop, true } },
    { TR_KEY_torrent_verify, { torrentVerify, true } },
} };

using AsyncHandler = void (*)(tr_session*, tr_variant::Map const&, tr_rpc_idle_data*);

auto const async_handlers = small::max_size_map<tr_quark, std::pair<AsyncHandler, bool /*has_side_effects*/>, 4U>{ {
    { TR_KEY_blocklist_update, { blocklistUpdate, true } },
    { TR_KEY_port_test, { portTest, false } },
    { TR_KEY_torrent_add, { torrentAdd, true } },
    { TR_KEY_torrent_rename_path, { torrentRenamePath, true } },
} };

void noop_response_callback(tr_variant&& /*response*/)
{
}

void noop_json_response_callback(std::string&& /*response*/)
{
}

[[nodiscard]] std::string format_envelope_error_string(rfl::Error const& err)
{
    auto const details = err.details();
    if (!std::empty(details) && !std::empty(details.front().path))
    {
        if (auto const* field = std::get_if<std::string>(&details.front().path.front()); field != nullptr)
        {
            if (*field == "jsonrpc"sv)
            {
                return "JSON-RPC version is not 2.0"s;
            }
            if (*field == "id"sv)
            {
                return "id type must be String, Number, or Null"s;
            }
        }
    }

    if (auto const& msg = err.what(); msg.find("'jsonrpc'") != std::string::npos)
    {
        return "JSON-RPC version is not 2.0"s;
    }
    else if (msg.find("'id'") != std::string::npos)
    {
        return "id type must be String, Number, or Null"s;
    }

    return err.what();
}

void dispatch_method(tr_session* session, tr_quark const method_key, tr_variant::Map const& params, tr_rpc_idle_data* data)
{
    using namespace JsonRpc;

    auto const is_notification = data->is_jsonrpc && (data->json_callback ? data->rpc_id.is_notification() : !data->id.has_value());

    if (auto const handler = async_handlers.find(method_key); handler != std::end(async_handlers))
    {
        auto const& [func, has_side_effects] = handler->second;
        if (is_notification && !has_side_effects)
        {
            tr_rpc_idle_done(data, Error::SUCCESS, {});
            return;
        }

        func(session, params, data);
        return;
    }

    if (auto const handler = sync_handlers.find(method_key); handler != std::end(sync_handlers))
    {
        auto const& [func, has_side_effects] = handler->second;
        if (is_notification && !has_side_effects)
        {
            tr_rpc_idle_done(data, Error::SUCCESS, {});
            return;
        }

        auto const [err, errmsg] = func(session, params, data->args_out);
        tr_rpc_idle_done(data, err, errmsg);
        return;
    }

    // couldn't find a handler
    tr_rpc_idle_done(data, Error::METHOD_NOT_FOUND, {});
}

void tr_rpc_request_exec_impl(tr_session* session, tr_variant& request, tr_rpc_response_func&& callback, bool is_batch)
{
    using namespace JsonRpc;

    if (!callback)
    {
        callback = noop_response_callback;
    }

    auto const* map = request.get_if<tr_variant::Map>();
    if (map == nullptr)
    {
        callback(build_response(
            Error::INVALID_REQUEST,
            nullptr,
            Error::build_data(is_batch ? "request must be an Object"sv : "request must be an Array or Object"sv, {})));
        return;
    }

    auto is_jsonrpc = false;
    if (auto jsonrpc = map->value_if<std::string_view>(TR_KEY_jsonrpc); jsonrpc == Version)
    {
        is_jsonrpc = true;
    }
    else if (jsonrpc || is_batch)
    {
        callback(build_response(Error::INVALID_REQUEST, nullptr, Error::build_data("JSON-RPC version is not 2.0"sv, {})));
        return;
    }
    else
    {
        tr::api_compat::convert_incoming_data(request);
        map = request.get_if<tr_variant::Map>();
        TR_ASSERT(map != nullptr);
        if (map == nullptr)
        {
            auto response = tr_variant::Map{ 2U };
            response.try_emplace(TR_KEY_arguments, tr_variant::Map{});
            response.try_emplace(
                TR_KEY_result,
                tr_variant::unmanaged_string(
                    "bug in api-compat, please report a bug at https://github.com/transmission/transmission/issues"sv));
            callback(std::move(response));
            return;
        }
    }

    auto const empty_params = tr_variant::Map{};
    auto const* params = map->find_if<tr_variant::Map>(TR_KEY_params);
    if (params == nullptr)
    {
        params = &empty_params;
    }

    auto const method_name = map->value_if<std::string_view>(TR_KEY_method).value_or(""sv);
    auto const method_key = tr_quark_new(method_name);

    auto* const data = new tr_rpc_idle_data{};
    data->session = session;
    data->is_jsonrpc = is_jsonrpc;
    data->callback = std::move(callback);

    if (auto iter = map->find(TR_KEY_id); iter != std::end(*map))
    {
        auto parsed_id = rfl::from_generic<tr::rpc::RequestId>(tr_variant_to_rfl_generic(iter->second));
        if (!parsed_id)
        {
            data->id = nullptr;
            tr_rpc_idle_done(data, tr::rpc::classify_envelope_error(parsed_id.error().kind()), parsed_id.error().what());
            return;
        }
        data->rpc_id = *parsed_id;
        data->id = request_id_to_tr_variant(*parsed_id);
    }

    dispatch_method(session, method_key, *params, data);
}

void exec_single_json_request(tr_session* session, std::string_view raw_json, tr_rpc_json_response_func&& callback, bool is_batch)
{
    using namespace JsonRpc;

    if (!callback)
    {
        callback = noop_json_response_callback;
    }

    auto raw_obj = rfl::json::read<rfl::Object<rfl::json::Raw>>(raw_json);
    if (!raw_obj)
    {
        auto const code = tr::rpc::classify_envelope_error(raw_obj.error().kind());
        if (code == Error::PARSE_ERROR)
        {
            callback(build_json_error_response(Error::PARSE_ERROR, tr::rpc::RequestId::null_id(), raw_obj.error().what()));
        }
        else
        {
            callback(build_json_error_response(
                Error::INVALID_REQUEST,
                tr::rpc::RequestId::null_id(),
                is_batch ? "request must be an Object"sv : "request must be an Array or Object"sv));
        }
        return;
    }

    if (!is_batch && !raw_obj->get("jsonrpc").has_value())
    {
        // Legacy Transmission 4.x RPC request (no "jsonrpc" field): convert via api-compat.
        auto serde = tr_variant_serde::json().inplace();
        if (auto legacy_var = serde.parse(raw_json); legacy_var)
        {
            tr_rpc_request_exec_impl(
                session,
                *legacy_var,
                [cb = std::move(callback)](tr_variant&& resp)
                {
                    if (!resp.has_value())
                    {
                        cb(std::string{});
                        return;
                    }
                    cb(tr_variant_serde::json().compact().to_string(resp));
                },
                false);
            return;
        }

        callback(build_json_error_response(Error::PARSE_ERROR, tr::rpc::RequestId::null_id(), serde.error_.message()));
        return;
    }

    auto env = rfl::json::read<tr::rpc::JsonRpcRequestEnvelope>(raw_json);
    if (!env)
    {
        auto const code = tr::rpc::classify_envelope_error(env.error().kind());
        callback(build_json_error_response(code, tr::rpc::RequestId::null_id(), format_envelope_error_string(env.error())));
        return;
    }

    auto params_map = tr_variant::Map{};
    if (auto const& raw_params = env->params.value(); raw_params)
    {
        auto parsed_params = rfl::json::read<rfl::Generic::Object>(raw_params->str());
        if (!parsed_params)
        {
            auto const code = tr::rpc::classify_params_error(parsed_params.error().kind());
            auto const& id = env->id.value().get();
            if (id.is_notification())
            {
                callback(std::string{});
            }
            else
            {
                callback(build_json_error_response(code, id, parsed_params.error().what()));
            }
            return;
        }

        auto params_var = rfl_generic_to_tr_variant(rfl::Generic{ std::move(*parsed_params) });
        if (auto* map_ptr = params_var.get_if<tr_variant::Map>(); map_ptr != nullptr)
        {
            params_map = std::move(*map_ptr);
        }
    }

    auto* const data = new tr_rpc_idle_data{};
    data->session = session;
    data->is_jsonrpc = true;
    data->rpc_id = env->id.value().get();
    data->json_callback = std::move(callback);

    auto const method_key = tr_quark_new(env->method.value());
    dispatch_method(session, method_key, params_map, data);
}

void exec_json_batch(tr_session* session, std::vector<rfl::json::Raw> const& requests, tr_rpc_json_response_func&& callback)
{
    using namespace JsonRpc;

    auto const n_requests = std::size(requests);
    if (n_requests == 0U)
    {
        callback(build_json_error_response(Error::INVALID_REQUEST, tr::rpc::RequestId::null_id(), "empty batch"sv));
        return;
    }

    auto responses = std::make_shared<std::vector<std::string>>(n_requests);
    auto n_responses = std::make_shared<size_t>(0U);
    auto cb = std::make_shared<tr_rpc_json_response_func>(std::move(callback));

    for (size_t i = 0U; i < n_requests; ++i)
    {
        exec_single_json_request(
            session,
            requests[i].str(),
            [responses, n_requests, n_responses, i, cb](std::string&& response)
            {
                (*responses)[i] = std::move(response);

                if (++(*n_responses) >= n_requests)
                {
                    auto non_empty = std::vector<rfl::json::Raw>{};
                    non_empty.reserve(n_requests);
                    for (auto& resp : *responses)
                    {
                        if (!std::empty(resp))
                        {
                            non_empty.emplace_back(std::move(resp));
                        }
                    }

                    (*cb)(std::empty(non_empty) ? std::string{} : rfl::json::write(non_empty));
                }
            },
            true);
    }
}

void tr_rpc_request_exec_batch(tr_session* session, tr_variant::Vector& requests, tr_rpc_response_func&& callback)
{
    auto const n_requests = std::size(requests);
    auto responses = std::make_shared<tr_variant::Vector>(n_requests);
    auto n_responses = std::make_shared<size_t>();
    auto cb = std::make_shared<tr_rpc_response_func>(std::move(callback));

    for (size_t i = 0U; i < n_requests; ++i)
    {
        tr_rpc_request_exec_impl(
            session,
            requests[i],
            [responses, n_requests, n_responses, i, cb](tr_variant&& response)
            {
                (*responses)[i] = std::move(response);

                if (++(*n_responses) >= n_requests)
                {
                    // Remove notifications from reply
                    auto const it_end = std::remove_if(
                        std::begin(*responses),
                        std::end(*responses),
                        [](auto const& r) { return !r.has_value(); });
                    responses->erase(it_end, std::end(*responses));

                    (*cb)(!std::empty(*responses) ? std::move(*responses) : tr_variant{});
                }
            },
            true);
    }
}
} // namespace

// TODO(tearfur): take `tr_variant const& request` after removing api_compat
void tr_rpc_request_exec(tr_session* session, tr_variant& request, tr_rpc_response_func&& callback)
{
    auto const lock = session->unique_lock();

    if (auto* const vec = request.get_if<tr_variant::Vector>(); vec != nullptr)
    {
        tr_rpc_request_exec_batch(session, *vec, std::move(callback));
        return;
    }

    tr_rpc_request_exec_impl(session, request, std::move(callback), false);
}

void tr_rpc_request_exec_json(tr_session* session, std::string_view request, tr_rpc_json_response_func&& callback)
{
    using namespace JsonRpc;

    if (!callback)
    {
        callback = noop_json_response_callback;
    }

    auto const lock = session->unique_lock();

    using TopLevel = std::variant<std::vector<rfl::json::Raw>, rfl::Object<rfl::json::Raw>>;
    auto parsed = rfl::json::read<TopLevel>(request);
    if (!parsed)
    {
        auto const code = tr::rpc::classify_envelope_error(parsed.error().kind());
        if (code == Error::PARSE_ERROR)
        {
            callback(build_json_error_response(Error::PARSE_ERROR, tr::rpc::RequestId::null_id(), parsed.error().what()));
        }
        else
        {
            callback(build_json_error_response(
                Error::INVALID_REQUEST,
                tr::rpc::RequestId::null_id(),
                "request must be an Array or Object"sv));
        }
        return;
    }

    if (auto const* batch = std::get_if<std::vector<rfl::json::Raw>>(&*parsed); batch != nullptr)
    {
        exec_json_batch(session, *batch, std::move(callback));
        return;
    }

    exec_single_json_request(session, request, std::move(callback), false);
}

void tr_rpc_request_exec(tr_session* session, std::string_view request, tr_rpc_response_func&& callback)
{
    if (!callback)
    {
        callback = noop_response_callback;
    }

    tr_rpc_request_exec_json(
        session,
        request,
        [cb = std::move(callback)](std::string&& response_json)
        {
            if (std::empty(response_json))
            {
                cb({});
                return;
            }
            auto serde = tr_variant_serde::json().inplace();
            if (auto parsed = serde.parse(response_json); parsed)
            {
                cb(std::move(*parsed));
                return;
            }
            cb({});
        });
}

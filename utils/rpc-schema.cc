// This file Copyright © Mnemosyne LLC.
// It may be used under GPLv2 (SPDX: GPL-2.0-only), GPLv3 (SPDX: GPL-3.0-only),
// or any future license endorsed by Mnemosyne LLC.
// License text can be found in the licenses/ folder.

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include <rfl/json.hpp>

#include "libtransmission/rpc-schema.h"
#include "libtransmission/rpcimpl.h"

namespace
{

using namespace std::literals;

void print_usage(std::ostream& out, char const* const argv0)
{
    out << "Usage: " << argv0 << " [OPTIONS]\n"
        << "\n"
        << "Emit draft 2020-12 JSON Schema directly from Transmission's C++20 RPC structs\n"
        << "(libtransmission/rpc-schema.h) via reflect-cpp.\n"
        << "\n"
        << "Options:\n"
        << "  --all              Emit full protocol schema (envelope + all 24 methods) [default]\n"
        << "  --envelope         Emit only the JSON-RPC 2.0 envelope schema\n"
        << "  --methods          Emit only the 24 RPC method contracts schema\n"
        << "  --method <name>    Emit the schema for a single RPC method (e.g. torrent_get)\n"
        << "  --compact          Emit compact JSON (default: 2-space pretty-printed)\n"
        << "  -o, --output <f>   Write JSON Schema to file <f> instead of stdout\n"
        << "  -h, --help         Show this help message\n";
}

[[nodiscard]] std::optional<std::string> emit_single_method_schema(
    std::string_view const method,
    yyjson_write_flag const flags,
    std::string const& comment)
{
    using namespace tr::rpc;

    if (method == "blocklist_update"sv)
    {
        return rfl::json::to_schema<MethodContract<EmptyObject, BlocklistUpdateResult>>(flags, comment);
    }
    if (method == "free_space"sv)
    {
        return rfl::json::to_schema<MethodContract<FreeSpaceParams, FreeSpaceResult>>(flags, comment);
    }
    if (method == "group_get"sv)
    {
        return rfl::json::to_schema<MethodContract<GroupGetParams, GroupGetResult>>(flags, comment);
    }
    if (method == "group_set"sv)
    {
        return rfl::json::to_schema<MethodContract<GroupSetParams, EmptyObject>>(flags, comment);
    }
    if (method == "port_test"sv)
    {
        return rfl::json::to_schema<MethodContract<PortTestParams, PortTestResult>>(flags, comment);
    }
    if (method == "queue_move_bottom"sv || method == "queue_move_down"sv || method == "queue_move_top"sv ||
        method == "queue_move_up"sv || method == "torrent_reannounce"sv || method == "torrent_start"sv ||
        method == "torrent_start_now"sv || method == "torrent_stop"sv || method == "torrent_verify"sv)
    {
        return rfl::json::to_schema<MethodContract<TorrentActionParams, EmptyObject>>(flags, comment);
    }
    if (method == "session_close"sv)
    {
        return rfl::json::to_schema<MethodContract<EmptyObject, EmptyObject>>(flags, comment);
    }
    if (method == "session_get"sv)
    {
        return rfl::json::to_schema<MethodContract<SessionGetParams, SessionGetResult>>(flags, comment);
    }
    if (method == "session_set"sv)
    {
        return rfl::json::to_schema<MethodContract<SessionSetParams, EmptyObject>>(flags, comment);
    }
    if (method == "session_stats"sv)
    {
        return rfl::json::to_schema<MethodContract<EmptyObject, SessionStatsResult>>(flags, comment);
    }
    if (method == "torrent_add"sv)
    {
        return rfl::json::to_schema<MethodContract<TorrentAddParams, TorrentAddResult>>(flags, comment);
    }
    if (method == "torrent_get"sv)
    {
        return rfl::json::to_schema<MethodContract<TorrentGetParams, TorrentGetResult>>(flags, comment);
    }
    if (method == "torrent_remove"sv)
    {
        return rfl::json::to_schema<MethodContract<TorrentRemoveParams, EmptyObject>>(flags, comment);
    }
    if (method == "torrent_rename_path"sv)
    {
        return rfl::json::to_schema<MethodContract<TorrentRenamePathParams, TorrentRenamePathResult>>(flags, comment);
    }
    if (method == "torrent_set"sv)
    {
        return rfl::json::to_schema<MethodContract<TorrentSetParams, EmptyObject>>(flags, comment);
    }
    if (method == "torrent_set_location"sv)
    {
        return rfl::json::to_schema<MethodContract<TorrentSetLocationParams, EmptyObject>>(flags, comment);
    }

    return std::nullopt;
}

} // namespace

int main(int argc, char** argv)
{
    enum class Mode
    {
        All,
        Envelope,
        Methods,
        SingleMethod,
    };

    auto mode = Mode::All;
    auto method_name = std::string{};
    auto output_path = std::string{};
    auto compact = false;

    for (int i = 1; i < argc; ++i)
    {
        auto const arg = std::string_view{ argv[i] };
        if (arg == "-h"sv || arg == "--help"sv)
        {
            print_usage(std::cout, argv[0]);
            return EXIT_SUCCESS;
        }
        if (arg == "--all"sv)
        {
            mode = Mode::All;
        }
        else if (arg == "--envelope"sv)
        {
            mode = Mode::Envelope;
        }
        else if (arg == "--methods"sv)
        {
            mode = Mode::Methods;
        }
        else if (arg == "--compact"sv)
        {
            compact = true;
        }
        else if (arg == "--method"sv)
        {
            if (i + 1 >= argc)
            {
                std::cerr << "error: --method requires a method name\n";
                return EXIT_FAILURE;
            }
            mode = Mode::SingleMethod;
            method_name = argv[++i];
        }
        else if (arg == "-o"sv || arg == "--output"sv)
        {
            if (i + 1 >= argc)
            {
                std::cerr << "error: " << arg << " requires a file path\n";
                return EXIT_FAILURE;
            }
            output_path = argv[++i];
        }
        else
        {
            std::cerr << "error: unknown option '" << arg << "'\n";
            print_usage(std::cerr, argv[0]);
            return EXIT_FAILURE;
        }
    }

    auto const flags = compact ? yyjson_write_flag{ 0 } : yyjson_write_flag{ YYJSON_WRITE_PRETTY_TWO_SPACES };
    auto const comment = std::string{ "Transmission JSON-RPC 2.0 protocol schema (RPC semver " } +
        std::string{ TrRpcVersionSemver } + "), generated from libtransmission/rpc-schema.h";

    auto schema = std::string{};
    switch (mode)
    {
    case Mode::All:
        schema = rfl::json::to_schema<tr::rpc::RpcProtocolSchema>(flags, comment);
        break;

    case Mode::Envelope:
        schema = rfl::json::to_schema<tr::rpc::RpcEnvelopeContract>(flags, comment);
        break;

    case Mode::Methods:
        schema = rfl::json::to_schema<tr::rpc::RpcMethodsContract>(flags, comment);
        break;

    case Mode::SingleMethod:
        if (auto res = emit_single_method_schema(method_name, flags, comment); res)
        {
            schema = std::move(*res);
        }
        else
        {
            std::cerr << "error: unknown RPC method '" << method_name << "'\n";
            return EXIT_FAILURE;
        }
        break;
    }

    if (!output_path.empty())
    {
        auto out = std::ofstream{ output_path, std::ios::binary | std::ios::trunc };
        if (!out)
        {
            std::cerr << "error: failed to open output file '" << output_path << "'\n";
            return EXIT_FAILURE;
        }
        out << schema << '\n';
        return out.good() ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    std::cout << schema << '\n';
    return EXIT_SUCCESS;
}

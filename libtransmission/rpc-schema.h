// This file Copyright © Mnemosyne LLC.
// It may be used under GPLv2 (SPDX: GPL-2.0-only), GPLv3 (SPDX: GPL-3.0-only),
// or any future license endorsed by Mnemosyne LLC.
// License text can be found in the licenses/ folder.

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <rfl.hpp>
#include <rfl/json.hpp>

#include "libtransmission/rpcimpl.h"

namespace tr::rpc
{

// --- JSON-RPC 2.0 Error Classification via rfl::ErrorKind ---

[[nodiscard]] constexpr JsonRpc::Error::Code classify_envelope_error(rfl::ErrorKind const kind) noexcept
{
    switch (kind)
    {
    case rfl::ErrorKind::syntax_error:
        return JsonRpc::Error::PARSE_ERROR;

    case rfl::ErrorKind::missing_field:
    case rfl::ErrorKind::unknown_field:
    case rfl::ErrorKind::duplicate_field:
    case rfl::ErrorKind::invalid_length:
    case rfl::ErrorKind::invalid_type:
    case rfl::ErrorKind::invalid_value:
    case rfl::ErrorKind::constraint_violation:
        return JsonRpc::Error::INVALID_REQUEST;

    default:
        return JsonRpc::Error::INTERNAL_ERROR;
    }
}

[[nodiscard]] constexpr JsonRpc::Error::Code classify_params_error(rfl::ErrorKind const kind) noexcept
{
    switch (kind)
    {
    case rfl::ErrorKind::syntax_error:
        return JsonRpc::Error::PARSE_ERROR;

    case rfl::ErrorKind::missing_field:
    case rfl::ErrorKind::unknown_field:
    case rfl::ErrorKind::duplicate_field:
    case rfl::ErrorKind::invalid_length:
    case rfl::ErrorKind::invalid_type:
    case rfl::ErrorKind::invalid_value:
    case rfl::ErrorKind::constraint_violation:
        return JsonRpc::Error::INVALID_PARAMS;

    default:
        return JsonRpc::Error::INTERNAL_ERROR;
    }
}

// --- JSON-RPC 2.0 Identifier ---

/// JSON-RPC 2.0 request/response identifier (String, Number, or Null).
/// Distinguishes an omitted `id` member (notification) from an explicit `"id": null`.
struct RequestId
{
    using ValueType = std::variant<std::nullptr_t, int64_t, double, std::string>;

    bool present = false;
    ValueType value = nullptr;

    [[nodiscard]] static RequestId omitted() noexcept
    {
        return RequestId{ .present = false, .value = nullptr };
    }

    [[nodiscard]] static RequestId null_id() noexcept
    {
        return RequestId{ .present = true, .value = nullptr };
    }

    [[nodiscard]] static RequestId from_int(int64_t const v) noexcept
    {
        return RequestId{ .present = true, .value = v };
    }

    [[nodiscard]] static RequestId from_double(double const v) noexcept
    {
        return RequestId{ .present = true, .value = v };
    }

    [[nodiscard]] static RequestId from_string(std::string v) noexcept
    {
        return RequestId{ .present = true, .value = std::move(v) };
    }

    [[nodiscard]] constexpr bool is_notification() const noexcept
    {
        return !present;
    }

    bool operator==(RequestId const&) const = default;
};

// --- JSON-RPC 2.0 Envelope (using rfl::json::Raw for deferred subtrees) ---

struct JsonRpcRequestEnvelope
{
    rfl::Description<"JSON-RPC protocol version; must be \"2.0\".", rfl::Literal<"2.0">> jsonrpc{};
    rfl::Description<"Name of the Transmission RPC method to invoke.", std::string> method{};
    rfl::Description<"Optional structured parameter object for the method.", std::optional<rfl::json::Raw>> params{};
    rfl::Description<"Optional request identifier (string, number, or null). Omit for notifications.", rfl::DefaultVal<RequestId>>
        id{};
};

struct JsonRpcErrorData
{
    rfl::Description<"Additional human-readable diagnostic detail.", std::optional<std::string>> error_string{};
    rfl::Description<"Optional method-specific partial result payload.", std::optional<rfl::json::Raw>> result{};
};

struct JsonRpcErrorObject
{
    rfl::Description<"Numeric JSON-RPC 2.0 or Transmission application error code.", int16_t> code{};
    rfl::Description<"Short human-readable description of the error code.", std::string> message{};
    rfl::Description<"Optional Transmission-specific error metadata.", std::optional<JsonRpcErrorData>> data{};
};

struct JsonRpcSuccessResponse
{
    rfl::Description<"JSON-RPC protocol version; always \"2.0\".", rfl::Literal<"2.0">> jsonrpc{};
    rfl::Description<"Method-specific result object.", rfl::json::Raw> result{ rfl::json::Raw{ "{}" } };
    rfl::Description<"Echoed request identifier.", RequestId> id = RequestId::null_id();
};

struct JsonRpcErrorResponse
{
    rfl::Description<"JSON-RPC protocol version; always \"2.0\".", rfl::Literal<"2.0">> jsonrpc{};
    rfl::Description<"Error object describing why the request failed.", JsonRpcErrorObject> error{};
    rfl::Description<"Echoed request identifier, or null if it could not be determined.", RequestId> id = RequestId::null_id();
};

// --- Shared RPC Primitives ---

struct EmptyObject
{
};

/// Torrent selector per docs/rpc-spec.md §3.1:
/// an integer torrent id, a string ("recently_active" or SHA1 hash),
/// or an array of integer ids and/or SHA1 hash strings.
using TorrentIdElement = std::variant<int64_t, std::string>;
using TorrentIds = std::variant<int64_t, std::string, std::vector<TorrentIdElement>>;

using IpProtocol = rfl::Literal<"ipv4", "ipv6">;
using EncryptionMode = rfl::Literal<"required", "preferred", "allowed">;
using TransportProtocol = rfl::Literal<"utp", "tcp">;
using TorrentGetFormat = rfl::Literal<"objects", "object", "table">;

// --- §3.1 Torrent Action & §4.6 Queue Movement Requests ---

struct TorrentActionParams
{
    rfl::Description<"Target torrent selector; omits to target all torrents.", std::optional<TorrentIds>> ids{};
};

// --- §3.2 Torrent Mutator: torrent_set ---

struct TorrentSetParams
{
    rfl::Description<"Target torrent selector; omits to target all torrents.", std::optional<TorrentIds>> ids{};
    rfl::Description<"Torrent bandwidth priority (tr_priority_t).", std::optional<int64_t>> bandwidth_priority{};
    rfl::Description<"Maximum download speed in kB/s.", std::optional<int64_t>> download_limit{};
    rfl::Description<"True if download_limit is honored.", std::optional<bool>> download_limited{};
    rfl::Description<"Indices of files to not download; empty array means all files.", std::optional<std::vector<int64_t>>>
        files_unwanted{};
    rfl::Description<"Indices of files to download; empty array means all files.", std::optional<std::vector<int64_t>>>
        files_wanted{};
    rfl::Description<"Name of this torrent's bandwidth group.", std::optional<std::string>> group{};
    rfl::Description<"True if session speed limits are honored.", std::optional<bool>> honors_session_limits{};
    rfl::Description<"Array of string labels.", std::optional<std::vector<std::string>>> labels{};
    rfl::Description<"Maximum number of peers.", std::optional<int64_t>> peer_limit{};
    rfl::Description<"Indices of high-priority files; empty array means all files.", std::optional<std::vector<int64_t>>>
        priority_high{};
    rfl::Description<"Indices of low-priority files; empty array means all files.", std::optional<std::vector<int64_t>>>
        priority_low{};
    rfl::Description<"Indices of normal-priority files; empty array means all files.", std::optional<std::vector<int64_t>>>
        priority_normal{};
    rfl::Description<"Position of this torrent in its queue [0..n).", std::optional<int64_t>> queue_position{};
    rfl::Description<"Torrent-level seeding inactivity limit in minutes.", std::optional<int64_t>> seed_idle_limit{};
    rfl::Description<"Seeding inactivity mode (tr_idlelimit).", std::optional<int64_t>> seed_idle_mode{};
    rfl::Description<"Torrent-level seeding ratio limit.", std::optional<double>> seed_ratio_limit{};
    rfl::Description<"Seeding ratio mode (tr_ratiolimit).", std::optional<int64_t>> seed_ratio_mode{};
    rfl::Description<"Download torrent pieces sequentially.", std::optional<bool>> sequential_download{};
    rfl::Description<"Start piece index when sequential_download is enabled.", std::optional<int64_t>>
        sequential_download_from_piece{};
    rfl::Deprecated<"Use tracker_list instead.", "Announce URLs to add.", std::optional<std::vector<std::string>>>
        tracker_add{};
    rfl::Description<"Announce URLs separated by newlines, blank lines between tiers.", std::optional<std::string>>
        tracker_list{};
    rfl::Deprecated<"Use tracker_list instead.", "Tracker IDs to remove.", std::optional<std::vector<int64_t>>>
        tracker_remove{};
    rfl::Deprecated<
        "Use tracker_list instead.",
        "Alternating [tracker_id, announce_url] pairs to replace.",
        std::optional<std::vector<std::variant<int64_t, std::string>>>>
        tracker_replace{};
    rfl::Description<"Maximum upload speed in kB/s.", std::optional<int64_t>> upload_limit{};
    rfl::Description<"True if upload_limit is honored.", std::optional<bool>> upload_limited{};
};

// --- §3.3 Torrent Accessor: torrent_get ---

struct TorrentFileInfo
{
    int64_t begin_piece = 0;
    int64_t bytes_completed = 0;
    int64_t end_piece = 0;
    int64_t length = 0;
    std::string name{};
};

struct TorrentFileStats
{
    int64_t bytes_completed = 0;
    int64_t priority = 0;
    bool wanted = false;
};

struct TorrentWebseedEx
{
    std::string url{};
    bool is_downloading = false;
    int64_t download_bytes_per_second = 0;
};

struct TorrentTrackerInfo
{
    std::string announce{};
    int64_t id = 0;
    std::string scrape{};
    std::string sitename{};
    int64_t tier = 0;
};

struct TorrentTrackerStats
{
    std::string announce{};
    int64_t announce_state = 0;
    int64_t download_count = 0;
    int64_t downloader_count = 0;
    bool has_announced = false;
    bool has_scraped = false;
    std::string host{};
    int64_t id = 0;
    bool is_backup = false;
    int64_t last_announce_peer_count = 0;
    std::string last_announce_result{};
    int64_t last_announce_start_time = 0;
    bool last_announce_succeeded = false;
    int64_t last_announce_time = 0;
    bool last_announce_timed_out = false;
    std::string last_scrape_result{};
    int64_t last_scrape_start_time = 0;
    bool last_scrape_succeeded = false;
    int64_t last_scrape_time = 0;
    bool last_scrape_timed_out = false;
    int64_t leecher_count = 0;
    int64_t next_announce_time = 0;
    int64_t next_scrape_time = 0;
    std::string scrape{};
    int64_t scrape_state = 0;
    int64_t seeder_count = 0;
    std::string sitename{};
    int64_t tier = 0;
};

struct TorrentPeerInfo
{
    std::string address{};
    bool client_is_choked = false;
    bool client_is_interested = false;
    std::string client_name{};
    std::string peer_id{};
    std::string flag_str{};
    bool is_downloading_from = false;
    bool is_encrypted = false;
    bool is_incoming = false;
    bool is_utp = false;
    bool is_uploading_to = false;
    bool supports_holepunch = false;
    bool peer_is_choked = false;
    bool peer_is_interested = false;
    int64_t port = 0;
    double progress = 0.0;
    int64_t rate_to_client = 0;
    int64_t rate_to_peer = 0;
    int64_t bytes_to_peer = 0;
    int64_t bytes_to_client = 0;
};

struct TorrentPeersFrom
{
    int64_t from_cache = 0;
    int64_t from_dht = 0;
    int64_t from_holepunch = 0;
    int64_t from_incoming = 0;
    int64_t from_lpd = 0;
    int64_t from_ltep = 0;
    int64_t from_pex = 0;
    int64_t from_tracker = 0;
};

struct TorrentGetParams
{
    rfl::Description<"Optional target torrent selector; omits to query all torrents.", std::optional<TorrentIds>> ids{};
    rfl::Description<"Required array of torrent field names to return.", std::optional<std::vector<std::string>>> fields{};
    rfl::Description<"Response layout: \"objects\" (default) or \"table\".", std::optional<TorrentGetFormat>> format{};
};

struct TorrentInfo
{
    std::optional<int64_t> activity_date{};
    std::optional<int64_t> added_date{};
    std::optional<std::vector<int64_t>> availability{};
    std::optional<int64_t> bandwidth_priority{};
    std::optional<std::vector<int64_t>> bytes_completed{};
    std::optional<std::string> comment{};
    std::optional<int64_t> corrupt_ever{};
    std::optional<std::string> creator{};
    std::optional<int64_t> date_created{};
    std::optional<int64_t> desired_available{};
    std::optional<int64_t> done_date{};
    std::optional<std::string> download_dir{};
    std::optional<int64_t> download_limit{};
    std::optional<bool> download_limited{};
    std::optional<int64_t> downloaded_ever{};
    std::optional<int64_t> edit_date{};
    std::optional<int64_t> error{};
    std::optional<std::string> error_string{};
    std::optional<int64_t> eta{};
    std::optional<int64_t> eta_idle{};
    std::optional<int64_t> file_count{};
    std::optional<std::vector<TorrentFileStats>> file_stats{};
    std::optional<std::vector<TorrentFileInfo>> files{};
    std::optional<std::string> group{};
    std::optional<std::string> hash_string{};
    std::optional<int64_t> have_unchecked{};
    std::optional<int64_t> have_valid{};
    std::optional<bool> honors_session_limits{};
    std::optional<int64_t> id{};
    std::optional<bool> is_finished{};
    std::optional<bool> is_private{};
    std::optional<bool> is_stalled{};
    std::optional<std::vector<std::string>> labels{};
    std::optional<int64_t> left_until_done{};
    std::optional<std::string> magnet_link{};
    rfl::Deprecated<"Never worked; do not use.", "Next manual announce time.", std::optional<int64_t>> manual_announce_time{};
    std::optional<int64_t> max_connected_peers{};
    std::optional<double> metadata_percent_complete{};
    std::optional<std::string> name{};
    std::optional<int64_t> peer_limit{};
    std::optional<std::vector<TorrentPeerInfo>> peers{};
    std::optional<int64_t> peers_connected{};
    std::optional<TorrentPeersFrom> peers_from{};
    std::optional<int64_t> peers_getting_from_us{};
    std::optional<int64_t> peers_sending_to_us{};
    std::optional<double> percent_complete{};
    std::optional<double> percent_done{};
    std::optional<int64_t> piece_count{};
    std::optional<int64_t> piece_size{};
    std::optional<std::string> pieces{};
    std::optional<std::string> primary_mime_type{};
    std::optional<std::vector<int64_t>> priorities{};
    std::optional<int64_t> queue_position{};
    std::optional<int64_t> rate_download{};
    std::optional<int64_t> rate_upload{};
    std::optional<double> recheck_progress{};
    std::optional<int64_t> seconds_downloading{};
    std::optional<int64_t> seconds_seeding{};
    std::optional<int64_t> seed_idle_limit{};
    std::optional<int64_t> seed_idle_mode{};
    std::optional<double> seed_ratio_limit{};
    std::optional<int64_t> seed_ratio_mode{};
    std::optional<bool> sequential_download{};
    std::optional<int64_t> sequential_download_from_piece{};
    std::optional<int64_t> size_when_done{};
    std::optional<std::string> source{};
    std::optional<int64_t> start_date{};
    std::optional<int64_t> status{};
    std::optional<std::string> torrent_file{};
    std::optional<int64_t> total_size{};
    std::optional<std::string> tracker_list{};
    std::optional<std::vector<TorrentTrackerStats>> tracker_stats{};
    std::optional<std::vector<TorrentTrackerInfo>> trackers{};
    std::optional<int64_t> upload_limit{};
    std::optional<bool> upload_limited{};
    std::optional<double> upload_ratio{};
    std::optional<int64_t> uploaded_ever{};
    std::optional<std::vector<bool>> wanted{};
    rfl::Deprecated<"Use webseeds_ex instead.", "Webseed URLs.", std::optional<std::vector<std::string>>> webseeds{};
    std::optional<std::vector<TorrentWebseedEx>> webseeds_ex{};
    std::optional<int64_t> webseeds_sending_to_us{};
};

struct TorrentGetResult
{
    rfl::Description<
        "Array of torrent objects (format=\"objects\") or table rows (format=\"table\").",
        std::variant<std::vector<TorrentInfo>, std::vector<std::vector<rfl::Generic>>>>
        torrents{};
    rfl::Description<"Recently removed torrent IDs when ids=\"recently_active\".", std::optional<std::vector<int64_t>>>
        removed{};
};

// --- §3.4 Adding a Torrent: torrent_add ---

struct TorrentAddParams
{
    rfl::Description<"HTTP cookies in NAME=CONTENTS; format.", std::optional<std::string>> cookies{};
    rfl::Description<"Absolute path to download the torrent to.", std::optional<std::string>> download_dir{};
    rfl::Description<"Filename, magnet URI, or HTTP(S)/FTP URL of the .torrent.", std::optional<std::string>> filename{};
    rfl::Description<"Array of string labels.", std::optional<std::vector<std::string>>> labels{};
    rfl::Description<"Base64-encoded .torrent file contents.", std::optional<std::string>> metainfo{};
    rfl::Description<"If true, add the torrent in paused state.", std::optional<bool>> paused{};
    rfl::Description<"Maximum number of peers.", std::optional<int64_t>> peer_limit{};
    rfl::Description<"Torrent bandwidth priority (tr_priority_t).", std::optional<int64_t>> bandwidth_priority{};
    rfl::Description<"Indices of files to download.", std::optional<std::vector<int64_t>>> files_wanted{};
    rfl::Description<"Indices of files to not download.", std::optional<std::vector<int64_t>>> files_unwanted{};
    rfl::Description<"Indices of high-priority files.", std::optional<std::vector<int64_t>>> priority_high{};
    rfl::Description<"Indices of low-priority files.", std::optional<std::vector<int64_t>>> priority_low{};
    rfl::Description<"Indices of normal-priority files.", std::optional<std::vector<int64_t>>> priority_normal{};
    rfl::Description<"Download torrent pieces sequentially.", std::optional<bool>> sequential_download{};
    rfl::Description<"Start piece index when sequential_download is enabled.", std::optional<int64_t>>
        sequential_download_from_piece{};
};

struct TorrentAddedSummary
{
    int64_t id = 0;
    std::string name{};
    std::string hash_string{};
};

struct TorrentAddResult
{
    rfl::Description<"Summary of the newly added torrent.", std::optional<TorrentAddedSummary>> torrent_added{};
    rfl::Description<"Summary of the existing duplicate torrent.", std::optional<TorrentAddedSummary>> torrent_duplicate{};
};

// --- §3.5 Removing a Torrent: torrent_remove ---

struct TorrentRemoveParams
{
    rfl::Description<"Target torrent selector; omits to target all torrents.", std::optional<TorrentIds>> ids{};
    rfl::Description<"If true, also delete local downloaded data (default: false).", std::optional<bool>> delete_local_data{};
};

// --- §3.6 Moving a Torrent: torrent_set_location ---

struct TorrentSetLocationParams
{
    rfl::Description<"Target torrent selector; omits to target all torrents.", std::optional<TorrentIds>> ids{};
    rfl::Description<"New absolute directory path for the torrent's data.", std::string> location{};
    rfl::Description<"If true, move existing files; otherwise search location for files.", std::optional<bool>> move{};
};

// --- §3.7 Renaming a Torrent's Path: torrent_rename_path ---

struct TorrentRenamePathParams
{
    rfl::Description<"Target torrent selector (must resolve to exactly 1 torrent).", std::optional<TorrentIds>> ids{};
    rfl::Description<"Existing file or folder path inside the torrent.", std::optional<std::string>> path{};
    rfl::Description<"New name for the file or folder.", std::optional<std::string>> name{};
};

struct TorrentRenamePathResult
{
    rfl::Description<"Torrent ID.", int64_t> id = 0;
    rfl::Description<"Original path requested for rename.", std::string> path{};
    rfl::Description<"New name requested.", std::string> name{};
};

// --- §4.1 Session Get / Set ---

struct SessionUnits
{
    int64_t memory_bytes = 0;
    std::vector<std::string> memory_units{};
    int64_t size_bytes = 0;
    std::vector<std::string> size_units{};
    int64_t speed_bytes = 0;
    std::vector<std::string> speed_units{};
};

struct SessionGetParams
{
    rfl::Description<"Optional list of session field names to return; omits to return all.", std::optional<std::vector<std::string>>>
        fields{};
};

struct SessionSetParams
{
    std::optional<int64_t> alt_speed_down{};
    std::optional<bool> alt_speed_enabled{};
    std::optional<int64_t> alt_speed_time_begin{};
    std::optional<int64_t> alt_speed_time_day{};
    std::optional<bool> alt_speed_time_enabled{};
    std::optional<int64_t> alt_speed_time_end{};
    std::optional<int64_t> alt_speed_up{};
    std::optional<bool> anti_brute_force_enabled{};
    std::optional<int64_t> anti_brute_force_threshold{};
    std::optional<bool> blocklist_enabled{};
    std::optional<std::string> blocklist_url{};
    rfl::Deprecated<"Will be removed in Transmission 5.0.0.", "Unused memory cache size in MiB.", std::optional<int64_t>>
        cache_size_mib{};
    std::optional<std::string> default_trackers{};
    std::optional<bool> dht_enabled{};
    std::optional<std::string> download_dir{};
    std::optional<bool> download_queue_enabled{};
    std::optional<int64_t> download_queue_size{};
    std::optional<EncryptionMode> encryption{};
    std::optional<int64_t> idle_seeding_limit{};
    std::optional<bool> idle_seeding_limit_enabled{};
    std::optional<std::string> incomplete_dir{};
    std::optional<bool> incomplete_dir_enabled{};
    std::optional<bool> lpd_enabled{};
    std::optional<int64_t> peer_limit_global{};
    std::optional<int64_t> peer_limit_per_torrent{};
    std::optional<int64_t> peer_port{};
    std::optional<bool> peer_port_random_on_start{};
    std::optional<bool> pex_enabled{};
    std::optional<bool> port_forwarding_enabled{};
    std::optional<std::vector<TransportProtocol>> preferred_transports{};
    std::optional<bool> queue_stalled_enabled{};
    std::optional<int64_t> queue_stalled_minutes{};
    std::optional<bool> rename_partial_files{};
    std::optional<int64_t> reqq{};
    std::optional<bool> script_torrent_added_enabled{};
    std::optional<std::string> script_torrent_added_filename{};
    std::optional<bool> script_torrent_done_enabled{};
    std::optional<std::string> script_torrent_done_filename{};
    std::optional<bool> script_torrent_done_seeding_enabled{};
    std::optional<std::string> script_torrent_done_seeding_filename{};
    std::optional<bool> seed_queue_enabled{};
    std::optional<int64_t> seed_queue_size{};
    std::optional<double> seed_ratio_limit{};
    std::optional<bool> seed_ratio_limited{};
    std::optional<bool> sequential_download{};
    std::optional<int64_t> speed_limit_down{};
    std::optional<bool> speed_limit_down_enabled{};
    std::optional<int64_t> speed_limit_up{};
    std::optional<bool> speed_limit_up_enabled{};
    std::optional<bool> start_added_torrents{};
    std::optional<bool> trash_original_torrent_files{};
    rfl::Deprecated<"Use preferred_transports instead.", "Enable µTP transport.", std::optional<bool>> utp_enabled{};
};

struct SessionGetResult
{
    rfl::Flatten<SessionSetParams> mutable_settings{};
    std::optional<int64_t> blocklist_size{};
    std::optional<std::string> config_dir{};
    rfl::Deprecated<"Use the free_space method instead.", "Free space in download_dir in bytes.", std::optional<int64_t>>
        download_dir_free_space{};
    rfl::Deprecated<"Use rpc_version_semver instead.", "Integer RPC API version.", std::optional<int64_t>> rpc_version{};
    rfl::Deprecated<"Use rpc_version_semver instead.", "Minimum supported integer RPC API version.", std::optional<int64_t>>
        rpc_version_minimum{};
    std::optional<std::string> rpc_version_semver{};
    std::optional<std::string> session_id{};
    rfl::Deprecated<"Use preferred_transports instead.", "Whether TCP transport is enabled.", std::optional<bool>>
        tcp_enabled{};
    std::optional<SessionUnits> units{};
    std::optional<std::string> version{};
};

// --- §4.2 Session Statistics: session_stats ---

struct SessionTransferStats
{
    rfl::Description<"Total bytes downloaded.", int64_t> downloaded_bytes = 0;
    rfl::Description<"Total number of torrents added.", int64_t> files_added = 0;
    rfl::Description<"Total seconds Transmission has been active.", int64_t> seconds_active = 0;
    rfl::Description<"Number of sessions counted.", int64_t> session_count = 0;
    rfl::Description<"Total bytes uploaded.", int64_t> uploaded_bytes = 0;
};

struct SessionStatsResult
{
    rfl::Description<"Number of active (running) torrents.", int64_t> active_torrent_count = 0;
    rfl::Description<"Cumulative statistics across all sessions.", SessionTransferStats> cumulative_stats{};
    rfl::Description<"Statistics for the current session.", SessionTransferStats> current_stats{};
    rfl::Description<"Current global download speed in bytes/sec.", int64_t> download_speed = 0;
    rfl::Description<"Number of paused torrents.", int64_t> paused_torrent_count = 0;
    rfl::Description<"Total number of torrents.", int64_t> torrent_count = 0;
    rfl::Description<"Current global upload speed in bytes/sec.", int64_t> upload_speed = 0;
};

// --- §4.3 Blocklist: blocklist_update ---

struct BlocklistUpdateResult
{
    rfl::Description<"Number of rules loaded in the updated blocklist.", int64_t> blocklist_size = 0;
};

// --- §4.4 Port Checking: port_test ---

struct PortTestParams
{
    rfl::Description<"Optional IP protocol version (\"ipv4\" or \"ipv6\").", std::optional<IpProtocol>> ip_protocol{};
};

struct PortTestResult
{
    rfl::Description<"IP protocol used for the check, if determined.", std::optional<IpProtocol>> ip_protocol{};
    rfl::Description<"True if the incoming peer port is open.", bool> port_is_open = false;
};

// --- §4.7 Free Space: free_space ---

struct FreeSpaceParams
{
    rfl::Description<"Absolute directory path to query.", std::string> path{};
};

struct FreeSpaceResult
{
    rfl::Description<"Directory path queried.", std::string> path{};
    rfl::Description<"Available free space in bytes, or -1 on error.", int64_t> size_bytes = -1;
    rfl::Description<"Total filesystem capacity in bytes, or -1 on error.", int64_t> total_size = -1;
};

// --- §4.8 Bandwidth Groups: group_get & group_set ---

struct GroupGetParams
{
    rfl::Description<
        "Optional bandwidth group name or list of names; omits to return all groups.",
        std::optional<std::variant<std::string, std::vector<std::string>>>>
        name{};
};

struct BandwidthGroupInfo
{
    rfl::Description<"True if session speed limits are honored.", bool> honors_session_limits = false;
    rfl::Description<"Bandwidth group name.", std::string> name{};
    rfl::Description<"Maximum download speed in kB/s.", int64_t> speed_limit_down = 0;
    rfl::Description<"True if speed_limit_down is enabled.", bool> speed_limit_down_enabled = false;
    rfl::Description<"Maximum upload speed in kB/s.", int64_t> speed_limit_up = 0;
    rfl::Description<"True if speed_limit_up is enabled.", bool> speed_limit_up_enabled = false;
};

struct GroupGetResult
{
    rfl::Description<"List of bandwidth group descriptions.", std::vector<BandwidthGroupInfo>> group{};
};

struct GroupSetParams
{
    rfl::Description<"Bandwidth group name.", std::string> name{};
    rfl::Description<"True if session speed limits are honored.", std::optional<bool>> honors_session_limits{};
    rfl::Description<"Maximum download speed in kB/s.", std::optional<int64_t>> speed_limit_down{};
    rfl::Description<"True if speed_limit_down is enabled.", std::optional<bool>> speed_limit_down_enabled{};
    rfl::Description<"Maximum upload speed in kB/s.", std::optional<int64_t>> speed_limit_up{};
    rfl::Description<"True if speed_limit_up is enabled.", std::optional<bool>> speed_limit_up_enabled{};
};

// --- Full RPC Protocol Contract (Single Source of Truth for JSON Schema) ---

template<typename ParamsType, typename ResultType>
struct MethodContract
{
    ParamsType params{};
    ResultType result{};
};

struct RpcEnvelopeContract
{
    JsonRpcRequestEnvelope request{};
    JsonRpcSuccessResponse success_response{};
    JsonRpcErrorResponse error_response{};
};

struct RpcMethodsContract
{
    MethodContract<EmptyObject, BlocklistUpdateResult> blocklist_update{};
    MethodContract<FreeSpaceParams, FreeSpaceResult> free_space{};
    MethodContract<GroupGetParams, GroupGetResult> group_get{};
    MethodContract<GroupSetParams, EmptyObject> group_set{};
    MethodContract<PortTestParams, PortTestResult> port_test{};
    MethodContract<TorrentActionParams, EmptyObject> queue_move_bottom{};
    MethodContract<TorrentActionParams, EmptyObject> queue_move_down{};
    MethodContract<TorrentActionParams, EmptyObject> queue_move_top{};
    MethodContract<TorrentActionParams, EmptyObject> queue_move_up{};
    MethodContract<EmptyObject, EmptyObject> session_close{};
    MethodContract<SessionGetParams, SessionGetResult> session_get{};
    MethodContract<SessionSetParams, EmptyObject> session_set{};
    MethodContract<EmptyObject, SessionStatsResult> session_stats{};
    MethodContract<TorrentAddParams, TorrentAddResult> torrent_add{};
    MethodContract<TorrentGetParams, TorrentGetResult> torrent_get{};
    MethodContract<TorrentActionParams, EmptyObject> torrent_reannounce{};
    MethodContract<TorrentRemoveParams, EmptyObject> torrent_remove{};
    MethodContract<TorrentRenamePathParams, TorrentRenamePathResult> torrent_rename_path{};
    MethodContract<TorrentSetParams, EmptyObject> torrent_set{};
    MethodContract<TorrentSetLocationParams, EmptyObject> torrent_set_location{};
    MethodContract<TorrentActionParams, EmptyObject> torrent_start{};
    MethodContract<TorrentActionParams, EmptyObject> torrent_start_now{};
    MethodContract<TorrentActionParams, EmptyObject> torrent_stop{};
    MethodContract<TorrentActionParams, EmptyObject> torrent_verify{};
};

struct RpcProtocolSchema
{
    rfl::Description<"JSON-RPC 2.0 envelope request and response shapes.", RpcEnvelopeContract> envelope{};
    rfl::Description<"All 24 Transmission JSON-RPC 2.0 methods (params and result schemas).", RpcMethodsContract> methods{};
};

} // namespace tr::rpc

// --- reflect-cpp Parser specialization for tr::rpc::RequestId ---

namespace rfl::parsing
{

template<class R, class W, class ProcessorsType>
    requires AreReaderAndWriter<R, W, tr::rpc::RequestId>
struct Parser<R, W, tr::rpc::RequestId, ProcessorsType>
{
    using InputVarType = typename R::InputVarType;
    using ParentType = Parent<W>;

    static Result<tr::rpc::RequestId> read(R const& _r, InputVarType const& _var) noexcept
    {
        if (_r.is_empty(_var))
        {
            return tr::rpc::RequestId::null_id();
        }
        if (auto i = _r.template to_basic_type<int64_t>(_var); i)
        {
            return tr::rpc::RequestId::from_int(*i);
        }
        if (auto d = _r.template to_basic_type<double>(_var); d)
        {
            return tr::rpc::RequestId::from_double(*d);
        }
        if (auto s = _r.template to_basic_type<std::string>(_var); s)
        {
            return tr::rpc::RequestId::from_string(std::move(*s));
        }
        return error("id type must be String, Number, or Null", ErrorKind::invalid_type);
    }

    template<class P>
    static void write(W const& _w, tr::rpc::RequestId const& _id, P const& _parent)
    {
        std::visit(
            [&](auto const& v)
            {
                using V = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<V, std::nullptr_t>)
                {
                    ParentType::add_null(_w, _parent);
                }
                else
                {
                    ParentType::add_value(_w, v, _parent);
                }
            },
            _id.value);
    }

    static schema::Type to_schema(std::map<std::string, schema::Type>* _definitions)
    {
        return Parser<R, W, std::optional<std::variant<std::string, int64_t, double>>, ProcessorsType>::to_schema(
            _definitions);
    }
};

} // namespace rfl::parsing

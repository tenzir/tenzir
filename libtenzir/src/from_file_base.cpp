//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/file.hpp>
#include <tenzir/format_utils.hpp>
#include <tenzir/from_file_base.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/session.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/filesystem/api.h>
#include <arrow/util/future.h>
#include <caf/actor_from_state.hpp>

#ifdef ARROW_S3
#  include <arrow/filesystem/s3fs.h>
#  include <aws/s3/S3Client.h>
#  include <aws/s3/S3ClientConfiguration.h>
#  include <aws/s3/model/DeleteObjectRequest.h>
#endif

#ifdef ARROW_AZURE
#  include <arrow/filesystem/azurefs.h>
#  include <azure/storage/blobs.hpp>
#endif

#include <utility>

namespace tenzir {

namespace {

/// How long to wait between file queries when using `from_file watch=true`.
constexpr auto watch_pause = std::chrono::seconds{10};

/// The maximum number of concurrent pipelines for `from_file`.
constexpr auto max_jobs = 10;

/// Add a callback to an `arrow::Future` that shall run inside an actor context.
template <class T, class F>
void add_actor_callback(caf::scheduled_actor* self,
                        const arrow::Future<T>& future, F&& f) {
  using result_type
    = std::conditional_t<std::same_as<T, arrow::internal::Empty>, arrow::Status,
                         arrow::Result<T>>;
  future.AddCallback(
    [self, weak = caf::weak_actor_ptr{self->ctrl(), caf::add_ref},
     f = std::forward<F>(f)](const result_type& result) mutable {
      if (auto strong = weak.lock()) {
        self->schedule_fn([f = std::move(f), result]() mutable -> void {
          return std::invoke(std::move(f), std::move(result));
        });
      }
    });
}

/// Iterate asynchronously over an `arrow::fs::FileInfoGenerator`.
template <class F>
void iterate_files(caf::scheduled_actor* self, arrow::fs::FileInfoGenerator gen,
                   F&& f) {
  auto future = gen();
  add_actor_callback(self, future,
                     [self, gen = std::move(gen), f = std::forward<F>(f)](
                       arrow::Result<arrow::fs::FileInfoVector> infos) {
                       auto more = infos.ok() and not infos->empty();
                       f(std::move(infos));
                       if (more) {
                         iterate_files(self, std::move(gen), std::move(f));
                       }
                     });
}

auto make_operator(const operator_factory_plugin& plugin, location location,
                   session ctx) -> failure_or<operator_ptr> {
  auto inv = invocation_for_plugin(plugin, location);
  return plugin.make({std::move(inv.op), std::move(inv.args)}, ctx);
}

constexpr auto extract_root_path(const glob& glob_, const std::string& expanded)
  -> std::string {
  if (not glob_.empty()) {
    if (const auto* prefix = try_as<std::string>(glob_[0])) {
      // Use the whole path if we don't do any actual globbing.
      if (glob_.size() == 1) {
        auto result = *prefix;
        // Preserve trailing slash semantics if present in original path
        if (expanded.ends_with('/') && not result.ends_with('/')) {
          result += '/';
        }
        return result;
      }
      // Otherwise use the last directory before the globbing starts.
      const auto slash = prefix->rfind("/");
      if (slash != std::string::npos) {
        // The slash itself should be included.
        return prefix->substr(0, slash + 1);
      }
    }
  }
  return "/";
}

#ifdef ARROW_S3
/// Parse S3 path into bucket and key.
/// Arrow paths look like "bucket/path/to/file.txt".
auto parse_s3_path(std::string_view path)
  -> std::pair<std::string, std::string> {
  auto slash = path.find('/');
  if (slash == std::string_view::npos) {
    return {std::string{path}, ""};
  }
  return {std::string{path.substr(0, slash)},
          std::string{path.substr(slash + 1)}};
}

/// Delete an S3 object directly using the AWS SDK, bypassing Arrow's DeleteFile
/// which creates 0-sized directory markers.
auto delete_file_s3(arrow::fs::S3FileSystem* fs, const std::string& path)
  -> arrow::Status {
  const auto& options = fs->options();
  auto config = Aws::S3::S3ClientConfiguration{};
  config.region = options.region;
  if (not options.endpoint_override.empty()) {
    config.endpointOverride = options.endpoint_override;
    // Use path-style addressing for custom endpoints (e.g., Minio).
    config.useVirtualAddressing = options.force_virtual_addressing;
  }
  config.scheme = options.scheme == "http" ? Aws::Http::Scheme::HTTP
                                           : Aws::Http::Scheme::HTTPS;
  // Get credentials from Arrow's credential provider.
  auto creds_provider = options.credentials_provider;
  auto [bucket, key] = parse_s3_path(path);
  // Arrow initializes the AWS SDK, so we can rely on that.
  auto client = Aws::S3::S3Client{creds_provider, nullptr, config};
  auto request = Aws::S3::Model::DeleteObjectRequest{};
  request.SetBucket(bucket);
  request.SetKey(key);
  auto outcome = client.DeleteObject(request);
  if (not outcome.IsSuccess()) {
    const auto& error = outcome.GetError();
    return arrow::Status::IOError("failed to delete S3 object: ",
                                  error.GetMessage());
  }
  return arrow::Status::OK();
}
#endif // ARROW_S3

#ifdef ARROW_AZURE
/// Delete an Azure blob directly using the Azure SDK, bypassing Arrow's
/// DeleteFile which creates 0-sized directory markers.
auto delete_file_azure(arrow::fs::AzureFileSystem* fs, const std::string& path)
  -> arrow::Status {
  const auto& options = fs->options();
  // Parse path: "container/blob_path"
  auto slash = path.find('/');
  auto container = path.substr(0, slash);
  auto blob_path = slash != std::string::npos ? path.substr(slash + 1) : "";
  // Create blob client from Arrow options.
  auto service_client_result = options.MakeBlobServiceClient();
  if (not service_client_result.ok()) {
    return service_client_result.status();
  }
  auto service_client = std::move(*service_client_result);
  auto container_client = service_client->GetBlobContainerClient(container);
  auto blob_client = container_client.GetBlobClient(blob_path);
  try {
    blob_client.Delete();
  } catch (const Azure::Storage::StorageException& e) {
    return arrow::Status::IOError("failed to delete Azure blob: ", e.what());
  }
  return arrow::Status::OK();
}
#endif // ARROW_AZURE

} // namespace

from_file_source::from_file_source() = default;

from_file_source::from_file_source(chunk_source_actor source)
  : source_{std::move(source)} {
  TENZIR_ASSERT(source_);
}

auto from_file_source::name() const -> std::string {
  return "from_file_source";
}

from_file_sink::from_file_sink(
  from_file_actor parent, event_order order,
  std::optional<std::pair<ast::field_path, std::string>> path_field)
  : parent_{std::move(parent)},
    order_{order},
    path_field_{std::move(path_field)} {
}

auto from_file_sink::name() const -> std::string {
  return "from_file_sink";
}

auto from_file_args::add_to(argument_parser2& p) -> void {
  p.positional("url", url);
  p.named_optional("watch", watch);
  p.named_optional("remove", remove);
  p.named("rename", rename, "string -> string");
  p.named("path_field", path_field);
  p.named("max_age", max_age);
  p.positional("{ … }", pipe);
}

auto from_file_args::handle(session ctx) const -> failure_or<pipeline> {
  auto result = pipeline{};
  if (pipe) {
    auto output_type = pipe->inner.infer_type<chunk_ptr>();
    if (not output_type) {
      diagnostic::error("pipeline must accept bytes").primary(*pipe).emit(ctx);
      return failure::promise();
    }
    if (not output_type->is_any<void, table_slice>()) {
      diagnostic::error("pipeline must return events or void")
        .primary(*pipe)
        .emit(ctx);
      return failure::promise();
    }
    if (output_type->is<void>()) {
      const auto* discard_op
        = plugins::find<operator_factory_plugin>("discard");
      TENZIR_ASSERT(discard_op);
      TRY(auto discard_pipe, make_operator(*discard_op, oploc, ctx));
      result.append(std::move(discard_pipe));
    }
  }
  if (remove.inner and rename) {
    diagnostic::error("cannot use both `remove` and `rename`")
      .primary(remove.source)
      .primary(*rename)
      .emit(ctx);
    return failure::promise();
  }
  if (max_age and max_age->inner <= duration::zero()) {
    diagnostic::error("`max_age` must be a positive duration")
      .primary(max_age->source)
      .emit(ctx);
    return failure::promise();
  }
  return result;
}

from_file_state::from_file_state(
  from_file_actor::pointer self, from_file_args args, std::string plaintext_url,
  event_order order, std::unique_ptr<diagnostic_handler> dh,
  std::string definition, node_actor node, bool is_hidden,
  metrics_receiver_actor metrics_receiver, uint64_t operator_index,
  std::string pipeline_id)
  : self_{self},
    dh_{std::move(dh)},
    args_{std::move(args)},
    order_{order},
    definition_{std::move(definition)},
    node_{std::move(node)},
    is_hidden_{is_hidden},
    pipeline_id_{std::move(pipeline_id)},
    operator_index_{operator_index},
    metrics_receiver_{std::move(metrics_receiver)} {
  TENZIR_ASSERT(dh_);
  auto expanded = expand_home(std::move(plaintext_url));
  if (not expanded.contains("://")) {
    // Arrow doesn't allow relative paths, so we make it absolute.
    expanded = std::filesystem::weakly_canonical(expanded);
  }
  // TODO: Arrow removes trailing slashes here. Do we need them?
  // TODO: Once we allow `?` in globs (which is currently not supported), we
  // run into trouble here because of `s3://bucket/a?.b?endpoint_override=`.
  auto path = std::string{};
  auto fs = arrow::fs::FileSystemFromUriOrPath(expanded, &path);
  if (not fs.ok()) {
    diagnostic::error("{}", fs.status().ToStringWithoutContextLines())
      .primary(args_.url)
      .emit(*dh_);
    self->quit(ec::silent);
    return;
  }
  fs_ = fs.MoveValueUnsafe();
  glob_ = parse_glob(path);
  root_path_ = extract_root_path(glob_, expanded);
  query_files();
}

from_file_state::from_file_state(
  from_file_actor::pointer self, from_file_args args, std::string expanded,
  std::string path, std::shared_ptr<arrow::fs::FileSystem> fs,
  event_order order, std::unique_ptr<diagnostic_handler> dh,
  std::string definition, node_actor node, bool is_hidden,
  metrics_receiver_actor metrics_receiver, uint64_t operator_index,
  std::string pipeline_id)
  : self_{self},
    dh_{std::move(dh)},
    fs_{std::move(fs)},
    args_{std::move(args)},
    order_{order},
    definition_{std::move(definition)},
    node_{std::move(node)},
    is_hidden_{is_hidden},
    pipeline_id_{std::move(pipeline_id)},
    operator_index_{operator_index},
    metrics_receiver_{std::move(metrics_receiver)} {
  TENZIR_ASSERT(dh_);
  TENZIR_ASSERT(fs_);
  glob_ = parse_glob(path);
  root_path_ = extract_root_path(glob_, expanded);
  query_files();
}

auto from_file_state::make_behavior() -> from_file_actor::behavior_type {
  return {
    [this](atom::get) -> caf::result<table_slice> {
      return get();
    },
    [this](atom::put, table_slice slice) -> caf::result<void> {
      return put(std::move(slice));
    },
    [this](diagnostic diag) {
      dh_->emit(std::move(diag));
    },
    [this](uint64_t nested_operator_index, uuid nested_metrics_id,
           type schema) {
      return register_metrics(nested_operator_index, nested_metrics_id,
                              std::move(schema));
    },
    [this](uint64_t nested_operator_index, uuid nested_metrics_id,
           record metrics) {
      return handle_metrics(nested_operator_index, nested_metrics_id,
                            std::move(metrics));
    },
    [](const operator_metric& metrics) {
      // Cannot forward operator metrics from nested pipelines.
      TENZIR_UNUSED(metrics);
    },
  };
}

auto from_file_state::get() -> caf::result<table_slice> {
  if (puts_.empty()) {
    auto rp = self_->make_response_promise<table_slice>();
    gets_.emplace_back(rp);
    check_termination();
    return rp;
  }
  auto slice = std::move(puts_.front().first);
  puts_.front().second.deliver();
  puts_.pop_front();
  return slice;
}

auto from_file_state::put(table_slice slice) -> caf::result<void> {
  if (gets_.empty()) {
    auto rp = self_->make_response_promise<void>();
    puts_.emplace_back(std::move(slice), rp);
    return rp;
  }
  gets_.front().deliver(std::move(slice));
  gets_.pop_front();
  return {};
}

auto from_file_state::query_files() -> void {
  add_actor_callback(
    self_, fs_->GetFileInfoAsync(std::vector{root_path_}),
    [this](arrow::Result<std::vector<arrow::fs::FileInfo>> infos) {
      if (not infos.ok()) {
        diagnostic::error("{}", infos.status().ToStringWithoutContextLines())
          .primary(args_.url)
          .emit(*dh_);
        return;
      }
      TENZIR_ASSERT(infos->size() == 1);
      auto root_info = std::move((*infos)[0]);
      switch (root_info.type()) {
        case arrow::fs::FileType::NotFound:
          // We only want to allow this if `watch=true`.
          if (args_.watch) {
            got_all_files();
          } else {
            diagnostic::error("`{}` does not exist", root_path_)
              .primary(args_.url)
              .emit(*dh_);
          }
          return;
        case arrow::fs::FileType::Unknown:
          diagnostic::error("`{}` is unknown", root_path_)
            .primary(args_.url)
            .emit(*dh_);
          return;
        case arrow::fs::FileType::File:
          if (matches(root_info.path(), glob_)) {
            add_job(std::move(root_info));
            got_all_files();
          } else if (not args_.watch) {
            diagnostic::error("`{}` is a file, not a directory", root_path_)
              .primary(args_.url)
              .emit(*dh_);
          }
          return;
        case arrow::fs::FileType::Directory:
          auto sel = arrow::fs::FileSelector{};
          sel.base_dir = root_path_;
          sel.recursive = true;
          auto gen = fs_->GetFileInfoGenerator(sel);
          iterate_files(self_, std::move(gen),
                        [this](arrow::Result<arrow::fs::FileInfoVector> files) {
                          if (not files.ok()) {
                            diagnostic::error(
                              "{}",
                              files.status().ToStringWithoutContextLines())
                              .primary(args_.url)
                              .emit(*dh_);
                            return;
                          }
                          if (files->empty()) {
                            got_all_files();
                            return;
                          }
                          for (auto& file : *files) {
                            process_file(std::move(file));
                          }
                        });
          return;
      }
      TENZIR_UNREACHABLE();
    });
}

auto from_file_state::process_file(arrow::fs::FileInfo file) -> void {
  // Clean up directory markers (S3/Azure only) when remove=true.
  // Directory markers are 0-sized objects with keys ending in '/'.
  if (args_.remove.inner && file.type() == arrow::fs::FileType::Directory) {
    auto marker_path = file.path() + '/';
#ifdef ARROW_S3
    if (auto* s3_fs = dynamic_cast<arrow::fs::S3FileSystem*>(fs_.get())) {
      std::ignore = delete_file_s3(s3_fs, marker_path);
    }
#endif
#ifdef ARROW_AZURE
    if (auto* azure_fs = dynamic_cast<arrow::fs::AzureFileSystem*>(fs_.get())) {
      std::ignore = delete_file_azure(azure_fs, marker_path);
    }
#endif
  }
  if (file.IsFile() and matches(file.path(), glob_)) {
    if (args_.max_age) {
      if (file.mtime() == arrow::fs::kNoTime) {
        diagnostic::warning("could not get last modification time for "
                            "file `{}`",
                            root_path_)
          .note("assuming file was recently modified")
          .emit(*dh_);
      } else if (time::clock::now() - file.mtime() >= args_.max_age->inner) {
        return;
      }
    }
    add_job(std::move(file));
  }
}

auto from_file_state::got_all_files() -> void {
  if (args_.watch) {
    std::swap(previous_, current_);
    current_.clear();
    self_->run_delayed_weak(watch_pause, [this] {
      query_files();
    });
  } else {
    added_all_jobs_ = true;
    check_termination();
  }
}

auto from_file_state::check_termination() -> void {
  if (added_all_jobs_ and jobs_.empty() and active_jobs_ == 0) {
    for (auto& get : gets_) {
      // If there are any unmatched gets, we know that there are no puts.
      get.deliver(table_slice{});
    }
  }
}

auto from_file_state::check_jobs() -> void {
  while (not jobs_.empty() and active_jobs_ < max_jobs) {
    start_job(jobs_.front());
    jobs_.pop_front();
  }
}

auto from_file_state::check_jobs_and_termination() -> void {
  check_jobs();
  check_termination();
}

auto from_file_state::add_job(arrow::fs::FileInfo file) -> void {
  auto inserted = current_.emplace(file).second;
  TENZIR_ASSERT(inserted);
  if (previous_.contains(file)) {
    return;
  }
  jobs_.push_back(std::move(file));
  check_jobs();
}

auto from_file_state::make_pipeline(std::string_view path)
  -> failure_or<pipeline> {
  if (args_.pipe) {
    return args_.pipe->inner;
  }
  auto parse_dh = transforming_diagnostic_handler{
    *dh_, [this, path](diagnostic d) {
      if (is_globbing()) {
        d.severity = severity::warning;
      }
      return std::move(d)
        .modify()
        .note(fmt::format("coming from `{}`", path))
        .done();
    }};
  TRY(auto compression_and_format,
      get_compression_and_format<true>(
        located<std::string_view>{path, args_.url.source}, nullptr,
        "https://docs.tenzir.com/reference/operators/from_file", parse_dh));
  const auto& format = compression_and_format.format.get();
  const auto* compression = compression_and_format.compression;
  auto provider = session_provider::make(parse_dh);
  auto ctx = provider.as_session();
  auto pipe = pipeline{};
  if (compression) {
    TRY(auto decompress, make_operator(*compression, args_.url.source, ctx));
    pipe.append(std::move(decompress));
  }
  TRY(auto read, make_operator(format, args_.url.source, ctx));
  pipe.append(std::move(read));
  return pipe;
}

auto from_file_state::start_job(const arrow::fs::FileInfo& file) -> void {
  TENZIR_ASSERT(active_jobs_ < max_jobs);
  active_jobs_ += 1;
  auto pipe = make_pipeline(file.path());
  if (pipe.is_error()) {
    active_jobs_ -= 1;
    return;
  }
  // We already checked the output type after parsing.
  auto output_type = pipe->infer_type<chunk_ptr>();
  TENZIR_ASSERT(output_type);
  TENZIR_ASSERT((output_type->is_any<void, table_slice>()));
  add_actor_callback(
    self_, fs_->OpenInputStreamAsync(file),
    [this, pipe = std::move(*pipe), path = file.path()](
      arrow::Result<std::shared_ptr<arrow::io::InputStream>> stream) mutable {
      start_stream(std::move(stream), std::move(pipe), std::move(path));
    });
}

auto from_file_state::start_stream(
  arrow::Result<std::shared_ptr<arrow::io::InputStream>> stream, pipeline pipe,
  std::string path) -> void {
  if (not stream.ok()) {
    pipeline_failed("failed to open `{}`", path)
      .primary(args_.url)
      .note(stream.status().ToStringWithoutContextLines())
      .emit(*dh_);
    active_jobs_ -= 1;
    return;
  }
  auto source = self_->spawn(caf::actor_from_state<arrow_chunk_source>,
                             std::move(*stream));
  auto weak = caf::weak_actor_ptr{source->ctrl(), caf::add_ref};
  pipe.prepend(std::make_unique<from_file_source>(std::move(source)));
  if (not pipe.is_closed()) {
    pipe.append(std::make_unique<from_file_sink>(
      self_, order_,
      args_.path_field ? std::optional{std::pair{*args_.path_field, path}}
                       : std::nullopt));
    pipe = pipe.optimize_if_closed();
  }
  auto executor
    = self_->spawn(pipeline_executor, std::move(pipe), definition_, self_,
                   self_, node_, false, is_hidden_, pipeline_id_);
  self_->attach_functor([this, weak]() {
    if (auto strong = weak.lock()) {
      // FIXME: This should not be necessary to ensure that the actor is
      // destroyed when the executor is done. This problem could also
      // apply to other operators.
      self_->send_exit(strong, caf::exit_reason::user_shutdown);
    }
  });
  self_->monitor(executor, [this, executor, path,
                            weak = std::move(weak)](caf::error error) {
    if (auto strong = weak.lock()) {
      // FIXME: This should not be necessary to ensure that the actor is
      // destroyed when the executor is done. This problem could also
      // apply to other operators.
      self_->send_exit(strong, caf::exit_reason::user_shutdown);
    }
    active_jobs_ -= 1;
    if (error.valid()) {
      pipeline_failed(std::move(error))
        .note("coming from `{}`", path)
        .emit(*dh_);
      return;
    }
    if (args_.rename) {
      match(
        eval(*args_.rename, path, *dh_),
        [&](const std::string& new_path) {
          std::string final_path = new_path;
          // If new_path has a trailing slash, append the original file name
          if (not new_path.empty() && new_path.back() == '/') {
            auto path_obj = std::filesystem::path(path);
            auto filename = path_obj.filename();
            final_path = (std::filesystem::path(new_path) / filename).string();
          }
          // Create any intermediate directories required for writing to
          // final_path
          auto final_path_obj = std::filesystem::path(final_path);
          auto parent_path = final_path_obj.parent_path();
          if (not parent_path.empty() && parent_path != ".") {
            auto create_status
              = fs_->CreateDir(parent_path.string(), /*recursive=*/true);
            if (not create_status.ok()) {
              diagnostic::warning("failed to create intermediate "
                                  "directories "
                                  "for `{}`",
                                  final_path)
                .primary(*args_.rename)
                .note(create_status.ToStringWithoutContextLines())
                .emit(*dh_);
              return;
            }
          }
          auto status = fs_->Move(path, final_path);
          if (not status.ok()) {
            diagnostic::warning("failed to rename `{}` to `{}`", path,
                                final_path)
              .primary(*args_.rename)
              .note(status.ToStringWithoutContextLines())
              .emit(*dh_);
          }
        },
        [&](const auto& x) {
          diagnostic::warning("expected `string`, but got `{}`",
                              type::infer(x).value_or(type{}).kind())
            .primary(*args_.rename)
            .emit(*dh_);
        });
    }
    if (args_.remove.inner) {
      // Use SDK-based deletion for S3/Azure to avoid Arrow's directory marker
      // creation. Arrow's DeleteFile() creates a 0-sized "directory marker"
      // object in the parent directory after deletion, which is undesirable.
      auto status = arrow::Status::OK();
#ifdef ARROW_S3
      if (auto* s3_fs = dynamic_cast<arrow::fs::S3FileSystem*>(fs_.get())) {
        status = delete_file_s3(s3_fs, path);
      } else
#endif
#ifdef ARROW_AZURE
        if (auto* azure_fs
            = dynamic_cast<arrow::fs::AzureFileSystem*>(fs_.get())) {
        status = delete_file_azure(azure_fs, path);
      } else
#endif
      {
        // Fall back to Arrow's DeleteFile for other filesystems.
        status = fs_->DeleteFile(path);
      }
      if (not status.ok()) {
        diagnostic::warning("failed to remove `{}`", path)
          .primary(args_.url)
          .note(status.ToStringWithoutContextLines())
          .emit(*dh_);
      }
    }
    check_jobs_and_termination();
  });
  self_->mail(atom::start_v)
    .request(executor, caf::infinite)
    .then([] {},
          [this, path = std::move(path)](caf::error error) {
            pipeline_failed(std::move(error))
              .note("coming from `{}`", path)
              .emit(*dh_);
          });
}

auto from_file_state::register_metrics(uint64_t, uuid nested_metrics_id,
                                       type schema) -> caf::result<void> {
  return self_->mail(operator_index_, nested_metrics_id, std::move(schema))
    .delegate(metrics_receiver_);
}

auto from_file_state::handle_metrics(uint64_t, uuid nested_metrics_id,
                                     record metrics) -> caf::result<void> {
  return self_->mail(operator_index_, nested_metrics_id, std::move(metrics))
    .delegate(metrics_receiver_);
}

auto from_file_state::pipeline_failed(caf::error error) const
  -> diagnostic_builder {
  if (is_globbing()) {
    return diagnostic::warning(std::move(error));
  }
  return diagnostic::error(std::move(error));
}

auto from_file_state::is_globbing() const -> bool {
  return glob_.size() != 1 or not is<std::string>(glob_[0]);
}

} // namespace tenzir

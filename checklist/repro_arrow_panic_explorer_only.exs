payload_path =
  Path.expand(
    "checklist/payloads/arrow_duplicate_columns.ipc",
    File.cwd!()
  )

bin = File.read!(payload_path)
IO.puts("Loaded payload: #{payload_path} (#{byte_size(bin)} bytes)")
IO.puts("About to call Explorer.DataFrame.load_ipc_stream/1 (expected to NIF panic)")

Explorer.DataFrame.load_ipc_stream(bin)

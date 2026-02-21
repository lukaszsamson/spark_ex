defmodule SparkEx.UserContextExtensionsTest do
  use ExUnit.Case, async: true

  alias Google.Protobuf.Any
  alias SparkEx.UserContextExtensions

  setup do
    UserContextExtensions.clear_user_context_extensions()

    on_exit(fn ->
      UserContextExtensions.clear_user_context_extensions()
    end)

    :ok
  end

  test "threadlocal extensions are process-scoped in ETS" do
    ext_main = %Any{type_url: "type.example/main", value: <<1>>}
    ext_other = %Any{type_url: "type.example/other", value: <<2>>}

    :ok = UserContextExtensions.add_threadlocal_user_context_extension(ext_main)

    task =
      Task.async(fn ->
        :ok = UserContextExtensions.add_threadlocal_user_context_extension(ext_other)
        UserContextExtensions.collect_extensions([])
      end)

    other_extensions = Task.await(task)
    main_extensions = UserContextExtensions.collect_extensions([])

    assert ext_other in other_extensions
    refute ext_main in other_extensions
    assert ext_main in main_extensions
    refute ext_other in main_extensions
  end

  test "clear_threadlocal_user_context_extensions clears only current process scope" do
    ext_global = %Any{type_url: "type.example/global", value: <<10>>}
    ext_local = %Any{type_url: "type.example/local", value: <<11>>}

    :ok = UserContextExtensions.add_global_user_context_extension(ext_global)
    :ok = UserContextExtensions.add_threadlocal_user_context_extension(ext_local)

    :ok = UserContextExtensions.clear_threadlocal_user_context_extensions()

    extensions = UserContextExtensions.collect_extensions([])
    assert ext_global in extensions
    refute ext_local in extensions
  end

  test "clear_user_context_extensions atomically clears global and threadlocal scopes" do
    ext_global = %Any{type_url: "type.example/global2", value: <<20>>}
    ext_local = %Any{type_url: "type.example/local2", value: <<21>>}

    :ok = UserContextExtensions.add_global_user_context_extension(ext_global)
    :ok = UserContextExtensions.add_threadlocal_user_context_extension(ext_local)

    assert length(UserContextExtensions.collect_extensions([])) == 2

    :ok = UserContextExtensions.clear_user_context_extensions()
    assert UserContextExtensions.collect_extensions([]) == []
  end
end

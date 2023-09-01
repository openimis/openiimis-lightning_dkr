defmodule Lightning.ImisSetupScript do
  require Logger
  use Ecto.Migration

  alias Ecto.Adapters.SQL
  import Ecto.Query  # import Ecto.Query for from macro


  defp db_exists? do
    result = Lightning.Repo.exists?(from d in "pg_database", where: d.datname == ^System.get_env("POSTGRES_DB"))
    result
  end


  defp check_and_create_db do
    Logger.info("Checking for database #{System.get_env("POSTGRES_DB")}...")
    unless db_exists?() do
      Logger.info("Database doesn't exist, creating.")
      {:ok, _, _} = SQL.query(Lightning.Repo, "CREATE DATABASE #{System.get_env("POSTGRES_DB")}")
    end
  end

  defp run_migrations do
    Ecto.Migrator.run(Lightning.Repo, "priv/repo/migrations", :up, all: true)
  end

  defp data_exists_in_table_users? do
    Lightning.Repo.exists?(from d in "users")
  end

  defp example_data_exists_in_table_projects? do
    Lightning.Repo.exists?(from d in "projects", where: d.name == "imisstarterproject")
  end


  defp beneficiary_upload_data_exists_in_table_projects? do
    Lightning.Repo.exists?(from d in "projects", where: d.name == "imisbenefitplanworkflows")
  end

  defp openimis-coremis-payment_adaptor_exists_in_table_projects? do
    Lightning.Repo.exists?(from d in "projects", where: d.name == "openimis-coremis-payment-adaptor")
  end


  defp execute_scripts(script_paths) do
    Enum.each(script_paths, fn script_path ->
      case File.exists?(script_path) do
        true ->
          {result, _} = Code.eval_file(script_path)
          IO.inspect(result)
        _ ->
          Logger.warn("Script not found: #{script_path}")
      end
    end)
  end

  def run do
    # Check and create db if not exists
    check_and_create_db()

    # Run migrations
    run_migrations()

    unless data_exists_in_table_users?() do
      script_paths = ["imisSetupScripts/CreateUser.exs"]
      execute_scripts(script_paths)
    else
      IO.puts("Users exists.")
    end

    unless example_data_exists_in_table_projects?() do
      script_paths = ["imisSetupScripts/CreateSetupProject.exs"]
      execute_scripts(script_paths)
    else
      IO.puts("Imis starter project already exists.")
    end

    unless beneficiary_upload_data_exists_in_table_projects?() do
      script_paths = ["imisSetupScripts/CreateBenefitPlanProjects.exs"]
      execute_scripts(script_paths)
    else
      IO.puts("Imis Beneficiary Workflows already exists.")
    end

    unless openimis-coremis-payment_adaptor_exists_in_table_projects?() do
      script_paths = ["imisSetupScripts/CreatePaymentAdaptorProject.exs"]
      execute_scripts(script_paths)
    else
      IO.puts("Imis CoreMIS Payment Adaptor project already exists.")
    end

  end
end

Lightning.ImisSetupScript.run

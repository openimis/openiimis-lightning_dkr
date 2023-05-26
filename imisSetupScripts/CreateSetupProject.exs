defmodule Lightning.SetupIProjectsMISWorkflow do
  import Lightning

  alias Lightning.{Projects, Accounts, Jobs, Workflows, Repo, Credentials}

  import Ecto.Query

  def create_starter_project(name, project_users) do
    # Project
    {:ok, project} =
      Lightning.Projects.create_project(%{
        name: name,
        project_users: project_users
      })

    {:ok, project} =
      Lightning.Projects.create_project(%{
        name: name,
        project_users: project_users
      })

    {:ok, workflow} =
      Lightning.Workflows.create_workflow(%{
        name: "Demo Benefit Plan Workflow",
        project_id: project.id
      })


    project_user = List.first(project_users)

    {:ok, credential} =
      Lightning.Credentials.create_credential(%{
        body: %{
          "host": System.fetch_env!("IMIS_DB_HOST"),
          "port": System.fetch_env!("IMIS_DB_PORT"),
          "database":  System.fetch_env!("IMIS_DB_DATABASE"),
          "user": System.fetch_env!("IMIS_DB_USER"),
          "password": System.fetch_env!("IMIS_DB_PASSWORD"),
        },
        name: "openIMIS Database Credentials",
        user_id: project_user.user_id,
        schema: "raw",
        project_credentials: [
          %{project_id: project.id}
        ]
      })

    {:ok, job_1} =
      Lightning.Jobs.create_job(%{
        name: "IndividualDataSource To Individual",
        body: """
        sql(state => `BEGIN;
        WITH new_entry AS (
          INSERT INTO individual_individual(
          "UUID", "isDeleted", version, "UserCreatedUUID", "UserUpdatedUUID",
          "Json_ext", first_name, last_name, dob
          )
          SELECT gen_random_uuid(), false, 1, ${state.data.userUUID}, ${state.data.userUUID},
          "Json_ext", "Json_ext"->'name', "Json_ext" -> 'surname', to_date("Json_ext" ->> 'dob', 'YYYY-mm-dd')
          FROM individual_individualdatasource
          WHERE source_name=${state.data.sourceName} and source_type=${state.data.sourceType} and individual_id is null and "isDeleted"=False
            RETURNING "UUID"
        )
        UPDATE individual_individualdatasource
        SET individual_id = new_entry."UUID"
        FROM new_entry
        WHERE source_name=${state.data.sourceName} and source_type=${state.data.sourceType} and individual_id is null and "isDeleted"=False; -- specify the condition to identify which row(s) to update in Table A

        COMMIT;`, { writeSql: true })
""",
        adaptor: "@openfn/language-postgresql@latest",
        trigger: %{type: "webhook"},
        enabled: true,
        workflow_id: workflow.id,
        project_credential_id: List.first(credential.project_credentials).id
      })

    %{
      project: project,
      workflow: workflow,
      jobs: [job_1]
    }
  end
  def run do
    create_starter_project(
      "imisstarterproject",
      Lightning.Repo.all(from u in Lightning.Accounts.User, where: u.email == ^System.fetch_env!("IMIS_USER_EMAIL"))
      |> Enum.map(&%{user_id: &1.id, role: :admin})
    )
  end

end

Lightning.SetupIProjectsMISWorkflow.run

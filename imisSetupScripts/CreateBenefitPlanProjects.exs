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
        name: "Benefit Plan Upload",
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
        name: "BeneficiariesFileUpload",
        body: """
        sql(state => `
        DO \$\$
        declare
            current_upload_id UUID := '${state.data.upload_uuid}'::UUID;
            userUUID UUID := '${state.data.user_uuid}'::UUID;
            benefitPlan UUID := '${state.data.benefit_plan_uuid}'::UUID;
            failing_entries UUID[];
            json_schema jsonb;
            failing_entries_invalid_json UUID[];
            failing_entries_first_name UUID[];
            failing_entries_last_name UUID[];
            failing_entries_dob UUID[];
        BEGIN

            -- Check if all required fields are present in the entries
            SELECT ARRAY_AGG("UUID") INTO failing_entries_first_name
            FROM individual_individualdatasource
            WHERE upload_id=current_upload_id and individual_id is null and "isDeleted"=False AND NOT "Json_ext" ? 'first_name';

            SELECT ARRAY_AGG("UUID") INTO failing_entries_last_name
            FROM individual_individualdatasource
            WHERE upload_id=current_upload_id and individual_id is null and "isDeleted"=False AND NOT "Json_ext" ? 'last_name';

            SELECT ARRAY_AGG("UUID") INTO failing_entries_dob
            FROM individual_individualdatasource
            WHERE upload_id=current_upload_id and individual_id is null and "isDeleted"=False AND NOT "Json_ext" ? 'dob';


            -- Check if any entries have invalid Json_ext according to the schema
            SELECT beneficiary_data_schema INTO json_schema FROM social_protection_benefitplan WHERE "UUID" = benefitPlan;
            SELECT ARRAY_AGG("UUID") INTO failing_entries_invalid_json
            FROM individual_individualdatasource
            WHERE upload_id=current_upload_id and individual_id is null and "isDeleted"=False AND NOT validate_json_schema(json_schema, "Json_ext");

            -- If any entries do not meet the criteria or missing required fields, set the error message in the upload table and do not proceed further
            IF failing_entries_invalid_json IS NOT NULL or failing_entries_first_name IS NOT NULL OR failing_entries_last_name IS NOT NULL OR failing_entries_dob IS NOT NULL THEN
                UPDATE individual_individualdatasourceupload
                SET error = coalesce(error, '{}'::jsonb) || jsonb_build_object('errors', jsonb_build_object(
                                    'error', 'Invalid entries',
                                    'timestamp', NOW()::text,
                                    'upload_id', current_upload_id::text,
                                    'failing_entries_first_name', failing_entries_first_name,
                                    'failing_entries_last_name', failing_entries_last_name,
                                    'failing_entries_dob', failing_entries_dob,
                                    'failing_entries_invalid_json', failing_entries_invalid_json
                                ))
                WHERE "UUID" = current_upload_id;

               update individual_individualdatasourceupload set status='FAIL' where "UUID" = current_upload_id;
            -- If no invalid entries, then proceed with the data manipulation
            ELSE
                BEGIN
                  WITH new_entry AS (
                    INSERT INTO individual_individual(
                    "UUID", "isDeleted", version, "UserCreatedUUID", "UserUpdatedUUID",
                    "Json_ext", first_name, last_name, dob
                    )
                    SELECT gen_random_uuid(), false, 1, userUUID, userUUID,
                    "Json_ext", "Json_ext"->>'first_name', "Json_ext" ->> 'last_name', to_date("Json_ext" ->> 'dob', 'YYYY-MM-DD')
                    FROM individual_individualdatasource
                    WHERE upload_id=current_upload_id and individual_id is null and "isDeleted"=False
                    RETURNING "UUID", "Json_ext"  -- also return the Json_ext
                  )
                  UPDATE individual_individualdatasource
                  SET individual_id = new_entry."UUID"
                  FROM new_entry
                  WHERE upload_id=current_upload_id 
                    and individual_id is null 
                    and "isDeleted"=False 
                    and individual_individualdatasource."Json_ext" = new_entry."Json_ext";  -- match on Json_ext


                    with new_entry_2 as (INSERT INTO social_protection_beneficiary(
                    "UUID", "isDeleted", "Json_ext", "DateCreated", "DateUpdated", version, "DateValidFrom", "DateValidTo", status, "benefit_plan_id", "individual_id", "UserCreatedUUID", "UserUpdatedUUID"
                    )
                    SELECT gen_random_uuid(), false, iids."Json_ext" - 'first_name' - 'last_name' - 'dob', NOW(), NOW(), 1, NOW(), NULL, 'POTENTIAL', benefitPlan, new_entry."UUID", userUUID, userUUID
                    FROM individual_individualdatasource iids right join individual_individual new_entry on new_entry."UUID" = iids.individual_id
                    WHERE iids.upload_id=current_upload_id and iids."isDeleted"=false
                    returning "UUID")
                    
                    
                    update individual_individualdatasourceupload set status='SUCCESS', error='{}' where "UUID" = current_upload_id;
                    EXCEPTION
                    WHEN OTHERS then

                    update individual_individualdatasourceupload set status='FAIL' where "UUID" = current_upload_id;
                        UPDATE individual_individualdatasourceupload
                        SET error = coalesce(error, '{}'::jsonb) || jsonb_build_object('errors', jsonb_build_object(
                                            'error', SQLERRM,
                                            'timestamp', NOW()::text,
                                            'upload_id', current_upload_id::text
                                        ))
                        WHERE "UUID" = current_upload_id;
                END;
            END IF;
        END \$\$;
        `, { writeSql: true })
)
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
      "imisbenefitplanworkflows",
      Lightning.Repo.all(from u in Lightning.Accounts.User, where: u.email == ^System.fetch_env!("IMIS_USER_EMAIL"))
      |> Enum.map(&%{user_id: &1.id, role: :admin})
    )
  end

end

Lightning.SetupIProjectsMISWorkflow.run

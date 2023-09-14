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
        name: "payment-adaptor",
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
        name: "process_payroll_data",
        body:
        """
        sql(state => `
        DO $$
        declare
            userUUID UUID := '${state.data.user_uuid}'::UUID;
            payroll UUID := '${state.data.payroll_uuid}'::UUID;
            payroll_bill_ids jsonb;
            payroll_amount text := '${state.data.payroll_amount}'::text;
        BEGIN
            SELECT array_to_json(ARRAY_AGG("tblBill"."UUID")) INTO payroll_bill_ids
            FROM "tblBill" INNER JOIN "payroll_payrollbill" ON ("tblBill"."UUID" = "payroll_payrollbill"."bill_id") INNER JOIN "payroll_payroll" ON ("payroll_payrollbill"."payroll_id" = "payroll_payroll"."UUID")
            WHERE (NOT "tblBill"."isDeleted" AND NOT "payroll_payrollbill"."isDeleted" AND NOT
            "payroll_payroll"."isDeleted" AND "payroll_payrollbill"."payroll_id" = payroll AND "tblBill"."Status" = 1);


            INSERT INTO payroll_paymentadaptorhistory(
              "UUID", "isDeleted", version, "UserCreatedUUID", "UserUpdatedUUID",
              "Json_ext", "payroll_id", "total_amount", "bills_ids"
            )
            VALUES (
              gen_random_uuid(), false, 1, userUUID, userUUID,
              '{}', payroll, payroll_amount, payroll_bill_ids
            );
        END $$;
        `, { writeSql: true })
        """,
        adaptor: "@openfn/language-postgresql@latest",
        trigger: %{type: "webhook"},
        enabled: true,
        workflow_id: workflow.id,
        project_credential_id: List.first(credential.project_credentials).id
      })

      {:ok, job_2} =
      Lightning.Jobs.create_job(%{
        name: "send_payroll_data_to_adaptor",
        body:
        """
        // Here will be the implementation of sendind payroll data (amount, code/id etc) to particular
        // payment gateway in chosen country implementation
        // Necessary data can be obtained from reference in state object
        // use functions PUT, POST, GET etc that are available based on docs of 'common' adaptor
        state.responseData = {"success": "true", "data": []}
        var bills = state.references[0].bills.split(',');

        // Generate a random number between 0 and 1
        var randomValue = Math.random();
        // Convert the random number to either 0 or 1
        var result = randomValue < 0.5 ? 0 : 1;

        var rejectedBills = [];
        // Check if there are at least 2 bills in the array
        if (result === 1) {
            if (bills.length >= 2) {
                // Select the last two bills
                var lastTwoBills = bills.slice(-2);
                // Now 'lastTwoBills' will contain the last two bills from the array
                rejectedBills = lastTwoBills;
            } else if (bills.length === 1) {
                // If there's only one bill, select that bill
                rejectedBills = bills[0];
            }
        }

        state.rejectedBills = arrayToString(rejectedBills,',');
        """,
        adaptor: "@openfn/language-common@latest",
        trigger: %{
          type: "on_job_success",
          upstream_job_id: job_1.id
        },
        enabled: true,
        workflow_id: workflow.id,
        project_credential_id: List.first(credential.project_credentials).id
      })

      {:ok, job_3} =
      Lightning.Jobs.create_job(%{
        name: "failed",
        body:
        """
        // Get started by adding operations from your adaptor here
        """,
        adaptor: "@openfn/language-http@latest",
        trigger: %{
          type: "on_job_failure",
          upstream_job_id: job_2.id
        },
        enabled: true,
        workflow_id: workflow.id,
        project_credential_id: List.first(credential.project_credentials).id
      })

      {:ok, job_4} =
      Lightning.Jobs.create_job(%{
        name: "get_jwt_token",
        body:
        """
        post('http://backend:8000/api/api_fhir_r4/login/', {
          body: { "username": "<USERNAME>", "password": "<PASSWORD>" },
          headers: {'content-type': 'application/json'}
        })
        """,
        adaptor: "@openfn/language-http@latest",
        trigger: %{
          type: "on_job_success",
          upstream_job_id: job_2.id
        },
        enabled: true,
        workflow_id: workflow.id,
        project_credential_id: List.first(credential.project_credentials).id,
        upstream_job_id: job_2.id
      })

      {:ok, job_5} =
      Lightning.Jobs.create_job(%{
        name: "callback_to_openIMIS_payroll",
        body:
        """
        post('http://backend:8000/api/payroll/send_callback_to_openimis/', {
          body: { "payroll_id": state.references[0].payroll_uuid, "response_from_gateway": state.responseData, "rejected_bills": state.rejectedBills },
          headers: {'content-type': 'application/json', "Authorization": `Bearer ${state.response.data.token}`}
        })
        """,
        adaptor: "@openfn/language-http@latest",
        trigger: %{
          type: "on_job_success",
          upstream_job_id: job_4.id
        },
        enabled: true,
        workflow_id: workflow.id,
        project_credential_id: List.first(credential.project_credentials).id,
        upstream_job_id: job_4.id
      })

    %{
      project: project,
      workflow: workflow,
      jobs: [job_1, job_2, job_3, job_4, job_5]
    }
  end
  def run do
    create_starter_project(
      "openimis-coremis-payment-adaptor",
      Lightning.Repo.all(from u in Lightning.Accounts.User, where: u.email == ^System.fetch_env!("IMIS_USER_EMAIL"))
      |> Enum.map(&%{user_id: &1.id, role: :admin})
    )
  end

end

Lightning.SetupIProjectsMISWorkflow.run

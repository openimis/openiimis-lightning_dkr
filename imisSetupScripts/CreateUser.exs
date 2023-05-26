defmodule RegisterSuperuser do
  def run do
    # Import module
    import Lightning.Accounts

    # Get environment variables
    first_name = System.fetch_env!("IMIS_USER_FIRST_NAME")
    last_name = System.fetch_env!("IMIS_USER_LAST_NAME")
    email = System.fetch_env!("IMIS_USER_EMAIL")
    password = System.fetch_env!("IMIS_USER_PASSWORD")
    disabled = false
    scheduled_deletion = :nil

    # Create attribute map
    attrs = %{
      first_name: first_name,
      last_name: last_name,
      email: email,
      password: password,
      disabled: disabled,
      scheduled_deletion: scheduled_deletion
    }

    # Call function
    register_superuser(attrs)
  end
end

# Run the function
RegisterSuperuser.run
